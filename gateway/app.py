"""HTTP 服务入口：构造网关并挂载管理接口。

管理接口（便于本地验证热更新）：
- GET  /admin/config   查看当前配置快照版本与内容
- POST /admin/reload   以 body 为配置源热更新；body={"base": "..."} 等价重建本地配置
- GET  /admin/health   网关存活
"""
from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .cache import ResponseCache
from .config import GatewayConfig, RouteConfig
from .local_config import build_local_config
from .logging_utils import get_logger
from .proxy import Gateway
from .ratelimit import RateLimiter
from .router import Router
from .upstream import UpstreamSelector


def create_app(
    config: GatewayConfig | None = None,
    *,
    client: httpx.AsyncClient | None = None,
) -> FastAPI:
    config = config or build_local_config()
    logger = get_logger("gateway")
    owns_client = client is None
    client = client or httpx.AsyncClient()

    router = Router(config)
    gateway = Gateway(
        router=router,
        selector=UpstreamSelector(fail_timeout=5.0),
        limiter=RateLimiter(),
        cache=ResponseCache(),
        client=client,
        logger=logger,
    )
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            yield
        finally:
            if owns_client:
                await client.aclose()

    app = FastAPI(title="local-reverse-proxy-gateway", lifespan=lifespan)
    app.state.gateway = gateway
    app.state.router = router
    app.state.owns_client = owns_client

    @app.get("/admin/health")
    async def health() -> dict:
        return {"status": "ok", "config_version": router.current_version}

    @app.get("/admin/config")
    async def get_config() -> dict:
        snap = router.snapshot()
        return {
            "version": router.current_version,
            "routes": [
                {
                    "id": r.id,
                    "prefix": r.prefix,
                    "strategy": r.strategy.value,
                    "upstreams": [u.base_url for u in r.upstreams],
                    "cache_ttl": r.cache_ttl,
                    "streaming": r.streaming,
                }
                for r in snap.routes
            ],
        }

    @app.post("/admin/reload")
    async def reload(request: Request) -> dict:
        body = await request.json()
        if "base" in body:
            new_config = build_local_config(body["base"])
        else:
            # 最小可用的内联热更新：切换某条路由的策略
            snap = router.snapshot()
            from .config import Strategy as S

            routes = list(snap.routes)
            for i, r in enumerate(routes):
                if r.id == body.get("route_id"):
                    routes[i] = RouteConfig(
                        id=r.id,
                        prefix=r.prefix,
                        upstreams=r.upstreams,
                        strategy=S(body["strategy"]),
                        rate_limit=r.rate_limit,
                        cache_ttl=r.cache_ttl,
                        streaming=r.streaming,
                        timeout=r.timeout,
                    )
            new_config = GatewayConfig(
                routes=tuple(routes),
                global_rate_limit=snap.global_rate_limit,
                allowlist=snap.allowlist,
                blocklist=snap.blocklist,
            )
        version = router.update(new_config)
        gateway.cache.clear()
        logger.info("admin reloaded config version=%d", version)
        return {"reloaded": True, "version": version}

    @app.api_route(
        "/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    async def proxy_entry(request: Request) -> JSONResponse:
        return await gateway.handle(request)

    return app
