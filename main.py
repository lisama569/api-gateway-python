"""HTTP 服务入口：集成反向代理网关。

默认本地配置把流量分发到 3 个假上游实例（由 run_local.py 启动）：
  /gateway/rr/echo   -> 轮询
  /gateway/lc/echo   -> 最少连接
  /gateway/cache/*   -> 开启 TTL 缓存
  /gateway/rl/*      -> 按客户端限流 5 次/2 秒
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse

from gateway import proxy
from gateway.admin import build_admin_router
from gateway.config import (
    GatewayConfig,
    RateLimitConfig,
    RouteConfig,
    UpstreamConfig,
)
from gateway.state import GatewayState


def build_local_config() -> GatewayConfig:
    upstreams = tuple(
        UpstreamConfig(id=f"u{i}", base_url=f"http://127.0.0.1:{9001 + i}")
        for i in range(3)
    )
    routes = (
        RouteConfig(id="rr", path_prefix="/gateway/rr",
                    upstream_ids=("u0", "u1", "u2"),
                    strategy="round_robin"),
        RouteConfig(id="lc", path_prefix="/gateway/lc",
                    upstream_ids=("u0", "u1", "u2"),
                    strategy="least_connections"),
        RouteConfig(id="cache", path_prefix="/gateway/cache",
                    upstream_ids=("u0", "u1"),
                    strategy="round_robin", cache_ttl=2.0),
        RouteConfig(id="rl", path_prefix="/gateway/rl",
                    upstream_ids=("u0", "u1", "u2"),
                    strategy="round_robin",
                    rate_limit=RateLimitConfig(requests=5, window_seconds=2.0)),
    )
    return GatewayConfig(upstreams=upstreams, routes=routes,
                         timeout_seconds=1.0)


state = GatewayState(build_local_config())
proxy.state = state


def set_runtime_state(new_state: GatewayState) -> None:
    """测试支持：替换全局运行时状态（含 admin 路由与生命周期）。"""
    global state
    state = new_state
    proxy.state = new_state
    app.router.lifespan_context = _make_lifespan(new_state)
    # 重建 admin 路由以绑定新 state（新版 FastAPI 以 _IncludedRouter 挂载）
    from fastapi.routing import _IncludedRouter
    app.router.routes = [
        r for r in app.router.routes
        if not (isinstance(r, _IncludedRouter)
                and getattr(r.original_router, "prefix", "") == "/admin")
    ]
    app.include_router(build_admin_router(new_state))


def _make_lifespan(target_state):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await target_state.startup()
        yield
        await target_state.shutdown()
    return lifespan


app = FastAPI(title="local-api-gateway", lifespan=_make_lifespan(state))
app.include_router(build_admin_router(state))


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "local-api-gateway", "status": "ok"}


@app.api_route(
    "/gateway/{full_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
)
async def gateway_entry(full_path: str, request: Request):
    result = await proxy.handle_proxy_request(request)
    return result
