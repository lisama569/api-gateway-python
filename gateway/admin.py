"""管理接口：热更新路由、切换选择策略、查看计数。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from gateway.config import (
    GatewayConfig,
    RateLimitConfig,
    RouteConfig,
    UpstreamConfig,
)


class UpstreamIn(BaseModel):
    id: str
    base_url: str


class RateLimitIn(BaseModel):
    requests: int = 0
    window_seconds: float = 1.0


class RouteIn(BaseModel):
    id: str
    path_prefix: str
    upstream_ids: list[str]
    strategy: str = "round_robin"
    rate_limit: RateLimitIn | None = None
    cache_ttl: float = 0.0
    strip_prefix: str = ""


class ConfigIn(BaseModel):
    upstreams: list[UpstreamIn]
    routes: list[RouteIn]
    default_rate_limit: RateLimitIn | None = None
    timeout_seconds: float = 5.0
    whitelist: list[str] = []
    blacklist: list[str] = []


class StrategyIn(BaseModel):
    strategy: str


def _to_config(payload: ConfigIn) -> GatewayConfig:
    upstreams = tuple(UpstreamConfig(id=u.id, base_url=u.base_url)
                      for u in payload.upstreams)
    routes = tuple(
        RouteConfig(
            id=r.id, path_prefix=r.path_prefix,
            upstream_ids=tuple(r.upstream_ids), strategy=r.strategy,
            rate_limit=RateLimitConfig(
                requests=r.rate_limit.requests,
                window_seconds=r.rate_limit.window_seconds,
            ) if r.rate_limit else None,
            cache_ttl=r.cache_ttl, strip_prefix=r.strip_prefix,
        )
        for r in payload.routes
    )
    default = (
        RateLimitConfig(payload.default_rate_limit.requests,
                        payload.default_rate_limit.window_seconds)
        if payload.default_rate_limit else None
    )
    return GatewayConfig(
        upstreams=upstreams, routes=routes, default_rate_limit=default,
        timeout_seconds=payload.timeout_seconds,
        whitelist=tuple(payload.whitelist), blacklist=tuple(payload.blacklist),
    )


def _route_json(r: RouteConfig) -> dict[str, object]:
    return {
        "id": r.id, "path_prefix": r.path_prefix,
        "upstream_ids": list(r.upstream_ids), "strategy": r.strategy,
        "cache_ttl": r.cache_ttl, "strip_prefix": r.strip_prefix,
        "rate_limit": None if r.rate_limit is None else {
            "requests": r.rate_limit.requests,
            "window_seconds": r.rate_limit.window_seconds,
        },
    }


def build_admin_router(gateway_state) -> APIRouter:
    router = APIRouter(prefix="/admin", tags=["admin"])

    @router.get("/config")
    def get_config() -> dict[str, object]:
        cfg = gateway_state.config
        return {
            "upstreams": [{"id": u.id, "base_url": u.base_url}
                          for u in cfg.upstreams],
            "routes": [_route_json(r) for r in cfg.routes],
            "default_rate_limit": None if cfg.default_rate_limit is None
            else {"requests": cfg.default_rate_limit.requests,
                  "window_seconds": cfg.default_rate_limit.window_seconds},
            "timeout_seconds": cfg.timeout_seconds,
            "whitelist": list(cfg.whitelist),
            "blacklist": list(cfg.blacklist),
        }

    @router.put("/config")
    def replace_config(payload: ConfigIn) -> dict[str, str]:
        cfg = _to_config(payload)
        gateway_state.apply_config(cfg)
        return {"status": "reloaded"}

    @router.post("/routes/{route_id}/strategy")
    def set_strategy(route_id: str, payload: StrategyIn) -> dict[str, str]:
        try:
            gateway_state.router.set_strategy(route_id, payload.strategy)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"status": "updated", "route": route_id,
                "strategy": payload.strategy}

    @router.get("/stats")
    def stats() -> dict[str, object]:
        return {
            "router": gateway_state.router.stats(),
            "cache": gateway_state.cache.stats(),
        }

    @router.post("/cache/clear")
    def clear_cache() -> dict[str, str]:
        gateway_state.cache.clear()
        return {"status": "cleared"}

    @router.post("/ratelimit/reset")
    def reset_rate_limit() -> dict[str, str]:
        gateway_state.limiter.reset()
        return {"status": "reset"}

    return router
