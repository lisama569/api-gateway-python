"""本地演示配置：指向本地假上游的多实例路由。

默认假上游基址 http://127.0.0.1:9101/{instance}，
可用环境变量 GATEWAY_UPSTREAM_BASE 覆盖。
"""
from __future__ import annotations

import os

from .config import GatewayConfig, RateLimitRule, RouteConfig, Strategy, UpstreamConfig


def build_local_config(base: str | None = None) -> GatewayConfig:
    base = base or os.environ.get("GATEWAY_UPSTREAM_BASE", "http://127.0.0.1:9101")

    def up(names: tuple[str, ...]) -> tuple[UpstreamConfig, ...]:
        return tuple(UpstreamConfig(name=n, base_url=f"{base}/{n}") for n in names)

    return GatewayConfig(
        routes=(
            RouteConfig(
                id="users",
                prefix="/api/users",
                upstreams=up(("a", "b")),
                strategy=Strategy.ROUND_ROBIN,
                rate_limit=RateLimitRule(capacity=3, rate=1.0),
                cache_ttl=3,
            ),
            RouteConfig(
                id="orders",
                prefix="/api/orders",
                upstreams=up(("a", "b", "c")),
                strategy=Strategy.LEAST_CONN,
                rate_limit=RateLimitRule(capacity=4, rate=2.0),
                cache_ttl=0,
            ),
            RouteConfig(
                id="stream",
                prefix="/stream",
                upstreams=up(("a",)),
                strategy=Strategy.ROUND_ROBIN,
                streaming=True,
                timeout=10.0,
            ),
        ),
        global_rate_limit=RateLimitRule(capacity=100, rate=50.0),
        allowlist=frozenset(),
        blocklist=frozenset(),
    )
