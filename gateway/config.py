"""网关配置类型定义。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class UpstreamConfig:
    """单个后端实例。"""

    id: str
    base_url: str


@dataclass(frozen=True)
class RateLimitConfig:
    """按 (客户端, 路由) 维度的固定窗口限流。"""

    requests: int = 0  # 0 表示不限流
    window_seconds: float = 1.0


@dataclass(frozen=True)
class RouteConfig:
    """一条路由规则。"""

    id: str
    path_prefix: str
    upstream_ids: tuple[str, ...]
    strategy: str = "round_robin"  # round_robin | least_connections
    rate_limit: RateLimitConfig | None = None
    cache_ttl: float = 0.0  # 0 表示不缓存
    strip_prefix: str = ""


@dataclass(frozen=True)
class GatewayConfig:
    """网关整体配置。"""

    upstreams: tuple[UpstreamConfig, ...] = ()
    routes: tuple[RouteConfig, ...] = ()
    default_rate_limit: RateLimitConfig | None = None
    timeout_seconds: float = 5.0
    whitelist: tuple[str, ...] = ()  # 非空时仅放行这些来源
    blacklist: tuple[str, ...] = ()
