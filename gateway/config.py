"""网关配置的数据模型与配置加载。"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Strategy(str, Enum):
    """上游实例选择策略。"""

    ROUND_ROBIN = "round_robin"
    LEAST_CONN = "least_conn"


@dataclass(frozen=True)
class RateLimitRule:
    """令牌桶限流规则：capacity 为桶容量，rate 为每秒补充令牌数。"""

    capacity: int
    rate: float


@dataclass(frozen=True)
class UpstreamConfig:
    """单个上游实例。"""

    name: str
    base_url: str


@dataclass(frozen=True)
class RouteConfig:
    """单条路由配置。"""

    id: str
    prefix: str
    upstreams: tuple[UpstreamConfig, ...]
    strategy: Strategy = Strategy.ROUND_ROBIN
    rate_limit: RateLimitRule | None = None
    cache_ttl: int = 0
    streaming: bool = False
    timeout: float = 5.0


@dataclass(frozen=True)
class GatewayConfig:
    """网关全量配置。"""

    routes: tuple[RouteConfig, ...] = ()
    global_rate_limit: RateLimitRule | None = None
    allowlist: frozenset[str] = frozenset()
    blocklist: frozenset[str] = frozenset()
