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
class RetryConfig:
    """重试与故障转移配置（仅对幂等的安全方法生效）。

    max_attempts    总尝试次数上限（含首次），1 表示不重试（默认，保持原行为）。
    per_try_timeout 单次尝试超时；None 表示沿用路由 timeout。
    budget          整个请求（含所有重试）的耗时上限（秒）。
    retry_on_5xx    上游返回 5xx 时是否换实例重试（连接错误/超时总是可重试）。
    """

    max_attempts: int = 1
    per_try_timeout: float | None = None
    budget: float = 10.0
    retry_on_5xx: bool = True


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
    retry: RetryConfig | None = None
    # 缓存未命中时同键请求合并回源的等待上限（秒）；0 表示不合并。
    coalesce_wait: float = 10.0
    # 熔断冷却后允许同时进入半开探测的请求数。
    half_open_probes: int = 1


@dataclass(frozen=True)
class GatewayConfig:
    """网关全量配置。"""

    routes: tuple[RouteConfig, ...] = ()
    global_rate_limit: RateLimitRule | None = None
    allowlist: frozenset[str] = frozenset()
    blocklist: frozenset[str] = frozenset()
