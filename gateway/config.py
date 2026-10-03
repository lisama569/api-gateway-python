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
    """失败重试与故障转移策略（仅对安全方法 GET/HEAD/OPTIONS 生效）。

    - max_attempts: 总尝试次数上限（含首次），>=1；1 表示不重试。
    - per_attempt_timeout: 单次尝试超时（秒）；None 表示沿用 route.timeout。
    - budget: 整个重试过程的整体耗时上限（秒），到期不再发起新尝试。
    - retry_on_statuses: 上游返回这些状态码时视为可重试失败。
    """

    max_attempts: int = 2
    per_attempt_timeout: float | None = None
    budget: float = 10.0
    retry_on_statuses: tuple[int, ...] = (500, 502, 503, 504)


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
    coalesce_wait: float = 10.0


@dataclass(frozen=True)
class GatewayConfig:
    """网关全量配置。

    - fail_timeout: 实例熔断冷却时长（秒），冷却结束后进入半开。
    - half_open_probes: 半开窗口内允许同时在途的探测请求数上限。
    """

    routes: tuple[RouteConfig, ...] = ()
    global_rate_limit: RateLimitRule | None = None
    allowlist: frozenset[str] = frozenset()
    blocklist: frozenset[str] = frozenset()
    fail_timeout: float = 5.0
    half_open_probes: int = 1
