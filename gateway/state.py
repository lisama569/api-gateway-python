"""网关运行时共享状态：配置、路由、限流、ACL、缓存、上游客户端。"""
from __future__ import annotations

from gateway.cache import ResponseCache
from gateway.config import GatewayConfig
from gateway.ratelimit import ACL, RateLimiter
from gateway.router import Router
from gateway.upstream import UpstreamClient


class GatewayState:
    def __init__(self, config: GatewayConfig) -> None:
        self._config = config
        self.router = Router(config)
        self.limiter = RateLimiter()
        self.acl = ACL(config.whitelist, config.blacklist)
        self.cache = ResponseCache()
        self.upstream = UpstreamClient(timeout_seconds=config.timeout_seconds)

    @property
    def config(self) -> GatewayConfig:
        # 策略热切换直接改了 router 内部快照，这里始终取最新值
        return self.router.snapshot()

    @config.setter
    def config(self, value: GatewayConfig) -> None:
        self._config = value
        if hasattr(self, "router"):
            self.router.update(value)

    def apply_config(self, config: GatewayConfig) -> None:
        """热更新配置：路由/策略与 ACL 原子切换，缓存与限流计数保留。"""
        self.router.update(config)
        self.acl.update(config.whitelist, config.blacklist)
        self.upstream._timeout = config.timeout_seconds  # noqa: SLF001

    async def startup(self) -> None:
        await self.upstream.start()

    async def shutdown(self) -> None:
        await self.upstream.close()
