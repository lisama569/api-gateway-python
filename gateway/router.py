"""路由匹配与配置热更新。

热更新语义：``update`` 以不可变快照原子替换当前配置；已经进入处理流程的
请求持有旧快照引用并继续使用旧配置，新请求立即看到新配置，互不影响，
在途请求不会因 reload 而中断或看到半更新状态。
"""
from __future__ import annotations

import itertools
import threading

from .config import GatewayConfig, RouteConfig


class Router:
    def __init__(self, config: GatewayConfig) -> None:
        self._lock = threading.Lock()
        self._snapshot: GatewayConfig = config
        self._version = itertools.count(1)
        self.current_version = next(self._version)

    def match(self, path: str) -> tuple[RouteConfig | None, GatewayConfig, str]:
        """在当前快照上做最长前缀匹配。

        返回 (路由或 None, 所使用的配置快照, 匹配原因)。
        """
        snap = self._snapshot  # 单次取引用；即便期间 reload 也不影响本请求
        best: RouteConfig | None = None
        for route in snap.routes:
            if path.startswith(route.prefix) and (
                best is None or len(route.prefix) > len(best.prefix)
            ):
                best = route
        if best is None:
            return None, snap, f"no_route path={path}"
        return best, snap, f"prefix={best.prefix} route={best.id}"

    def update(self, config: GatewayConfig) -> int:
        """原子替换全量配置，返回新版本号。"""
        with self._lock:
            self._snapshot = config
            self.current_version = next(self._version)
            return self.current_version

    def snapshot(self) -> GatewayConfig:
        return self._snapshot
