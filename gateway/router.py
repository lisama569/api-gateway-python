"""路由匹配与上游选择策略（轮询 / 最少连接），支持运行期热更新。

热更新通过原子替换配置快照实现：在途请求已经拿到的 UpstreamConfig 与
active 计数不受影响，update() 之后新请求才使用新配置，故不中断在途请求。
"""
from __future__ import annotations

import itertools
import threading

from gateway.config import GatewayConfig, RouteConfig, UpstreamConfig
from gateway.errors import RouteNotFoundError

STRATEGIES = ("round_robin", "least_connections")


class Router:
    def __init__(self, config: GatewayConfig) -> None:
        self._lock = threading.RLock()
        self._config = config
        # upstream_id -> 在途连接数
        self._active: dict[str, int] = {u.id: 0 for u in config.upstreams}
        # route_id -> 轮询下标
        self._rr: dict[str, int] = {}
        # (route_id, upstream_id) -> 累计选中次数
        self.selection_count: dict[tuple[str, str], int] = {}

    # ---- 热更新 ----------------------------------------------------------
    def update(self, config: GatewayConfig) -> None:
        with self._lock:
            self._config = config
            for u in config.upstreams:
                self._active.setdefault(u.id, 0)
        print("[router] config hot-reloaded: "
              f"{len(config.upstreams)} upstreams, {len(config.routes)} routes")

    def snapshot(self) -> GatewayConfig:
        return self._config

    def set_strategy(self, route_id: str, strategy: str) -> None:
        if strategy not in STRATEGIES:
            raise ValueError(f"unknown strategy: {strategy}")
        with self._lock:
            old = self._config
            routes = tuple(
                RouteConfig(
                    id=r.id, path_prefix=r.path_prefix, upstream_ids=r.upstream_ids,
                    strategy=strategy if r.id == route_id else r.strategy,
                    rate_limit=r.rate_limit, cache_ttl=r.cache_ttl,
                    strip_prefix=r.strip_prefix,
                )
                for r in old.routes
            )
            if not any(r.id == route_id for r in routes):
                raise RouteNotFoundError(f"route {route_id} not found")
            self._config = GatewayConfig(
                upstreams=old.upstreams, routes=routes,
                default_rate_limit=old.default_rate_limit,
                timeout_seconds=old.timeout_seconds,
                whitelist=old.whitelist, blacklist=old.blacklist,
            )
        print(f"[router] route={route_id} strategy -> {strategy}")

    # ---- 匹配 ------------------------------------------------------------
    def match(self, method: str, path: str) -> RouteConfig:
        config = self._config
        best: RouteConfig | None = None
        for route in config.routes:
            if path == route.path_prefix.rstrip("/") or \
                    path.startswith(route.path_prefix.rstrip("/") + "/"):
                if best is None or len(route.path_prefix) > len(best.path_prefix):
                    best = route
        if best is None:
            print(f"[router] MATCH MISS method={method} path={path}")
            raise RouteNotFoundError(f"no route for {method} {path}")
        print(f"[router] MATCH HIT  method={method} path={path} -> route={best.id}")
        return best

    def upstream_of(self, upstream_id: str) -> UpstreamConfig:
        for u in self._config.upstreams:
            if u.id == upstream_id:
                return u
        raise RouteNotFoundError(f"upstream {upstream_id} not found")

    # ---- 选择 ------------------------------------------------------------
    def select(self, route: RouteConfig) -> UpstreamConfig:
        with self._lock:
            ids = [uid for uid in route.upstream_ids
                   if any(u.id == uid for u in self._config.upstreams)]
            if not ids:
                raise RouteNotFoundError(
                    f"route {route.id} has no available upstream")
            if route.strategy == "round_robin":
                idx = self._rr.get(route.id, 0)
                chosen = ids[idx % len(ids)]
                self._rr[route.id] = idx + 1
            elif route.strategy == "least_connections":
                tie = itertools.count()
                chosen = min(
                    ids,
                    key=lambda uid: (self._active.get(uid, 0), next(tie)),
                )
            else:
                raise ValueError(f"unknown strategy: {route.strategy}")
            self._active[chosen] = self._active.get(chosen, 0) + 1
            key = (route.id, chosen)
            self.selection_count[key] = self.selection_count.get(key, 0) + 1
            print(f"[router] SELECT route={route.id} strategy={route.strategy} "
                  f"upstream={chosen} active={self._active[chosen]} "
                  f"active_map={dict(sorted(self._active.items()))}")
            return self.upstream_of(chosen)

    def release(self, upstream_id: str) -> None:
        with self._lock:
            n = max(0, self._active.get(upstream_id, 0) - 1)
            self._active[upstream_id] = n
            print(f"[router] RELEASE upstream={upstream_id} active={n}")

    def stats(self) -> dict[str, object]:
        with self._lock:
            return {
                "active": dict(self._active),
                "selection_count": {
                    f"{route}->{up}": n
                    for (route, up), n in sorted(self.selection_count.items())
                },
            }
