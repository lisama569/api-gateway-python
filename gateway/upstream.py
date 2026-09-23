"""上游实例选择策略（轮询 / 最少连接）与健康状态。

- round_robin : 在健康实例间按顺序轮转，平局再走轮转。
- least_conn  : 选活动连接数最少的健康实例。
- 健康管理   : ``mark_down`` 后进入冷却（fail_timeout），冷却结束可重新参与选择；
              所有实例均不健康时抛出 NoHealthyUpstreamError，与“无路由”区分。
"""
from __future__ import annotations

import threading
import time

from .config import RouteConfig, Strategy, UpstreamConfig


class NoHealthyUpstreamError(Exception):
    """路由存在但没有任何可用上游实例（区别于无匹配路由）。"""

    def __init__(self, route_id: str) -> None:
        super().__init__(f"no healthy upstream for route={route_id}")
        self.route_id = route_id


class _RouteState:
    def __init__(self, route: RouteConfig) -> None:
        self.route = route
        self.lock = threading.Lock()
        self.next_index = 0
        self.active: dict[str, int] = {u.name: 0 for u in route.upstreams}
        self.dead_until: dict[str, float] = {u.name: 0.0 for u in route.upstreams}


class UpstreamSelector:
    def __init__(self, fail_timeout: float = 10.0, clock=time.monotonic) -> None:
        self._states: dict[str, _RouteState] = {}
        self._guard = threading.Lock()
        self._fail_timeout = fail_timeout
        self._clock = clock

    def _state(self, route: RouteConfig) -> _RouteState:
        st = self._states.get(route.id)
        if st is None:
            with self._guard:
                st = self._states.get(route.id)
                if st is None:
                    st = _RouteState(route)
                    self._states[route.id] = st
                    return st
        return self._adapt(st, route)

    def _state_for(self, route_id: str) -> _RouteState | None:
        return self._states.get(route_id)

    def _adapt(self, st: _RouteState, route: RouteConfig) -> _RouteState:
        # 热更新：同一 route id 的新快照，保留在途连接计数，仅适配实例集合。
        # 旧快照的在途请求按 (route_id, upstream_name) release，计数仍然正确；
        # 已从路由移除的实例 release 时做 no-op。
        if st.route is not route:
            with st.lock:
                if st.route is not route:
                    st.route = route
                    st.active = {
                        u.name: st.active.get(u.name, 0) for u in route.upstreams
                    }
                    st.dead_until = {
                        u.name: st.dead_until.get(u.name, 0.0)
                        for u in route.upstreams
                    }
        return st

    def _healthy(self, st: _RouteState, now: float) -> list[UpstreamConfig]:
        return [u for u in st.route.upstreams if st.dead_until[u.name] <= now]

    def acquire(self, route: RouteConfig) -> UpstreamConfig:
        st = self._state(route)
        with st.lock:
            now = self._clock()
            healthy = self._healthy(st, now)
            if not healthy:
                raise NoHealthyUpstreamError(route.id)
            if route.strategy == Strategy.LEAST_CONN:
                target = min(
                    healthy,
                    key=lambda u: (st.active[u.name], self._order(st, u)),
                )
            else:
                order = self._healthy_order(st, healthy)
                target = order[st.next_index % len(order)]
                st.next_index += 1
            st.active[target.name] += 1
            return target

    @staticmethod
    def _healthy_order(
        st: _RouteState, healthy: list[UpstreamConfig]
    ) -> list[UpstreamConfig]:
        return sorted(healthy, key=lambda u: st.route.upstreams.index(u))

    def _order(self, st: _RouteState, u: UpstreamConfig) -> int:
        return st.route.upstreams.index(u)

    def release(self, route_id: str, upstream: UpstreamConfig) -> None:
        st = self._states.get(route_id)
        if st is None:
            return
        with st.lock:
            if st.active.get(upstream.name, 0) > 0:
                st.active[upstream.name] -= 1

    def mark_down(self, route: RouteConfig, upstream: UpstreamConfig) -> None:
        st = self._state(route)
        with st.lock:
            st.dead_until[upstream.name] = self._clock() + self._fail_timeout

    def mark_up(self, route: RouteConfig, upstream: UpstreamConfig) -> None:
        st = self._state(route)
        with st.lock:
            st.dead_until[upstream.name] = 0.0

    def active_count(self, route_id: str, upstream_name: str) -> int:
        st = self._states.get(route_id)
        if st is None:
            return 0
        with st.lock:
            return st.active.get(upstream_name, 0)
