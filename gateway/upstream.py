"""上游实例选择策略（轮询 / 最少连接）与健康状态机。

- round_robin : 在健康实例间按顺序轮转，平局再走轮转。
- least_conn  : 选活动连接数最少的健康实例。
- 健康状态机   : UP -> (report_failure) -> DOWN（冷却 fail_timeout）
              -> 冷却结束 -> HALF_OPEN（放行有限探测，route.half_open_probes）
              -> 探测成功 -> UP 重新参与常规分流；探测失败 -> 回到 DOWN 重新冷却。
- acquire(exclude=...) 可在一组实例中排除指定实例（重试换实例时使用）。
- 所有实例均不可用时抛出 NoHealthyUpstreamError，与“无路由”区分。
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


# 实例状态字
UP = "up"
DOWN = "down"
HALF_OPEN = "half_open"


class _RouteState:
    def __init__(self, route: RouteConfig) -> None:
        self.route = route
        self.lock = threading.Lock()
        self.next_index = 0
        self.active: dict[str, int] = {u.name: 0 for u in route.upstreams}
        self.dead_until: dict[str, float] = {u.name: 0.0 for u in route.upstreams}
        # 半开探测：half_open 标记 + 在途探测数
        self.half_open: dict[str, bool] = {u.name: False for u in route.upstreams}
        self.probes: dict[str, int] = {u.name: 0 for u in route.upstreams}


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
        # 已从路由移除的实例 release 时做 no-op，其半开/探测状态一并清除，不残留。
        if st.route is not route:
            with st.lock:
                if st.route is not route:
                    st.route = route
                    names = {u.name for u in route.upstreams}
                    st.active = {u.name: st.active.get(u.name, 0) for u in route.upstreams}
                    st.dead_until = {
                        u.name: st.dead_until.get(u.name, 0.0) for u in route.upstreams
                    }
                    st.half_open = {
                        u.name: st.half_open.get(u.name, False) for u in route.upstreams
                    }
                    st.probes = {u.name: st.probes.get(u.name, 0) for u in route.upstreams}
                    # 显式丢弃已移除实例的残留（dict 推导已按键过滤，这里仅注释说明）
                    assert set(st.active) == names == set(st.probes)
        return st

    # ---- 状态判定 -------------------------------------------------------
    def _refresh(self, st: _RouteState, now: float) -> None:
        """把冷却到期的 DOWN 实例迁入 HALF_OPEN（惰性状态推进）。"""
        for u in st.route.upstreams:
            du = st.dead_until[u.name]
            if 0.0 < du <= now:
                st.dead_until[u.name] = 0.0
                st.half_open[u.name] = True
                st.probes[u.name] = 0

    def _eligible(self, st: _RouteState, exclude: frozenset[str]) -> list[UpstreamConfig]:
        out = []
        max_probes = max(1, st.route.half_open_probes)
        for u in st.route.upstreams:
            if u.name in exclude:
                continue
            if st.dead_until[u.name] > 0.0:
                continue  # DOWN，冷却中
            if st.half_open[u.name] and st.probes[u.name] >= max_probes:
                continue  # HALF_OPEN 且探测名额已满
            out.append(u)
        return out

    def state_of(self, route_id: str, upstream_name: str) -> str:
        """供日志与测试观察的实例状态。"""
        st = self._states.get(route_id)
        if st is None:
            return UP
        with st.lock:
            now = self._clock()
            self._refresh(st, now)
            if st.dead_until.get(upstream_name, 0.0) > now:
                return DOWN
            if st.half_open.get(upstream_name, False):
                return HALF_OPEN
            return UP

    # ---- 选择 -----------------------------------------------------------
    def acquire(
        self, route: RouteConfig, exclude: frozenset[str] = frozenset()
    ) -> UpstreamConfig:
        st = self._state(route)
        with st.lock:
            now = self._clock()
            self._refresh(st, now)
            eligible = self._eligible(st, exclude)
            if not eligible:
                raise NoHealthyUpstreamError(route.id)
            if route.strategy == Strategy.LEAST_CONN:
                target = min(
                    eligible,
                    key=lambda u: (st.active[u.name], self._order(st, u)),
                )
            else:
                order = self._healthy_order(st, eligible)
                target = order[st.next_index % len(order)]
                st.next_index += 1
            st.active[target.name] += 1
            if st.half_open[target.name]:
                st.probes[target.name] += 1
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

    # ---- 结果上报 -------------------------------------------------------
    def report_success(self, route: RouteConfig, upstream: UpstreamConfig) -> bool:
        """上游返回了 <500 的响应。半开探测成功则恢复为 UP，返回是否发生了恢复。"""
        st = self._state(route)
        with st.lock:
            st.probes[upstream.name] = 0
            if st.half_open.get(upstream.name, False):
                st.half_open[upstream.name] = False
                st.dead_until[upstream.name] = 0.0
                return True
            return False

    def report_failure(self, route: RouteConfig, upstream: UpstreamConfig) -> bool:
        """连接/传输/超时失败：进入 DOWN 重新冷却。返回失败前是否处于半开。"""
        st = self._state(route)
        with st.lock:
            was_probe = st.half_open.get(upstream.name, False)
            st.half_open[upstream.name] = False
            st.probes[upstream.name] = 0
            st.dead_until[upstream.name] = self._clock() + self._fail_timeout
            return was_probe

    def report_5xx(self, route: RouteConfig, upstream: UpstreamConfig) -> bool:
        """上游返回 5xx：仅对半开探测有意义（探测失败回到 DOWN）；
        正常 UP 实例不改变健康状态（保持既有语义）。返回是否触发了回退。"""
        st = self._state(route)
        with st.lock:
            st.probes[upstream.name] = 0
            if st.half_open.get(upstream.name, False):
                st.half_open[upstream.name] = False
                st.dead_until[upstream.name] = self._clock() + self._fail_timeout
                return True
            return False

    # ---- 兼容旧 API -----------------------------------------------------
    def mark_down(self, route: RouteConfig, upstream: UpstreamConfig) -> None:
        self.report_failure(route, upstream)

    def mark_up(self, route: RouteConfig, upstream: UpstreamConfig) -> None:
        st = self._state(route)
        with st.lock:
            st.dead_until[upstream.name] = 0.0
            st.half_open[upstream.name] = False
            st.probes[upstream.name] = 0

    def active_count(self, route_id: str, upstream_name: str) -> int:
        st = self._states.get(route_id)
        if st is None:
            return 0
        with st.lock:
            return st.active.get(upstream_name, 0)
