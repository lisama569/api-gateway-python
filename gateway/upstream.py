"""上游实例选择策略（轮询 / 最少连接）、健康熔断与半开恢复。

- round_robin : 在健康实例间按顺序轮转，平局再走轮转。
- least_conn  : 选活动连接数最少的健康实例。
- 熔断        : ``report_failure``/``mark_down`` 后进入冷却（fail_timeout），
                冷却期内不参与选择。
- 半开恢复    : 冷却结束后实例**不直接恢复**，而是进入半开窗口：只允许至多
                ``half_open_probes`` 个并发探测请求打到该实例；探测成功
                （``report_success``）才重新参与常规分流，探测失败
                （``report_failure``）则重新进入冷却。避免冷却一到就把
                全部流量压回刚恢复的实例。
- 重试排除    : ``acquire(route, exclude=...)`` 跳过本次请求已试过的实例，
                用于失败时换到同路由下其它健康实例。
- 热更新适配  : 同一 route id 的新快照保留在途连接计数与半开状态，仅适配
                实例集合；已移除实例的 release/report 均为 no-op，不留残留。
- 所有实例均不可用（健康为空且无空闲探测槽）时抛出 NoHealthyUpstreamError，
  与"无路由"区分。
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
        # dead_until == 0   -> 健康（或已恢复）
        # dead_until > now  -> 熔断冷却中
        # 0 < dead_until <= now -> 半开：只允许少量探测
        self.dead_until: dict[str, float] = {u.name: 0.0 for u in route.upstreams}
        # 半开窗口内在途探测数
        self.probing: dict[str, int] = {u.name: 0 for u in route.upstreams}


class UpstreamSelector:
    def __init__(
        self,
        fail_timeout: float = 10.0,
        clock=time.monotonic,
        half_open_probes: int = 1,
        logger=None,
    ) -> None:
        self._states: dict[str, _RouteState] = {}
        self._guard = threading.Lock()
        self._fail_timeout = fail_timeout
        self._half_open_probes = max(1, half_open_probes)
        self._clock = clock
        self._logger = logger

    # ---- 路由状态 -------------------------------------------------------
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

    def _adapt(self, st: _RouteState, route: RouteConfig) -> _RouteState:
        # 热更新：同一 route id 的新快照，保留在途连接计数与半开状态，
        # 仅适配实例集合。旧快照的在途请求按 (route_id, upstream_name)
        # release/report，计数仍然正确；已移除实例的相关操作做 no-op。
        if st.route is not route:
            with st.lock:
                if st.route is not route:
                    st.route = route
                    names = {u.name for u in route.upstreams}
                    st.active = {n: st.active.get(n, 0) for n in names}
                    st.dead_until = {n: st.dead_until.get(n, 0.0) for n in names}
                    st.probing = {n: st.probing.get(n, 0) for n in names}
        return st

    def _log(self, msg: str, *args) -> None:
        if self._logger is not None:
            self._logger.info(msg, *args)

    # ---- 选择 -----------------------------------------------------------
    def _healthy(self, st: _RouteState, exclude: frozenset[str]) -> list[UpstreamConfig]:
        return [
            u
            for u in st.route.upstreams
            if u.name not in exclude and st.dead_until[u.name] == 0.0
        ]

    def _probe_candidates(
        self, st: _RouteState, now: float, exclude: frozenset[str]
    ) -> list[UpstreamConfig]:
        return [
            u
            for u in st.route.upstreams
            if u.name not in exclude
            and 0.0 < st.dead_until[u.name] <= now
            and st.probing[u.name] < self._half_open_probes
        ]

    def acquire(
        self, route: RouteConfig, exclude: frozenset[str] = frozenset()
    ) -> UpstreamConfig:
        """选择一个可用实例。

        优先在健康实例中按策略选择；存在有空闲探测槽的半开实例时，
        以受控小流量（并发受 half_open_probes 限制）发起探测。
        ``exclude`` 中的实例（本次请求已试过）一律跳过。
        """
        st = self._state(route)
        with st.lock:
            now = self._clock()
            probes = self._probe_candidates(st, now, exclude)
            healthy = self._healthy(st, exclude)
            if probes:
                # 半开探测：冷却刚结束的实例只放小流量试探
                target = min(probes, key=lambda u: (st.probing[u.name], self._order(st, u)))
                st.probing[target.name] += 1
                st.active[target.name] += 1
                self._log(
                    "probe route=%s upstream=%s decision=start inflight_probes=%d",
                    route.id,
                    target.name,
                    st.probing[target.name],
                )
                return target
            if not healthy:
                raise NoHealthyUpstreamError(route.id)
            if route.strategy == Strategy.LEAST_CONN:
                target = min(
                    healthy,
                    key=lambda u: (st.active[u.name], self._order(st, u)),
                )
            else:
                order = sorted(healthy, key=lambda u: self._order(st, u))
                target = order[st.next_index % len(order)]
                st.next_index += 1
            st.active[target.name] += 1
            return target

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
    def _current(self, route_id: str) -> _RouteState | None:
        # 上报/释放只查当前状态，绝不按旧快照反向适配：
        # 在途请求持有的旧 route 对象不能把已热更新的实例集合换回去。
        return self._states.get(route_id)

    def is_probe(self, route_id: str, upstream_name: str) -> bool:
        """该实例当前是否有在途探测（用于代理判定本次尝试是否为探测）。"""
        st = self._current(route_id)
        if st is None:
            return False
        with st.lock:
            return st.probing.get(upstream_name, 0) > 0

    def report_success(self, route: RouteConfig, upstream: UpstreamConfig) -> None:
        """请求成功；若本次是探测，则实例恢复为健康，重新参与常规分流。"""
        st = self._current(route.id)
        if st is None:
            return
        with st.lock:
            if upstream.name not in st.active:
                return  # 实例已从路由移除：no-op，不留残留状态
            if st.probing.get(upstream.name, 0) > 0:
                st.probing[upstream.name] -= 1
                st.dead_until[upstream.name] = 0.0
                self._log(
                    "probe route=%s upstream=%s decision=recovered reason=probe_ok",
                    route.id,
                    upstream.name,
                )

    def report_failure(self, route: RouteConfig, upstream: UpstreamConfig) -> None:
        """请求失败；实例进入（或重新进入）冷却，探测槽释放。"""
        st = self._current(route.id)
        if st is None:
            return
        with st.lock:
            if upstream.name not in st.active:
                return  # 实例已从路由移除：no-op
            was_probe = st.probing.get(upstream.name, 0) > 0
            if was_probe:
                st.probing[upstream.name] -= 1
            st.dead_until[upstream.name] = self._clock() + self._fail_timeout
            if was_probe:
                self._log(
                    "probe route=%s upstream=%s decision=fail reason=probe_failed cooldown=%.1f",
                    route.id,
                    upstream.name,
                    self._fail_timeout,
                )

    # ---- 兼容旧接口 -----------------------------------------------------
    def mark_down(self, route: RouteConfig, upstream: UpstreamConfig) -> None:
        st = self._current(route.id) or self._state(route)
        with st.lock:
            if upstream.name not in st.active:
                return
            st.dead_until[upstream.name] = self._clock() + self._fail_timeout

    def mark_up(self, route: RouteConfig, upstream: UpstreamConfig) -> None:
        st = self._current(route.id) or self._state(route)
        with st.lock:
            if upstream.name not in st.active:
                return
            st.dead_until[upstream.name] = 0.0
            st.probing[upstream.name] = 0

    # ---- 观测 -----------------------------------------------------------
    def active_count(self, route_id: str, upstream_name: str) -> int:
        st = self._states.get(route_id)
        if st is None:
            return 0
        with st.lock:
            return st.active.get(upstream_name, 0)

    def probing_count(self, route_id: str, upstream_name: str) -> int:
        st = self._states.get(route_id)
        if st is None:
            return 0
        with st.lock:
            return st.probing.get(upstream_name, 0)
