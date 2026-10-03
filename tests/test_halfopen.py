"""半开恢复：熔断冷却后不整批放回，用小流量探测逐步恢复。

- 单元：选择器状态机（冷却 -> 半开 -> 恢复/再熔断）与热更新时的状态适配。
- 端到端：真实假上游 + 网关，观察实例选择、探测与恢复日志。
"""
from __future__ import annotations

import logging
import time

import httpx
import pytest

from gateway.config import RouteConfig, Strategy, UpstreamConfig
from gateway.upstream import UpstreamSelector


@pytest.fixture
def gateway_logs(caplog):
    logger = logging.getLogger("gateway")
    logger.addHandler(caplog.handler)
    caplog.set_level(logging.INFO, logger="gateway")
    yield caplog
    logger.removeHandler(caplog.handler)


def make_route(rid="r", strategy=Strategy.ROUND_ROBIN, names=("a", "b", "c"),
               half_open_probes=1):
    return RouteConfig(
        id=rid,
        prefix="/",
        upstreams=tuple(UpstreamConfig(n, f"http://{n}") for n in names),
        strategy=strategy,
        half_open_probes=half_open_probes,
    )


# ---------- 单元：冷却到期后进入半开，探测名额有限 ----------
def test_halfopen_after_cooldown_and_probe_limit():
    now = [0.0]
    sel = UpstreamSelector(fail_timeout=10, clock=lambda: now[0])
    route = make_route(names=("a", "b"))
    a, b = route.upstreams

    sel.mark_down(route, a)
    assert sel.state_of(route.id, "a") == "down"

    # 冷却到期 -> 半开
    now[0] = 11.0
    assert sel.state_of(route.id, "a") == "half_open"

    # 只允许 1 个并发探测：第一个请求给 a，第二个只能去 b
    first = sel.acquire(route)
    assert first.name == "a"
    second = sel.acquire(route)
    assert second.name == "b"
    assert sel.active_count(route.id, "a") == 1


# ---------- 单元：探测成功 -> 完全恢复 ----------
def test_probe_success_recovers():
    now = [0.0]
    sel = UpstreamSelector(fail_timeout=10, clock=lambda: now[0])
    route = make_route(names=("a", "b"))
    a = route.upstreams[0]

    sel.mark_down(route, a)
    now[0] = 11.0
    probe = sel.acquire(route)
    assert probe.name == a.name
    recovered = sel.report_success(route, a)
    assert recovered is True
    assert sel.state_of(route.id, "a") == "up"
    # 恢复后 a 重新参与常规分流
    names = [sel.acquire(route).name for _ in range(4)]
    assert "a" in names


# ---------- 单元：探测失败 -> 回到熔断，重新冷却 ----------
def test_probe_failure_backs_off():
    now = [0.0]
    sel = UpstreamSelector(fail_timeout=10, clock=lambda: now[0])
    route = make_route(names=("a", "b"))
    a = route.upstreams[0]

    sel.mark_down(route, a)
    now[0] = 11.0
    sel.acquire(route)  # 探测 a
    was_probe = sel.report_failure(route, a)
    assert was_probe is True
    assert sel.state_of(route.id, "a") == "down"
    now[0] = 20.9
    assert sel.state_of(route.id, "a") == "down"  # 重新冷却中
    now[0] = 21.1
    assert sel.state_of(route.id, "a") == "half_open"  # 再次进入半开


# ---------- 单元：熔断期间实例不接收流量 ----------
def test_down_instance_not_selected():
    now = [0.0]
    sel = UpstreamSelector(fail_timeout=100, clock=lambda: now[0])
    route = make_route(names=("a", "b"))
    a = route.upstreams[0]
    sel.mark_down(route, a)
    got = {sel.acquire(route).name for _ in range(5)}
    assert got == {"b"}


# ---------- 单元：热更新保留在途计数、清理被移除实例 ----------
def test_reload_preserves_counts_and_drops_removed():
    sel = UpstreamSelector(fail_timeout=100)
    route_v1 = make_route(names=("a", "b", "c"))
    a, _, c = route_v1.upstreams
    sel.acquire(route_v1)  # a
    sel.acquire(route_v1)  # b
    sel.acquire(route_v1)  # c
    sel.mark_down(route_v1, c)

    # 热更新：c 被移除
    route_v2 = make_route(names=("a", "b"))
    sel._state(route_v2)  # 触发适配
    st = sel._state_for("r")
    assert set(st.active) == {"a", "b"}
    assert set(st.dead_until) == {"a", "b"}
    assert set(st.half_open) == {"a", "b"}
    assert set(st.probes) == {"a", "b"}
    assert st.active["a"] == 1  # 在途计数保留
    assert st.active["b"] == 1

    # 旧请求按 (route_id, name) 释放：c 是 no-op，a 正常递减
    sel.release("r", c)
    assert "c" not in st.active
    sel.release("r", a)
    assert st.active["a"] == 0


# ---------- 单元：热更新后保留实例的半开状态不丢失 ----------
def test_reload_preserves_halfopen_state():
    now = [0.0]
    sel = UpstreamSelector(fail_timeout=10, clock=lambda: now[0])
    route_v1 = make_route(names=("a", "b"))
    a = route_v1.upstreams[0]
    sel.mark_down(route_v1, a)
    now[0] = 11.0
    assert sel.state_of(route_v1.id, "a") == "half_open"

    # 配置对象替换（同一 route id、同一批实例）
    route_v2 = make_route(names=("a", "b"))
    sel._state(route_v2)
    assert sel.state_of(route_v2.id, "a") == "half_open"
    # 半开探测名额仍然受限
    assert sel.acquire(route_v2).name == "a"
    assert sel.acquire(route_v2).name == "b"


# ---------- 端到端：实例恢复后通过探测重新参与分流 ----------
def test_halfopen_recovery_e2e(make_config, gateway_server, upstream_base, gateway_logs):
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a", "b")),
                strategy=Strategy.ROUND_ROBIN,
                cache_ttl=0,
                timeout=0.3,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg, fail_timeout=0.5)

    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 6000})
    r1 = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r1.status_code == 504  # a 超时，被熔断
    r2 = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r2.json()["instance"] == "b"  # 熔断期间只走 b

    # 冷却窗口过后，a 恢复，探测成功 -> 重新参与分流
    time.sleep(0.6)
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 0})
    r3 = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r3.status_code == 200
    assert r3.json()["instance"] == "a"  # 半开探测命中 a
    assert "recovery route=svc upstream=a decision=half_open_to_up" in gateway_logs.text
    r4 = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r4.json()["instance"] == "b"  # 恢复后 a/b 轮询
    r5 = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r5.json()["instance"] == "a"


# ---------- 端到端：探测失败则回到熔断 ----------
def test_probe_failure_goes_back_to_down_e2e(
    make_config, gateway_server, upstream_base, gateway_logs
):
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a", "b")),
                strategy=Strategy.ROUND_ROBIN,
                cache_ttl=0,
                timeout=0.3,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg, fail_timeout=0.5)

    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 6000})
    assert httpx.get(f"{gb}/svc/x", timeout=10).status_code == 504  # a 熔断
    assert httpx.get(f"{gb}/svc/x", timeout=10).json()["instance"] == "b"

    time.sleep(0.6)  # 冷却结束，但 a 仍然慢：探测失败
    r = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r.status_code == 504  # 探测 a 再次超时
    assert "was_probe=True" in gateway_logs.text
    # a 回到熔断，后续请求继续走 b
    assert httpx.get(f"{gb}/svc/x", timeout=10).json()["instance"] == "b"


# ---------- 端到端：热更新移除实例后无残留状态 ----------
def test_reload_removes_instance_state(make_config, gateway_server, upstream_base):
    from gateway.app import create_app
    from tests.conftest import ServerHandle, _free_port

    cfg_v1 = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a", "b")),
                strategy=Strategy.ROUND_ROBIN,
                cache_ttl=0,
            )
        ]
    )
    app = create_app(cfg_v1)
    handle = ServerHandle(app, _free_port()).start()
    try:
        gb = f"http://127.0.0.1:{handle.port}"
        assert httpx.get(f"{gb}/svc/x", timeout=5).status_code == 200
        gateway = app.state.gateway
        selector = gateway.selector
        assert set(selector._states["svc"].active) == {"a", "b"}

        # 热更新：移除实例 b
        cfg_v2 = make_config(
            [
                make_config.route(
                    id="svc",
                    prefix="/svc",
                    upstreams=make_config.ups(("a",)),
                    strategy=Strategy.ROUND_ROBIN,
                    cache_ttl=0,
                )
            ]
        )
        gateway.router.update(cfg_v2)
        r = httpx.get(f"{gb}/svc/x", timeout=5)
        assert r.status_code == 200
        assert r.json()["instance"] == "a"
        # b 的残留状态被清理，只剩 a
        st = selector._states["svc"]
        assert set(st.active) == {"a"}
        assert set(st.dead_until) == {"a"}
        assert set(st.half_open) == {"a"}
        assert set(st.probes) == {"a"}
        # 旧请求对 b 的 release 是 no-op，不产生残留
        selector.release("svc", cfg_v1.routes[0].upstreams[1])
        assert set(st.active) == {"a"}
    finally:
        handle.stop()
