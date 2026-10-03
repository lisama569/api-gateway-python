"""半开恢复：熔断后不整批放回，用小流量探测逐步恢复。

覆盖：探测槽位上限、探测成功恢复、探测失败重新冷却、
热更新保留在途计数且清除已移除实例的残留状态。
"""
from __future__ import annotations

import concurrent.futures
import time

import httpx

from gateway.config import RouteConfig, Strategy, UpstreamConfig
from gateway.upstream import NoHealthyUpstreamError, UpstreamSelector


def make_route(rid="r", strategy=Strategy.ROUND_ROBIN, names=("a", "b")):
    return RouteConfig(
        id=rid,
        prefix="/",
        upstreams=tuple(UpstreamConfig(n, f"http://{n}") for n in names),
        strategy=strategy,
    )


# ---------- 冷却结束后不直接恢复，只放探测 ----------
def test_half_open_probe_slot_limits_traffic():
    t = {"v": 0.0}
    sel = UpstreamSelector(fail_timeout=10, clock=lambda: t["v"], half_open_probes=1)
    route = make_route(names=("a",))
    sel.mark_down(route, route.upstreams[0])
    t["v"] = 11  # 冷却结束
    probe = sel.acquire(route)  # 半开：放出 1 个探测
    assert probe.name == "a"
    assert sel.probing_count("r", "a") == 1
    # 探测在途期间，槽位占满 -> 不再放行（不整批放回）
    try:
        sel.acquire(route)
        raise AssertionError("expected NoHealthyUpstreamError")
    except NoHealthyUpstreamError:
        pass


# ---------- 探测成功：实例重新参与常规分流 ----------
def test_probe_success_restores_instance():
    t = {"v": 0.0}
    sel = UpstreamSelector(fail_timeout=10, clock=lambda: t["v"], half_open_probes=1)
    route = make_route(names=("a",))
    sel.mark_down(route, route.upstreams[0])
    t["v"] = 11
    probe = sel.acquire(route)
    sel.report_success(route, probe)
    sel.release(route.id, probe)
    assert sel.probing_count("r", "a") == 0
    # 已恢复健康：后续 acquire 正常选中
    assert sel.acquire(route).name == "a"


# ---------- 探测失败：重新进入冷却 ----------
def test_probe_failure_reenters_cooldown():
    t = {"v": 0.0}
    sel = UpstreamSelector(fail_timeout=10, clock=lambda: t["v"], half_open_probes=1)
    route = make_route(names=("a",))
    sel.mark_down(route, route.upstreams[0])
    t["v"] = 11
    probe = sel.acquire(route)
    sel.report_failure(route, probe)  # 探测失败 -> 重新冷却
    sel.release(route.id, probe)
    assert sel.probing_count("r", "a") == 0
    t["v"] = 15  # 旧冷却窗口内
    try:
        sel.acquire(route)
        raise AssertionError("expected NoHealthyUpstreamError")
    except NoHealthyUpstreamError:
        pass
    t["v"] = 22  # 新冷却结束后可再次探测
    assert sel.acquire(route).name == "a"


# ---------- 多实例：健康实例照常分流，半开实例只收探测小流量 ----------
def test_probe_does_not_steal_healthy_traffic():
    t = {"v": 0.0}
    sel = UpstreamSelector(fail_timeout=10, clock=lambda: t["v"], half_open_probes=1)
    route = make_route(names=("a", "b"))
    sel.mark_down(route, route.upstreams[0])  # a 熔断
    t["v"] = 11  # a 进入半开
    first = sel.acquire(route)
    assert first.name == "a"  # 空闲探测槽：放一个探测
    second = sel.acquire(route)
    assert second.name == "b"  # 探测槽占满后，流量走健康实例
    third = sel.acquire(route)
    assert third.name == "b"


# ---------- 热更新：在途计数不清零，移除实例不留残留 ----------
def test_reload_keeps_inflight_and_drops_removed_probe_state():
    t = {"v": 0.0}
    sel = UpstreamSelector(fail_timeout=10, clock=lambda: t["v"], half_open_probes=1)
    r1 = make_route(names=("a", "b"))
    sel.mark_down(r1, r1.upstreams[1])  # b 熔断
    t["v"] = 11
    probe = sel.acquire(r1)  # b 的探测在途
    assert probe.name == "b"
    held = sel.acquire(r1)  # a 的正常请求在途
    assert held.name == "a"
    # 热更新：b 从路由移除，新增 c
    r2 = make_route(names=("a", "c"))
    sel._state(r2)
    # 在途计数不清零
    assert sel.active_count("r", "a") == 1
    # 已移除实例的探测状态被清除，迟到的 report/release 是 no-op
    assert sel.probing_count("r", "b") == 0
    sel.report_success(r1, probe)  # 旧快照对象的迟到上报：不炸、不留残留
    sel.release(r1.id, probe)
    assert sel.probing_count("r", "b") == 0
    assert "b" not in sel._states["r"].dead_until
    # 新实例 c 正常参与选择
    sel.release(r1.id, held)
    picked = {sel.acquire(r2).name for _ in range(4)}
    assert picked <= {"a", "c"}


# ---------- 端到端：实例恢复后经探测重新接管流量 ----------
def test_e2e_half_open_recovery(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 3000})
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a",)),
                cache_ttl=0,
                timeout=0.3,
            )
        ],
        fail_timeout=1.0,
        half_open_probes=1,
    )
    _, start = gateway_server
    gb = start(cfg)
    r1 = httpx.get(f"{gb}/svc/x", timeout=5)
    assert r1.status_code == 504  # 超时 -> 熔断
    r2 = httpx.get(f"{gb}/svc/x", timeout=5)
    assert r2.status_code == 503  # 冷却期：无可用实例
    assert r2.json()["error"] == "no_upstream"
    # 上游恢复，但冷却未结束仍 503
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 0})
    r3 = httpx.get(f"{gb}/svc/x", timeout=5)
    assert r3.status_code == 503
    # 冷却结束：下一个请求作为探测放行并成功 -> 实例恢复
    time.sleep(1.1)
    r4 = httpx.get(f"{gb}/svc/x", timeout=5)
    assert r4.status_code == 200
    # 恢复后正常分流
    r5 = httpx.get(f"{gb}/svc/x", timeout=5)
    assert r5.status_code == 200


# ---------- 端到端：探测失败重新冷却，不整批放回 ----------
def test_e2e_probe_failure_reenters_cooldown(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 3000})
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a",)),
                cache_ttl=0,
                timeout=0.3,
            )
        ],
        fail_timeout=1.0,
        half_open_probes=1,
    )
    _, start = gateway_server
    gb = start(cfg)
    assert httpx.get(f"{gb}/svc/x", timeout=5).status_code == 504  # 熔断
    time.sleep(1.1)  # 冷却结束，但上游仍然慢
    t0 = time.monotonic()
    r_probe = httpx.get(f"{gb}/svc/x", timeout=5)  # 探测：仍超时 -> 重新冷却
    assert r_probe.status_code == 504
    # 探测在途/刚失败时，并发与后续请求不会被整批放到 a 上
    r_after = httpx.get(f"{gb}/svc/x", timeout=5)
    assert r_after.status_code == 503
    assert time.monotonic() - t0 < 3.0  # 没有干等上游的 3s 慢响应
    # 上游恢复 + 新冷却结束后，探测成功恢复
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 0})
    time.sleep(1.1)
    assert httpx.get(f"{gb}/svc/x", timeout=5).status_code == 200


# ---------- 端到端：探测并发受限，恢复后不冲击单实例 ----------
def test_e2e_probe_concurrency_limited(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 2000})
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a",)),
                cache_ttl=0,
                timeout=0.3,
            )
        ],
        fail_timeout=0.8,
        half_open_probes=1,
    )
    _, start = gateway_server
    gb = start(cfg)
    assert httpx.get(f"{gb}/svc/x", timeout=5).status_code == 504  # 熔断 a
    time.sleep(0.9)  # 冷却结束；上游仍慢（探测会挂 2s）
    # 冷却刚结束的窗口里并发打 6 个请求
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futs = [pool.submit(lambda: httpx.get(f"{gb}/svc/y", timeout=8)) for _ in range(6)]
        results = [f.result() for f in futs]
    statuses = [r.status_code for r in results]
    # 只有 1 个探测真正打到 a（最终 504），其余在探测占槽期间拿到 503，
    # 不会整批压到刚半开的实例上
    assert statuses.count(504) == 1, statuses
    assert statuses.count(503) == 5, statuses
