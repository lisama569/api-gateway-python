"""轮询 / 最少连接选择策略、健康熔断与冷却恢复。"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from gateway.config import RouteConfig, Strategy, UpstreamConfig
from gateway.upstream import NoHealthyUpstreamError, UpstreamSelector


def make_route(rid="r", strategy=Strategy.ROUND_ROBIN, names=("a", "b", "c")):
    return RouteConfig(
        id=rid,
        prefix="/",
        upstreams=tuple(UpstreamConfig(n, f"http://{n}") for n in names),
        strategy=strategy,
    )

def test_round_robin_cycles():
    sel = UpstreamSelector(fail_timeout=100)
    route = make_route()
    got = [sel.acquire(route).name for _ in range(7)]
    assert got == ["a", "b", "c", "a", "b", "c", "a"]
    for u in route.upstreams:
        sel.release(route.id, u)


def test_least_conn_picks_idlest():
    sel = UpstreamSelector(fail_timeout=100)
    route = make_route(strategy=Strategy.LEAST_CONN)
    u1 = sel.acquire(route)  # a:1
    u2 = sel.acquire(route)  # b:1
    u3 = sel.acquire(route)  # c:1
    assert {u1.name, u2.name, u3.name} == {"a", "b", "c"}
    sel.release(route.id, u2)  # b 空闲，下一个必选 b
    assert sel.acquire(route).name == "b"


def test_mark_down_skips_unhealthy_and_distinct_error():
    sel = UpstreamSelector(fail_timeout=100)
    route = make_route(names=("a", "b"))
    sel.mark_down(route, route.upstreams[1])
    picked = sel.acquire(route)
    assert picked.name == "a"
    sel.release(route.id, picked)
    sel.mark_down(route, route.upstreams[0])
    # b 仍在冷却，a 刚熔断 -> 全部不可用
    try:
        sel.acquire(route)
        raise AssertionError("expected NoHealthyUpstreamError")
    except NoHealthyUpstreamError as exc:
        assert exc.route_id == "r"


def test_cooldown_recovery():
    t = {"v": 0.0}
    sel = UpstreamSelector(fail_timeout=10, clock=lambda: t["v"])
    route = make_route(names=("a",))
    sel.mark_down(route, route.upstreams[0])
    try:
        sel.acquire(route)
        raise AssertionError("expected down")
    except NoHealthyUpstreamError:
        pass
    t["v"] = 11
    assert sel.acquire(route).name == "a"


def test_reload_keeps_inflight_count():
    """热更新后旧在途请求 release 仍正确落到 route id 上。"""
    sel = UpstreamSelector(fail_timeout=100)
    r1 = make_route(names=("a", "b"))
    picked_a = sel.acquire(r1)  # a:1
    picked_b = sel.acquire(r1)  # b:1 (RR)
    r2 = make_route(strategy=Strategy.LEAST_CONN, names=("a", "b", "c"))
    sel._state(r2)  # 触发适配：新增 c，保留 a/b 的在途计数
    assert sel.active_count("r", picked_a.name) == 1
    assert sel.active_count("r", picked_b.name) == 1
    # 新加入的 c 计数为 0，是唯一最闲实例
    assert sel.acquire(r2).name == "c"
    sel.release(r1.id, picked_a)  # 旧快照对象 release
    sel.release(r1.id, picked_b)
    assert sel.active_count("r", picked_a.name) == 0


def test_concurrent_acquire_balances():
    sel = UpstreamSelector(fail_timeout=100)
    route = make_route(names=("a", "b", "c"))
    held = []
    lock = threading.Lock()

    def worker():
        u = sel.acquire(route)
        with lock:
            held.append(u.name)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda _: worker(), range(30)))

    counts = {n: held.count(n) for n in "abc"}
    assert counts == {"a": 10, "b": 10, "c": 10}
