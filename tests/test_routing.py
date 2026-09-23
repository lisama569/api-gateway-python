"""路由匹配、轮询/最少连接、热更新不中断、可区分失败原因。"""
from __future__ import annotations

import concurrent.futures

from gateway import proxy
from gateway.config import GatewayConfig, RouteConfig, UpstreamConfig
from gateway.errors import RouteNotFoundError


def test_round_robin_distribution(gw):
    client, _ = gw
    instances = [client.get("/gateway/rr/echo").json()["instance"]
                 for _ in range(6)]
    assert instances.count("u0") == 2
    assert instances.count("u1") == 2
    assert instances.count("u2") == 2
    print("[test] round-robin sequence:", instances)


def test_least_connections_prefers_idle(gw):
    client, state = gw
    # 确定性验证：直接驱动 Router 的选择/释放，模拟在途活跃计数
    router = state.router
    route = next(r for r in router.snapshot().routes if r.id == "lc")

    first = router.select(route).id           # u0 active=1
    second = router.select(route).id          # u1 active=1（u0 忙）
    third = router.select(route).id           # u2 active=1
    assert (first, second, third) == ("u0", "u1", "u2")
    router.release(first)                      # u0 active=0
    fourth = router.select(route).id           # 应回到 u0
    assert fourth == "u0", (first, second, third, fourth)
    for uid in (second, third, fourth):
        router.release(uid)
    assert all(v == 0 for v in router.stats()["active"].values())

    # 集成层：慢请求在途重叠时，统计中三个实例都被选中过
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futs = [pool.submit(lambda: client.get(
            "/gateway/lc/slow", params={"ms": 100}).status_code)
            for _ in range(6)]
        assert all(f.result() == 200 for f in futs)
    picked = {k.split("->")[1] for k in router.stats()["selection_count"]
              if k.startswith("lc->")}
    assert picked == {"u0", "u1", "u2"}
    print("[test] least-connections deterministic + integration ok:",
          router.stats())


def test_concurrent_least_connections_balances(gw):
    client, state = gw

    def hit(i):
        # slow 让请求在途重叠，观察 active 分配
        return client.get("/gateway/lc/slow", params={"ms": 150}).json()["instance"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(hit, range(6)))
    counts = {u: results.count(u) for u in set(results)}
    print("[test] concurrent LC distribution:", counts)
    # 最少连接下三个实例都应承担流量，且最大/最小之差不超过 1
    assert set(counts) == {"u0", "u1", "u2"}, counts
    assert max(counts.values()) - min(counts.values()) <= 1, counts


def test_no_route_distinguishable(gw):
    client, _ = gw
    r = client.get("/gateway/does-not-exist")
    assert r.status_code == 404
    body = r.json()
    assert body["error"] == "route_not_found"
    assert body["retryable"] is False


def test_misconfigured_upstream(gw):
    client, _ = gw
    r = client.get("/gateway/broken/echo")
    assert r.status_code == 404
    assert r.json()["error"] == "route_not_found"


def test_strategy_hot_switch(gw):
    client, state = gw
    r = client.post("/admin/routes/rr/strategy", json={"strategy": "least_connections"})
    assert r.status_code == 200
    rr = next(r for r in state.router.snapshot().routes if r.id == "rr")
    assert rr.strategy == "least_connections"
    r = client.post("/admin/routes/rr/strategy", json={"strategy": "bogus"})
    assert r.status_code == 400
    client.post("/admin/routes/rr/strategy", json={"strategy": "round_robin"})


def test_config_hot_reload_does_not_break_inflight(gw, upstreams):
    client, state = gw
    import threading

    errors = []

    def slow_requests():
        for _ in range(5):
            try:
                r = client.get("/gateway/rr/slow", params={"ms": 100})
                if r.status_code != 200:
                    errors.append(r.status_code)
            except Exception as exc:  # noqa: BLE001
                errors.append(repr(exc))

    t = threading.Thread(target=slow_requests)
    t.start()

    # 在途期间反复热更新（含切换路由/上游集合）
    for i in range(3):
        ids = ("u0", "u1", "u2")
        routes = (RouteConfig(
            id="rr", path_prefix="/gateway/rr", upstream_ids=ids,
            strategy="least_connections" if i % 2 else "round_robin"),)
        state.apply_config(GatewayConfig(
            upstreams=tuple(UpstreamConfig(id=k, base_url=v)
                            for k, v in upstreams.items()),
            routes=routes, timeout_seconds=1.0))
    t.join()
    assert errors == [], f"inflight requests interrupted: {errors}"


def test_router_unit_match_prefix():
    cfg = GatewayConfig(
        upstreams=(UpstreamConfig(id="a", base_url="http://x"),),
        routes=(RouteConfig(id="r1", path_prefix="/gateway/rr",
                            upstream_ids=("a",)),),
    )
    from gateway.router import Router
    router = Router(cfg)
    assert router.match("GET", "/gateway/rr/echo").id == "r1"
    assert router.match("GET", "/gateway/rr").id == "r1"
    try:
        router.match("GET", "/other")
        assert False
    except RouteNotFoundError:
        pass
