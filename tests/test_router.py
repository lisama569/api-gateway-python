"""路由匹配与配置热更新测试。"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from gateway.config import (
    GatewayConfig,
    RouteConfig,
    Strategy,
    UpstreamConfig,
)
from gateway.router import Router


def R(rid: str, prefix: str) -> RouteConfig:
    return RouteConfig(
        id=rid,
        prefix=prefix,
        upstreams=(UpstreamConfig("a", "http://a"),),
        strategy=Strategy.ROUND_ROBIN,
    )


def test_longest_prefix_wins():
    router = Router(
        GatewayConfig(routes=(R("r1", "/api"), R("r2", "/api/v2"), R("r3", "/")))
    )
    route, _, why = router.match("/api/v2/x")
    assert route.id == "r2"
    assert "prefix=/api/v2" in why
    route, _, _ = router.match("/api/old")
    assert route.id == "r1"
    route, _, _ = router.match("/other")
    assert route.id == "r3"


def test_no_route_is_distinguishable():
    router = Router(GatewayConfig(routes=(R("r1", "/api"),)))
    route, snap, why = router.match("/nope")
    assert route is None
    assert "no_route" in why
    assert snap.routes[0].id == "r1"


def test_atomic_reload_new_requests_see_new_config():
    router = Router(GatewayConfig(routes=(R("r1", "/api"),)))
    v1 = router.current_version
    v2 = router.update(GatewayConfig(routes=(R("r2", "/api"), R("r3", "/admin"))))
    assert v2 > v1
    assert router.match("/admin")[0].id == "r3"
    assert router.match("/api")[0].id == "r2"


def test_inflight_request_keeps_old_snapshot_during_reload():
    """在途请求持有的快照引用不受 reload 影响。"""
    old_cfg = GatewayConfig(routes=(R("r1", "/api"),))
    router = Router(old_cfg)
    seen = []

    def request_with_old_snapshot():
        _route, snap, _ = router.match("/api")
        time.sleep(0.05)  # 模拟在途期间发生热更新
        seen.append(snap.routes[0].id)

    t = threading.Thread(target=request_with_old_snapshot)
    t.start()
    time.sleep(0.01)
    router.update(GatewayConfig(routes=(R("r2", "/api"),)))
    t.join()
    assert seen == ["r1"]
    assert router.match("/api")[0].id == "r2"


def test_concurrent_match_and_reload_never_corrupts():
    router = Router(GatewayConfig(routes=tuple(R(f"r{i}", f"/p{i}") for i in range(8))))
    errors = []

    def match_loop(_):
        try:
            for i in range(200):
                router.match(f"/p{i % 8}/x")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def reload_loop(_):
        try:
            for i in range(50):
                router.update(
                    GatewayConfig(routes=tuple(R(f"r{j}", f"/p{j}") for j in range(8)))
                )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(match_loop, range(4)))
        list(pool.map(reload_loop, range(2)))

    assert errors == []
