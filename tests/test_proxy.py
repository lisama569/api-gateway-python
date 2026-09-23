"""端到端：真实本地假上游 + 网关（真实 TCP/uvicorn）。"""
from __future__ import annotations

import concurrent.futures
import time

import httpx
import pytest

from gateway.config import RateLimitRule, Strategy


# ---------- 路由分发：轮询 ----------
def test_round_robin_distribution(make_config, gateway_server, upstream_base):
    cfg = make_config(
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
    _base, start = gateway_server
    gb = start(cfg)
    got = [httpx.get(f"{gb}/svc/x", timeout=5).json()["instance"] for _ in range(4)]
    assert got == ["a", "b", "a", "b"]


# ---------- 路由分发：最少连接 ----------
def test_least_conn_distribution(make_config, gateway_server, upstream_base):
    # 给假上游统一加延迟，使请求在途重叠，才能体现 least-conn 差异
    for n in ("a", "b", "c"):
        httpx.post(f"{upstream_base}/{n}/state/slow", params={"ms": 150})
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a", "b", "c")),
                strategy=Strategy.LEAST_CONN,
                cache_ttl=0,
                timeout=5.0,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futs = [
            pool.submit(lambda: httpx.get(f"{gb}/svc/x", timeout=5))
            for _ in range(6)
        ]
        got = [f.result().json()["instance"] for f in futs]
    counts = {n: got.count(n) for n in "abc"}
    assert all(c == 2 for c in counts.values()), counts


# ---------- 无匹配路由 ----------
def test_no_route_distinguishable(make_config, gateway_server):
    base, _ = gateway_server
    r = httpx.get(f"{base}/does/not/exist", timeout=5)
    assert r.status_code == 404
    assert r.json()["error"] == "no_route"


# ---------- 缓存命中不再访问上游 ----------
def test_cache_hit_skips_upstream(make_config, gateway_server, upstream_base):
    cfg = make_config(
        [
            make_config.route(
                id="c",
                prefix="/c",
                upstreams=make_config.ups(("a", "b")),
                strategy=Strategy.ROUND_ROBIN,
                cache_ttl=30,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r1 = httpx.get(f"{gb}/c/data", timeout=5)
    r2 = httpx.get(f"{gb}/c/data", timeout=5)
    r3 = httpx.get(f"{gb}/c/data", timeout=5)
    assert r1.headers["x-cache"] == "MISS"
    assert r2.headers["x-cache"] == "HIT"
    assert r3.headers["x-cache"] == "HIT"
    # 轮询路由若每次都转发，a/b 都会被打；缓存命中后上游仅 1 次命中
    hits = {
        n: httpx.get(f"{upstream_base}/{n}/state").json()["hits"] for n in ("a", "b")
    }
    assert sum(hits.values()) == 1


# ---------- 条件请求：304 ----------
def test_conditional_request_returns_304(make_config, gateway_server):
    cfg = make_config(
        [
            make_config.route(
                id="c",
                prefix="/c",
                upstreams=make_config.ups(("a",)),
                cache_ttl=30,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r1 = httpx.get(f"{gb}/c/et", timeout=5)
    etag = r1.headers["etag"]
    r2 = httpx.get(f"{gb}/c/et", headers={"if-none-match": etag}, timeout=5)
    assert r2.status_code == 304
    assert r2.headers["etag"] == etag
    assert r2.content == b""
    # 不同 etag 应正常 200
    r3 = httpx.get(f"{gb}/c/et", headers={"if-none-match": '"other"'}, timeout=5)
    assert r3.status_code == 200


def test_ims_conditional_304(make_config, gateway_server):
    cfg = make_config(
        [
            make_config.route(
                id="c", prefix="/c", upstreams=make_config.ups(("a",)), cache_ttl=30
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    httpx.get(f"{gb}/c/d", timeout=5)
    r = httpx.get(
        f"{gb}/c/d",
        headers={"if-modified-since": "Tue, 22 Sep 2026 11:00:00 GMT"},
        timeout=5,
    )
    assert r.status_code == 304


# ---------- TTL 过期重新回源 ----------
def test_cache_expires_after_ttl(make_config, gateway_server, upstream_base):
    cfg = make_config(
        [
            make_config.route(
                id="c",
                prefix="/c",
                upstreams=make_config.ups(("a",)),
                cache_ttl=1,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r1 = httpx.get(f"{gb}/c/t", timeout=5)
    r2 = httpx.get(f"{gb}/c/t", timeout=5)
    assert r1.headers["x-cache"] == "MISS" and r2.headers["x-cache"] == "HIT"
    import time

    time.sleep(1.2)
    r3 = httpx.get(f"{gb}/c/t", timeout=5)
    assert r3.headers["x-cache"] == "MISS"


# ---------- 不可缓存方法不读不写 ----------
@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE"])
def test_uncacheable_methods_passthrough(make_config, gateway_server, method):
    cfg = make_config(
        [
            make_config.route(
                id="c",
                prefix="/c",
                upstreams=make_config.ups(("a",)),
                cache_ttl=30,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r = httpx.request(method, f"{gb}/c/x", timeout=5)
    assert r.status_code in (200, 405)
    assert "x-cache" not in r.headers


# ---------- 流式透传 ----------
def test_streaming_passthrough(make_config, gateway_server):
    cfg = make_config(
        [
            make_config.route(
                id="s",
                prefix="/stream",
                upstreams=make_config.ups(("a",)),
                streaming=True,
                timeout=10.0,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    with httpx.stream("GET", f"{gb}/stream", timeout=10) as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        chunks = list(resp.iter_text())
    body = "".join(chunks)
    assert body.count("event: chunk") == 5
    assert "a-0" in body and "a-4" in body



# ---------- 上游失败原因可区分 ----------
def test_connect_refused_is_502(make_config, gateway_server):
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    dead_port = s.getsockname()[1]
    s.close()  # 端口无人监听 -> 连接被拒绝 -> 502
    cfg = make_config(
        [
            make_config.route(
                id="d",
                prefix="/d",
                upstreams=(
                    __import__("gateway.config", fromlist=["UpstreamConfig"]).UpstreamConfig(
                        "dead", f"http://127.0.0.1:{dead_port}/dead"
                    ),
                ),
                cache_ttl=0,
                timeout=2.0,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r = httpx.get(f"{gb}/d/x", timeout=5)
    assert r.status_code == 502
    body = r.json()
    assert body["error"] == "upstream_error"
    assert body["upstream"] == "dead"


def test_upstream_timeout_is_504(make_config, gateway_server):
    cfg = make_config(
        [
            make_config.route(
                id="t",
                prefix="/t",
                upstreams=make_config.ups(("a",)),
                cache_ttl=0,
                timeout=0.2,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    t0 = time.time()
    r = httpx.get(f"{gb}/t/slow/1000", timeout=5)
    elapsed = time.time() - t0
    assert r.status_code == 504
    assert r.json()["error"] == "upstream_timeout"
    assert r.json()["upstream"] == "a"
    assert elapsed < 2.0  # 确实由超时提前返回，而非干等


def test_all_upstreams_down_is_503(make_config, gateway_server):
    import socket

    sockets = []
    ports = []
    for _ in range(2):
        sk = socket.socket()
        sk.bind(("127.0.0.1", 0))
        ports.append(sk.getsockname()[1])
        sockets.append(sk)
    sockets[0].close()  # 两个端口都不监听
    sockets[1].close()
    from gateway.config import UpstreamConfig

    cfg = make_config(
        [
            make_config.route(
                id="d2",
                prefix="/d2",
                upstreams=tuple(
                    UpstreamConfig(f"dead{i}", f"http://127.0.0.1:{p}/x")
                    for i, p in enumerate(ports)
                ),
                cache_ttl=0,
                timeout=1.0,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r1 = httpx.get(f"{gb}/d2/x", timeout=5)
    r2 = httpx.get(f"{gb}/d2/x", timeout=5)
    # 首次失败标记不健康；实例数少时很快全部熔断 -> 503 no_upstream
    assert {r1.status_code, r2.status_code} <= {502, 503}
    assert r2.json()["error"] in {"upstream_error", "no_upstream"}
    rr = httpx.get(f"{gb}/d2/x", timeout=5)
    assert rr.status_code == 503
    assert rr.json()["error"] == "no_upstream"


def test_failures_are_distinguishable_from_no_route(make_config, gateway_server):
    base, _ = gateway_server
    r = httpx.get(f"{base}/nope/xyz", timeout=5)
    assert r.status_code == 404 and r.json()["error"] == "no_route"


# ---------- 热更新：运行期切策略 / 加路由，不中断服务 ----------
def test_hot_reload_via_admin(gateway_server, upstream_base):
    base, _ = gateway_server
    # 默认配置下 users 是 round_robin
    r = httpx.get(f"{base}/api/users/hot", timeout=5)
    assert r.status_code == 200
    resp = httpx.post(
        f"{base}/admin/reload", json={"route_id": "users", "strategy": "least_conn"}, timeout=5
    )
    assert resp.status_code == 200 and resp.json()["reloaded"] is True
    cfg_resp = httpx.get(f"{base}/admin/config", timeout=5).json()
    users = next(r for r in cfg_resp["routes"] if r["id"] == "users")
    assert users["strategy"] == "least_conn"
    # reload 后路由仍可用
    r2 = httpx.get(f"{base}/api/users/hot2", timeout=5)
    assert r2.status_code == 200


def test_reload_to_new_base_takes_effect(make_config, gateway_server):
    _base, start = gateway_server
    gb = start(
        make_config(
            [
                make_config.route(
                    id="users",
                    prefix="/api/users",
                    upstreams=make_config.ups(("a",)),
                    cache_ttl=0,
                )
            ]
        )
    )
    assert httpx.get(f"{gb}/api/users/ok", timeout=5).status_code == 200
    # 换成不存在的上游基址（reload 以本地配置重建，路由 id=users 保持一致）
    resp = httpx.post(
        f"{gb}/admin/reload", json={"base": "http://127.0.0.1:1"}, timeout=5
    )
    assert resp.status_code == 200
    r = httpx.get(f"{gb}/api/users/ok", timeout=5)
    assert r.status_code == 502
    assert r.json()["error"] == "upstream_error"


# ---------- 白名单 / 黑名单 ----------
def test_blocklist_and_allowlist(make_config, gateway_server):
    cfg = make_config(
        [
            make_config.route(
                id="r", prefix="/r", upstreams=make_config.ups(("a",)), cache_ttl=0
            )
        ],
        blocklist=frozenset({"1.2.3.4"}),
    )
    _, start = gateway_server
    gb = start(cfg)
    # 正常客户端（测试机 loopback）放行
    assert httpx.get(f"{gb}/r/x", timeout=5).status_code == 200
    # 黑名单经 X-Forwarded-For 不可伪造（网关取直连 IP）；这里直接验证 AccessController
    from gateway.access import AccessController

    ac = AccessController(frozenset(), frozenset({"1.2.3.4"}))
    allowed, reason = ac.decide("1.2.3.4")
    assert not allowed and reason == "blocked"
    ac2 = AccessController(frozenset({"10.0.0.0/8"}), frozenset())
    ok, reason = ac2.decide("10.1.1.1")
    assert ok and reason == "allowed"
    ok2, reason2 = ac2.decide("8.8.8.8")
    assert not ok2 and reason2 == "not_allowed"


# ---------- 限流端到端：429 + Retry-After + scope 可区分 ----------
def test_rate_limit_e2e(make_config, gateway_server):
    cfg = make_config(
        [
            make_config.route(
                id="r",
                prefix="/r",
                upstreams=make_config.ups(("a",)),
                rate_limit=RateLimitRule(2, 1.0),
                cache_ttl=0,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    statuses = [httpx.get(f"{gb}/r/{i}", timeout=5).status_code for i in range(4)]
    assert statuses == [200, 200, 429, 429]
    denied = httpx.get(f"{gb}/r/x", timeout=5)
    body = denied.json()
    assert body["error"] == "rate_limited"
    assert body["scope"] in {"route", "client_route"}
    assert "retry-after" in denied.headers
    assert float(body["retry_after"]) >= 0


def test_rate_limit_concurrent_no_over_admit(make_config, gateway_server):
    cfg = make_config(
        [
            make_config.route(
                id="r",
                prefix="/r",
                upstreams=make_config.ups(("a",)),
                rate_limit=RateLimitRule(5, 0.0),
                cache_ttl=0,
                timeout=5.0,
            )
        ],
        global_limit=RateLimitRule(10_000, 10_000.0),
    )
    _, start = gateway_server
    gb = start(cfg)
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        results = list(
            pool.map(lambda i: httpx.get(f"{gb}/r/c{i}", timeout=5), range(30))
        )
    allowed = [r for r in results if r.status_code == 200]
    denied = [r for r in results if r.status_code == 429]
    assert len(allowed) == 5, len(allowed)
    assert len(denied) == 25, len(denied)
    # 被拒请求必须带 Retry-After 与可区分错误码
    body = denied[0].json()
    assert body["error"] == "rate_limited"
    assert "retry-after" in denied[0].headers


# ---------- 在途请求跨越 reload 不中断 ----------
def test_inflight_requests_survive_reload(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 400})
    cfg = make_config(
        [
            make_config.route(
                id="v1",
                prefix="/svc",
                upstreams=make_config.ups(("a",)),
                strategy=Strategy.ROUND_ROBIN,
                cache_ttl=0,
                timeout=5.0,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)

    inflight = []

    def fire():
        r = httpx.get(f"{gb}/svc/payload", timeout=8)
        inflight.append((r.status_code, r.json() if r.status_code == 200 else None))

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futs = [pool.submit(fire) for _ in range(3)]
        time.sleep(0.1)  # 等 3 个请求都已在途
        # 热更新：换策略（旧快照上的在途请求必须照常完成）
        rr = httpx.post(
            f"{gb}/admin/reload",
            json={"route_id": "v1", "strategy": "least_conn"},
            timeout=5,
        )
        assert rr.status_code == 200
        for f in futs:
            f.result()

    assert all(status == 200 for status, _ in inflight), inflight
    assert all(body["path"] == "svc/payload" for _, body in inflight)
    # reload 后的新请求仍工作
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 0})
    new_r = httpx.get(f"{gb}/svc/after", timeout=5)
    assert new_r.status_code == 200


# ---------- Vary 变体缓存 ----------
def test_vary_variants_are_cached_separately(make_config, gateway_server, upstream_base):
    cfg = make_config(
        [
            make_config.route(
                id="v",
                prefix="/v",
                upstreams=make_config.ups(("a",)),
                cache_ttl=30,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)

    def get(enc):
        return httpx.get(
            f"{gb}/v/vary",
            headers={"accept-encoding": enc} if enc else {},
            timeout=5,
        )

    g1 = get("gzip")
    i1 = get("identity")
    assert g1.json()["variant"] == "gzip"
    assert i1.json()["variant"] == "identity"
    # 第二次起各自命中自己的变体，且互不串味
    g2 = get("gzip")
    i2 = get("identity")
    assert g2.headers["x-cache"] == "HIT" and i2.headers["x-cache"] == "HIT"
    assert g2.json()["variant"] == "gzip" and i2.json()["variant"] == "identity"
    # 未缓存过的变体 -> MISS 回源
    other = get("deflate")
    assert other.headers["x-cache"] == "MISS"
    # 每种变体仅回源一次（gzip/identity/deflate）
    assert httpx.get(f"{upstream_base}/a/state").json()["hits"] == 3
