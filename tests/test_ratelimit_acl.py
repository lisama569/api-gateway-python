"""按客户端+路由维度限流、白/黑名单、并发不多放行、状态可区分。"""
from __future__ import annotations

import concurrent.futures

from gateway.config import (
    GatewayConfig,
    RateLimitConfig,
    RouteConfig,
    UpstreamConfig,
)
from gateway.ratelimit import RateLimiter
from gateway.config import RateLimitConfig as RLC


def test_rate_limit_sequential_distinguishable(gw):
    client, _ = gw
    statuses = [client.get("/gateway/rl2/echo").status_code for _ in range(4)]
    assert statuses == [200, 200, 429, 429], statuses
    body = client.get("/gateway/rl2/echo").json()
    assert body["error"] == "rate_limited"
    assert body["retryable"] is True
    retry = client.get("/gateway/rl2/echo")
    assert retry.headers.get("retry-after") is not None
    print("[test] rate limit statuses:", statuses)


def test_rate_limit_window_reopens(gw):
    client, _ = gw
    for _ in range(3):
        assert client.get("/gateway/rl2/echo").status_code in (200, 429)
    import time
    time.sleep(1.1)
    assert client.get("/gateway/rl2/echo").status_code == 200
    print("[test] rate limit window reopened")


def test_concurrent_rate_limit_no_over_admit(gw):
    """并发 20 个受限请求（上限 5/窗口），恰好放行 5 个。"""
    client, state = gw
    state.limiter.reset()

    def one(i):
        return client.get("/gateway/rl/echo",
                          headers={"X-Forwarded-For": "192.168.1.50"}).status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        statuses = list(pool.map(one, range(20)))
    allowed = statuses.count(200)
    rejected = statuses.count(429)
    print(f"[test] concurrent limiter allowed={allowed} rejected={rejected}")
    assert allowed == 5, statuses
    assert rejected == 15, statuses


def test_rate_limit_per_client_and_route(gw):
    """不同客户端与不同路由独立计数。"""
    client, _ = gw
    # 客户端 A 用满
    a = [client.get("/gateway/rl2/echo",
                    headers={"X-Forwarded-For": "1.1.1.1"}).status_code
         for _ in range(3)]
    # 客户端 B 不受影响
    b = client.get("/gateway/rl2/echo",
                   headers={"X-Forwarded-For": "2.2.2.2"}).status_code
    # 同客户端换路由不受影响
    other = client.get("/gateway/rl/echo",
                       headers={"X-Forwarded-For": "1.1.1.1"}).status_code
    assert a == [200, 200, 429]
    assert b == 200
    assert other == 200


def test_unit_rate_limiter_exact_counts():
    rl = RateLimiter()
    cfg = RLC(requests=3, window_seconds=1.0)
    from gateway.errors import RateLimitedError
    for _ in range(3):
        rl.check("10.0.0.1", "r", cfg)
    try:
        rl.check("10.0.0.1", "r", cfg)
        assert False, "should be limited"
    except RateLimitedError as exc:
        assert exc.retry_after is not None
        assert exc.status_code == 429


def test_blacklist_and_whitelist(gw, upstreams):
    client, state = gw

    # 黑名单
    state.acl.update((), ("6.6.6.6",))
    r = client.get("/gateway/rr/echo",
                   headers={"X-Forwarded-For": "6.6.6.6"})
    assert r.status_code == 403
    assert r.json()["error"] == "ip_blacklisted"

    # 白名单：非名单来源被拒，原因与黑名单不同
    state.acl.update(("7.7.7.7",), ())
    r = client.get("/gateway/rr/echo",
                   headers={"X-Forwarded-For": "8.8.8.8"})
    assert r.status_code == 403
    assert r.json()["error"] == "ip_not_allowed"
    r = client.get("/gateway/rr/echo",
                   headers={"X-Forwarded-For": "7.7.7.7"})
    assert r.status_code == 200

    # CIDR 白名单
    state.acl.update(("10.10.0.0/16",), ())
    assert client.get(
        "/gateway/rr/echo",
        headers={"X-Forwarded-For": "10.10.3.3"}).status_code == 200
    assert client.get(
        "/gateway/rr/echo",
        headers={"X-Forwarded-For": "10.11.3.3"}).status_code == 403
    print("[test] acl blacklist/whitelist/cidr passed")


def test_acl_hot_reload_via_admin(gw):
    client, state = gw
    payload = {
        "upstreams": [{"id": "u0", "base_url": state.config.upstreams[0].base_url}],
        "routes": [{"id": "only", "path_prefix": "/gateway/rr",
                    "upstream_ids": ["u0"]}],
        "blacklist": ["127.0.0.1"],
    }
    r = client.put("/admin/config", json=payload)
    assert r.status_code == 200
    r = client.get("/gateway/rr/echo",
                   headers={"X-Forwarded-For": "127.0.0.1"})
    assert r.status_code == 403
    assert r.json()["error"] == "ip_blacklisted"
