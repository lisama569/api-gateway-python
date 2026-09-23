"""上游超时 504、连接失败 502、上游 5xx 透传、失败原因可区分。"""
from __future__ import annotations

from main import set_runtime_state
from gateway.state import GatewayState


def test_upstream_timeout_504(gw):
    client, _ = gw
    r = client.get("/gateway/rr/slow", params={"ms": 3000})
    assert r.status_code == 504
    body = r.json()
    assert body["error"] == "upstream_timeout"
    assert body["retryable"] is True
    assert r.headers.get("retry-after") is not None
    print("[test] upstream timeout -> 504 distinguishable:", body["error"])


def test_upstream_5xx_passes_through(gw):
    client, _ = gw
    r = client.get("/gateway/rr/fail")
    assert r.status_code == 500
    assert r.json()["error"] == "boom"
    print("[test] upstream 500 passed through")


def test_dead_upstream_502(gw, dead_upstream_config):
    client, _ = gw
    set_runtime_state(GatewayState(dead_upstream_config))
    try:
        r = client.get("/gateway/dead/echo")
        assert r.status_code == 502
        body = r.json()
        assert body["error"] == "upstream_unavailable"
        assert body["retryable"] is True
        print("[test] dead upstream -> 502 distinguishable:", body["error"])
    finally:
        # 恢复夹具 state（gw 的 teardown 也会做，这里保险）
        from main import build_local_config
        set_runtime_state(GatewayState(build_local_config()))


def test_error_codes_are_distinguishable(gw, dead_upstream_config):
    client, _ = gw
    cases = {
        404: ("get", "/gateway/unknown/path"),
        504: ("get", "/gateway/rr/slow?ms=3000"),
    }
    # 429
    for _ in range(10):
        client.get("/gateway/rl2/echo")
    r429 = client.get("/gateway/rl2/echo")
    assert r429.status_code == 429
    assert r429.json()["error"] == "rate_limited"

    r404 = client.get("/gateway/unknown/path")
    assert r404.json()["error"] == "route_not_found"

    r504 = client.get("/gateway/rr/slow", params={"ms": 3000})
    assert r504.json()["error"] == "upstream_timeout"

    set_runtime_state(GatewayState(dead_upstream_config))
    r502 = client.get("/gateway/dead/echo")
    assert r502.json()["error"] == "upstream_unavailable"

    from main import build_local_config
    set_runtime_state(GatewayState(build_local_config()))
    codes = {r404.json()["error"], r429.json()["error"],
             r502.json()["error"], r504.json()["error"]}
    assert len(codes) == 4
    print("[test] distinguishable error codes:", sorted(codes))
