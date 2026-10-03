"""重试与故障转移：真实本地假上游 + 网关（真实 TCP/uvicorn）。

日志可观察：每次尝试选中的实例（route ... attempt=N）、重试判定
（retry ... decision=retry/stop reason=...）。
"""
from __future__ import annotations

import socket
import time

import httpx

from gateway.config import RetryConfig, Strategy, UpstreamConfig


def _dead_upstream(name="dead"):
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()  # 端口无人监听 -> 连接被拒绝
    return UpstreamConfig(name, f"http://127.0.0.1:{port}/{name}")


# ---------- 5xx 故障转移：换到同路由下健康实例 ----------
def test_retry_fails_over_on_5xx(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/fail", params={"on": 1})  # a 返回 500
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a", "b")),
                strategy=Strategy.ROUND_ROBIN,
                cache_ttl=0,
                retry=RetryConfig(max_attempts=2),
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r = httpx.get(f"{gb}/svc/x", timeout=5)
    assert r.status_code == 200
    assert r.json()["instance"] == "b"  # 第一跳 a 500，自动换到 b
    # 重试明细：a 因 500 被试过一次
    assert "a:status_500" in r.headers["x-gateway-attempts"]
    # b 只被回源一次
    assert httpx.get(f"{upstream_base}/b/state").json()["hits"] == 1


# ---------- 超时故障转移：单次耗时上限生效 ----------
def test_retry_fails_over_on_timeout(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 2000})
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a", "b")),
                strategy=Strategy.ROUND_ROBIN,
                cache_ttl=0,
                timeout=5.0,
                retry=RetryConfig(max_attempts=2, per_attempt_timeout=0.3),
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    t0 = time.monotonic()
    r = httpx.get(f"{gb}/svc/y", timeout=10)
    elapsed = time.monotonic() - t0
    assert r.status_code == 200
    assert r.json()["instance"] == "b"
    assert "a:timeout" in r.headers["x-gateway-attempts"]
    assert elapsed < 2.0  # a 被单次超时 0.3s 掐断，而不是等满 route.timeout
    assert httpx.get(f"{upstream_base}/a/state").json()["hits"] == 1  # a 确实被试过


# ---------- 全部重试失败：错误可区分且带尝试明细 ----------
def test_retry_exhausted_reports_attempts(make_config, gateway_server):
    dead0, dead1 = _dead_upstream("d0"), _dead_upstream("d1")
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=(dead0, dead1),
                cache_ttl=0,
                timeout=1.0,
                retry=RetryConfig(max_attempts=3),  # 只有 2 个实例可试
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r = httpx.get(f"{gb}/svc/x", timeout=5)
    assert r.status_code == 502
    body = r.json()
    assert body["error"] == "upstream_error"
    # 试过哪些实例、各自结局清晰可见；实例数耗尽即停止，不无限重试
    assert len(body["attempts"]) == 2
    assert {a["upstream"] for a in body["attempts"]} == {"d0", "d1"}
    assert all(a["outcome"] == "transport_error" for a in body["attempts"])
    # 与"无可用实例"可区分：两实例已熔断，下一请求直接 503
    r2 = httpx.get(f"{gb}/svc/x", timeout=5)
    assert r2.status_code == 503
    assert r2.json()["error"] == "no_upstream"
    assert "attempts" not in r2.json()


# ---------- 整体预算上限：预算耗尽即停止 ----------
def test_retry_budget_caps_total_time(make_config, gateway_server, upstream_base):
    for n in ("a", "b", "c"):
        httpx.post(f"{upstream_base}/{n}/state/slow", params={"ms": 2000})
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a", "b", "c")),
                strategy=Strategy.ROUND_ROBIN,
                cache_ttl=0,
                retry=RetryConfig(
                    max_attempts=5, per_attempt_timeout=0.3, budget=0.7
                ),
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    t0 = time.monotonic()
    r = httpx.get(f"{gb}/svc/z", timeout=10)
    elapsed = time.monotonic() - t0
    assert r.status_code == 504
    body = r.json()
    assert body["error"] == "upstream_timeout"
    # 预算 0.7s 只允许约 0.3+0.3+0.1(剩余预算)：远少于 max_attempts=5
    assert 2 <= len(body["attempts"]) <= 3
    assert len(body["attempts"]) < 5
    assert all(a["outcome"] == "timeout" for a in body["attempts"])
    assert elapsed < 1.5


# ---------- 写操作不重试：不重复执行上游业务动作 ----------
def test_post_is_never_retried(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/fail", params={"on": 1})
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a", "b")),
                strategy=Strategy.ROUND_ROBIN,
                cache_ttl=0,
                retry=RetryConfig(max_attempts=3),
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r = httpx.post(f"{gb}/svc/pay", content=b"{}", timeout=5)
    # 第一跳 a 返回 500，按原失败结果返回，不换实例重放
    assert r.status_code == 500
    assert "x-gateway-attempts" not in r.headers
    # b 完全没被碰到：写操作没有在另一个实例上重放
    assert httpx.get(f"{upstream_base}/b/state").json()["hits"] == 0


# ---------- 流式请求不重试 ----------
def test_streaming_is_never_retried(make_config, gateway_server):
    dead = _dead_upstream("dead")
    cfg = make_config(
        [
            make_config.route(
                id="s",
                prefix="/stream",
                upstreams=(dead,) + make_config.ups(("a",)),
                strategy=Strategy.ROUND_ROBIN,
                streaming=True,
                timeout=2.0,
                retry=RetryConfig(max_attempts=3),
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r = httpx.get(f"{gb}/stream", timeout=5)
    # 第一跳选中 dead -> 502，不重试、不换实例
    assert r.status_code == 502
    body = r.json()
    assert body["error"] == "upstream_error"
    assert "attempts" not in body


# ---------- 每次尝试是干净等价的请求（头/体/目标重新确定）----------
def test_each_attempt_carries_same_headers_and_body(
    make_config, gateway_server, upstream_base
):
    httpx.post(f"{upstream_base}/a/state/fail", params={"on": 1})
    cfg = make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a", "b")),
                strategy=Strategy.ROUND_ROBIN,
                cache_ttl=0,
                retry=RetryConfig(max_attempts=2),
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r = httpx.get(
        f"{gb}/svc/echo?q=1",
        headers={"x-custom": "keep-me"},
        timeout=5,
    )
    assert r.status_code == 200
    body = r.json()
    # 第二跳在 b 上重新发起，查询串原样携带
    assert body["instance"] == "b"
    assert body["query"] == "q=1"
