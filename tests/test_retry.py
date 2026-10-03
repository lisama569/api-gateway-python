"""重试与故障转移：真实本地假上游 + 网关（真实 TCP/uvicorn）。

日志可观察：每次尝试的实例选择（route ... attempt=k/n）、重试判定
（retry ... decision=retry reason=...）、耗尽结果（decision=exhausted tried=...）。
"""
from __future__ import annotations

import logging

import httpx
import pytest

from gateway.config import RetryConfig, Strategy


@pytest.fixture
def gateway_logs(caplog):
    """网关 logger propagate=False，直接把 caplog handler 挂上去捕获。"""
    logger = logging.getLogger("gateway")
    logger.addHandler(caplog.handler)
    caplog.set_level(logging.INFO, logger="gateway")
    yield caplog
    logger.removeHandler(caplog.handler)


def _retry_route(make_config, **retry_kw):
    retry = RetryConfig(**retry_kw)
    return make_config(
        [
            make_config.route(
                id="svc",
                prefix="/svc",
                upstreams=make_config.ups(("a", "b")),
                strategy=Strategy.ROUND_ROBIN,
                cache_ttl=0,
                timeout=5.0,
                retry=retry,
            )
        ]
    )


def _hits(upstream_base, name) -> int:
    return httpx.get(f"{upstream_base}/{name}/state", timeout=5).json()["hits"]


# ---------- 超时后换实例重试 ----------
def test_retry_failover_on_timeout(
    make_config, gateway_server, upstream_base, gateway_logs
):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 6000})
    cfg = _retry_route(make_config, max_attempts=2, per_try_timeout=0.3, budget=5.0)
    _, start = gateway_server
    gb = start(cfg)

    r = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r.status_code == 200
    assert r.json()["instance"] == "b"  # a 超时后换到 b
    assert r.headers["x-gateway-tried"] == "a,b"
    # 日志能看到两次尝试各自选中的实例与重试判定
    text = gateway_logs.text
    assert "attempt=1/2" in text and "upstream=a" in text
    assert "attempt=2/2" in text and "upstream=b" in text
    assert "decision=retry reason=timeout" in text


# ---------- 5xx 后换实例重试 ----------
def test_retry_failover_on_5xx(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/fail", params={"on": 1})
    cfg = _retry_route(make_config, max_attempts=2, retry_on_5xx=True)
    _, start = gateway_server
    gb = start(cfg)

    r = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r.status_code == 200
    assert r.json()["instance"] == "b"
    assert r.headers["x-gateway-tried"] == "a,b"
    assert _hits(upstream_base, "a") == 1  # a 只被打了一次
    assert _hits(upstream_base, "b") == 1


# ---------- 重试耗尽：超时 ----------
def test_retry_exhausted_timeout(make_config, gateway_server, upstream_base, gateway_logs):
    for n in ("a", "b"):
        httpx.post(f"{upstream_base}/{n}/state/slow", params={"ms": 6000})
    cfg = _retry_route(make_config, max_attempts=2, per_try_timeout=0.3, budget=5.0)
    _, start = gateway_server
    gb = start(cfg)

    r = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r.status_code == 504
    body = r.json()
    assert body["error"] == "upstream_timeout"  # 与无路由/限流/无实例可区分
    assert body["attempts"] == 2
    assert body["tried_upstreams"] == ["a", "b"]
    assert "decision=exhausted" in gateway_logs.text


# ---------- 重试耗尽：最后一跳是 5xx 则原样透传 ----------
def test_retry_exhausted_5xx_passthrough(make_config, gateway_server, upstream_base):
    for n in ("a", "b"):
        httpx.post(f"{upstream_base}/{n}/state/fail", params={"on": 1})
    cfg = _retry_route(make_config, max_attempts=2, retry_on_5xx=True)
    _, start = gateway_server
    gb = start(cfg)

    r = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r.status_code == 500  # 上游 5xx 原样透传
    assert r.text == "boom"
    assert r.headers["x-gateway-tried"] == "a,b"


# ---------- 写操作不重试 ----------
def test_post_not_retried(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/fail", params={"on": 1})
    cfg = _retry_route(make_config, max_attempts=3, retry_on_5xx=True)
    _, start = gateway_server
    gb = start(cfg)

    r = httpx.post(f"{gb}/svc/x", json={"k": "v"}, timeout=10)
    assert r.status_code == 500  # 按原有单次失败结果返回
    assert "x-gateway-tried" not in r.headers
    assert _hits(upstream_base, "a") == 1
    assert _hits(upstream_base, "b") == 0  # 没有重放到 b


# ---------- 整体预算上限 ----------
def test_retry_budget_caps_attempts(make_config, gateway_server, upstream_base):
    for n in ("a", "b"):
        httpx.post(f"{upstream_base}/{n}/state/slow", params={"ms": 6000})
    # 单次 0.5s、预算 0.6s：第二次尝试后预算耗尽，不会打满 max_attempts=5
    cfg = _retry_route(make_config, max_attempts=5, per_try_timeout=0.5, budget=0.6)
    _, start = gateway_server
    gb = start(cfg)

    r = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r.status_code == 504
    body = r.json()
    assert body["error"] == "upstream_timeout"
    assert body["attempts"] == 2  # 预算截断，而非 5 次
    assert body["tried_upstreams"] == ["a", "b"]


# ---------- 无重试配置时保持原有单次行为 ----------
def test_no_retry_config_keeps_single_attempt(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 6000})
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
    gb = start(cfg)

    r = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r.status_code == 504
    body = r.json()
    assert body["error"] == "upstream_timeout"
    assert body["upstream"] == "a"
    assert _hits(upstream_base, "b") == 0  # 没有故障转移
