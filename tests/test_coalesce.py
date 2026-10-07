"""缓存未命中时的同键请求合并回源（single-flight）。

日志可观察：coalesce key=... role=leader/waiter decision=fetch/shared/timeout。
"""
from __future__ import annotations

import concurrent.futures
import logging

import httpx
import pytest

from gateway.config import Strategy


@pytest.fixture
def gateway_logs(caplog):
    logger = logging.getLogger("gateway")
    logger.addHandler(caplog.handler)
    caplog.set_level(logging.INFO, logger="gateway")
    yield caplog
    logger.removeHandler(caplog.handler)


def _cache_route(make_config, upstream_base, **route_kw):
    kw = dict(
        id="svc",
        prefix="/svc",
        upstreams=make_config.ups(("a",)),
        strategy=Strategy.ROUND_ROBIN,
        cache_ttl=30,
        timeout=5.0,
    )
    kw.update(route_kw)
    return make_config([make_config.route(**kw)])


def _hits(upstream_base, name="a") -> int:
    return httpx.get(f"{upstream_base}/{name}/state", timeout=5).json()["hits"]


def _burst(n, fn):
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as pool:
        return [f.result() for f in [pool.submit(fn) for _ in range(n)]]


# ---------- 同键并发：只回源一次，其余共享 ----------
def test_same_key_burst_shares_one_fetch(
    make_config, gateway_server, upstream_base, gateway_logs
):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 500})
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base))

    rs = _burst(6, lambda: httpx.get(f"{gb}/svc/x", timeout=10))
    assert all(r.status_code == 200 for r in rs)
    assert _hits(upstream_base) == 1  # 只回源一次
    bodies = {r.json()["hits"] for r in rs}
    assert len(bodies) == 1  # 大家拿到同一份结果
    roles = [r.headers.get("x-coalesce") for r in rs]
    assert roles.count("leader") == 1 and roles.count("shared") == 5
    text = gateway_logs.text
    assert "role=leader decision=fetch" in text
    assert "role=waiter decision=shared" in text


# ---------- 不同键不共享 ----------
def test_different_keys_not_shared(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 300})
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base))

    rs = _burst(2, lambda: None) and []
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(lambda: httpx.get(f"{gb}/svc/x?k=1", timeout=10))
        f2 = pool.submit(lambda: httpx.get(f"{gb}/svc/x?k=2", timeout=10))
        rs = [f1.result(), f2.result()]
    assert all(r.status_code == 200 for r in rs)
    assert _hits(upstream_base) == 2  # 两个键各自回源
    assert all(r.headers.get("x-coalesce") == "leader" for r in rs)


# ---------- 影响结果的请求头（Vary）参与合并键 ----------
def test_vary_headers_separate_flights(make_config, gateway_server, upstream_base):
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base))

    # 先暖 identity 变体（让缓存知道该资源 Vary: accept-encoding）
    r0 = httpx.get(f"{gb}/svc/vary", headers={"accept-encoding": "identity"}, timeout=5)
    assert r0.headers["x-cache"] == "MISS"
    assert _hits(upstream_base) == 1

    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 400})
    # 并发：3 个 gzip（同键，合并回源）+ 1 个 identity（缓存命中，不回源）
    def get_gzip():
        return httpx.get(f"{gb}/svc/vary", headers={"accept-encoding": "gzip"}, timeout=10)

    def get_identity():
        return httpx.get(
            f"{gb}/svc/vary", headers={"accept-encoding": "identity"}, timeout=10
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        fs = [pool.submit(get_gzip) for _ in range(3)] + [pool.submit(get_identity)]
        rs = [f.result() for f in fs]

    gzip_rs = [r for r in rs if r.json()["variant"] == "gzip"]
    ident_rs = [r for r in rs if r.json()["variant"] == "identity"]
    assert len(gzip_rs) == 3 and len(ident_rs) == 1
    assert ident_rs[0].headers["x-cache"] == "HIT"  # identity 走缓存，未被合并污染
    # gzip 变体只回源一次：1 个 leader，其余要么共享、要么（回源完成后）缓存命中
    roles = [r.headers.get("x-coalesce") for r in gzip_rs]
    assert roles.count("leader") == 1
    for r, role in zip(gzip_rs, roles):
        assert role is not None or r.headers.get("x-cache") == "HIT"
    assert _hits(upstream_base) == 2  # gzip 变体只回源一次


# ---------- 冷启动：影响结果的请求头不一致，不得合并到同一次回源 ----------
def test_cold_start_vary_requests_not_merged(
    make_config, gateway_server, upstream_base, gateway_logs
):
    """缓存完全冷启动（无任何已缓存变体）时，并发 gzip / identity 请求
    必须各自回源，且每个请求只能拿到与自己 accept-encoding 对得上的变体。"""
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 400})
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base))

    def get(enc):
        return httpx.get(f"{gb}/svc/vary", headers={"accept-encoding": enc}, timeout=10)

    encs = ["gzip", "gzip", "identity", "identity"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        rs = [f.result() for f in [pool.submit(get, e) for e in encs]]

    assert all(r.status_code == 200 for r in rs)
    for r, enc in zip(rs, encs):
        assert r.json()["variant"] == enc  # 响应变体与各自请求头一致
    assert _hits(upstream_base) == 2  # 两个变体各自回源一次，互不套用
    text = gateway_logs.text
    assert text.count("role=leader decision=fetch") == 2  # 两个航班各自有 leader
    assert "role=waiter decision=shared" in text  # 同变体的并发请求仍被合并


# ---------- 键外的 Vary 头：共享结果校验不通过，等待方自己回源 ----------
def test_unknown_vary_header_waiter_self_fetches(
    make_config, gateway_server, upstream_base, gateway_logs
):
    """上游按 x-variant（不在合并键保守候选集内）出不同变体：冷启动并发时
    两请求会同航班，但等待方按响应 Vary 校验发现对不上，必须自己回源，
    不能套用 leader 的结果。"""
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 400})
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base))

    def get(v):
        return httpx.get(f"{gb}/svc/varyh", headers={"x-variant": v}, timeout=10)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f_blue = pool.submit(get, "blue")
        f_green = pool.submit(get, "green")
        r_blue, r_green = f_blue.result(), f_green.result()

    assert r_blue.status_code == r_green.status_code == 200
    assert r_blue.json()["variant"] == "blue"  # 各自拿到与自己请求头匹配的响应
    assert r_green.json()["variant"] == "green"
    assert _hits(upstream_base) == 2  # leader 回源 + 等待方校验不通过后自己回源
    assert "decision=vary_mismatch -> self fetch" in gateway_logs.text


# ---------- 共享失败：等待方拿到可判断的失败，之后可重新发起 ----------
def test_shared_failure_and_recovery(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/fail", params={"on": 1})
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 300})
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base))

    rs = _burst(4, lambda: httpx.get(f"{gb}/svc/x", timeout=10))
    assert all(r.status_code == 500 for r in rs)  # 等待方也拿到失败
    assert _hits(upstream_base) == 1  # 失败也只回源一次
    assert all(r.text == "boom" for r in rs)

    # 上游恢复后，后续请求可以重新发起并成功
    httpx.post(f"{upstream_base}/a/state/fail", params={"on": 0})
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 0})
    r = httpx.get(f"{gb}/svc/x", timeout=10)
    assert r.status_code == 200
    assert r.headers["x-cache"] == "MISS"


# ---------- 等待超时有明确结果 ----------
def test_wait_timeout_gives_definite_result(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 2000})
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base, coalesce_wait=0.2))

    rs = _burst(2, lambda: httpx.get(f"{gb}/svc/x", timeout=10))
    by_status = sorted(r.status_code for r in rs)
    assert by_status == [200, 504]
    waiter = next(r for r in rs if r.status_code == 504)
    assert waiter.json()["error"] == "coalesce_wait_timeout"
    assert _hits(upstream_base) == 1  # 等待方超时后没有自己回源


# ---------- 不可缓存方法不合并 ----------
def test_non_cacheable_method_not_coalesced(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 300})
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base))

    rs = _burst(3, lambda: httpx.post(f"{gb}/svc/x", json={"k": 1}, timeout=10))
    assert all(r.status_code == 200 for r in rs)
    assert _hits(upstream_base) == 3  # 每个 POST 都真实回源
    assert all("x-coalesce" not in r.headers for r in rs)
