"""缓存未命中时的同键请求合并回源（single-flight）。

日志可观察：coalesce key=... role=leader/waiter decision=fetch/shared/timeout。
"""
from __future__ import annotations

import concurrent.futures
import logging

import httpx
import pytest

from gateway.config import Strategy
from gateway.coalesce import SharedResult, sharing_allowed, vary_fields_of


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


# ---------- 冷启动：不同 Accept-Encoding 并发，不能合进同一次回源 ----------
def test_cold_start_gzip_and_identity_each_fetched(
    make_config, gateway_server, upstream_base, gateway_logs
):
    """核心回归：没有任何已缓存变体时，压缩/不压缩请求各自回源，各自拿到匹配的变体。"""
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 500})
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f_gz = pool.submit(
            lambda: httpx.get(
                f"{gb}/svc/vary", headers={"accept-encoding": "gzip"}, timeout=10
            )
        )
        f_id = pool.submit(
            lambda: httpx.get(
                f"{gb}/svc/vary", headers={"accept-encoding": "identity"}, timeout=10
            )
        )
        r_gz, r_id = f_gz.result(), f_id.result()

    assert r_gz.status_code == 200 and r_id.status_code == 200
    # 每个请求拿到的都必须是和自己请求头对得上的响应
    assert r_gz.json()["variant"] == "gzip"
    assert r_id.json()["variant"] == "identity"
    assert _hits(upstream_base) == 2  # 两个变体分别回源，没有合并
    # 两个都是各自航班的 leader（冷启动预分流按 accept-encoding 拆开了）
    assert r_gz.headers.get("x-coalesce") == "leader"
    assert r_id.headers.get("x-coalesce") == "leader"
    text = gateway_logs.text
    assert "role=leader decision=fetch" in text
    assert "role=waiter decision=self_fetch" not in text  # 预分流直接拆开，无需回包后纠正


# ---------- 冷启动 + 同变体并发：同变体仍合并，不同变体拆开 ----------
def test_cold_start_same_variant_merged_other_split(
    make_config, gateway_server, upstream_base
):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 500})
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base))

    def get_gzip():
        return httpx.get(f"{gb}/svc/vary", headers={"accept-encoding": "gzip"}, timeout=10)

    def get_identity():
        return httpx.get(
            f"{gb}/svc/vary", headers={"accept-encoding": "identity"}, timeout=10
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        fs = [pool.submit(get_gzip) for _ in range(3)] + [pool.submit(get_identity)]
        rs = [f.result() for f in fs]

    gz = [r for r in rs if r.json()["variant"] == "gzip"]
    ident = [r for r in rs if r.json()["variant"] == "identity"]
    assert len(gz) == 3 and len(ident) == 1
    assert all(r.json()["variant"] == "gzip" for r in gz)
    roles_gz = sorted(r.headers.get("x-coalesce") for r in gz)
    assert roles_gz == ["leader", "shared", "shared"]
    assert ident[0].headers.get("x-coalesce") == "leader"
    assert _hits(upstream_base) == 2  # gzip 合一次 + identity 一次


# ---------- 非标准 Vary 头不在冷启动预分流里：回包后二次校验，各自自回源 ----------
def test_cold_start_custom_vary_waiter_self_fetches(
    make_config, gateway_server, upstream_base, gateway_logs
):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 500})
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(
            lambda: httpx.get(
                f"{gb}/svc/vary-custom", headers={"x-tenant": "1"}, timeout=10
            )
        )
        f2 = pool.submit(
            lambda: httpx.get(
                f"{gb}/svc/vary-custom", headers={"x-tenant": "2"}, timeout=10
            )
        )
        r1, r2 = f1.result(), f2.result()

    assert r1.status_code == 200 and r2.status_code == 200
    # waiter 绝不能拿到 leader 的变体
    tenants = {r.json()["tenant"] for r in (r1, r2)}
    assert tenants == {"1", "2"}
    assert _hits(upstream_base) == 2  # 二次校验不通过 -> waiter 自己回源
    roles = sorted(r.headers.get("x-coalesce") for r in (r1, r2))
    assert roles == ["leader", "self"]
    text = gateway_logs.text
    assert "role=waiter decision=self_fetch reason=vary_mismatch:x-tenant" in text


# ---------- 自回源后再发：走缓存，仍然拿到正确变体，不再卡合并 ----------
def test_cold_start_self_fetch_then_cache_hit(
    make_config, gateway_server, upstream_base
):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 400})
    _, start = gateway_server
    gb = start(_cache_route(make_config, upstream_base))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(
            lambda: httpx.get(
                f"{gb}/svc/vary-custom", headers={"x-tenant": "1"}, timeout=10
            )
        )
        f2 = pool.submit(
            lambda: httpx.get(
                f"{gb}/svc/vary-custom", headers={"x-tenant": "2"}, timeout=10
            )
        )
        r1, r2 = f1.result(), f2.result()
    assert {r1.json()["tenant"], r2.json()["tenant"]} == {"1", "2"}
    assert _hits(upstream_base) == 2

    # 两个变体都已缓存；之后再发不再回源，且各自命中自己的变体
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 0})
    a = httpx.get(f"{gb}/svc/vary-custom", headers={"x-tenant": "1"}, timeout=5)
    b = httpx.get(f"{gb}/svc/vary-custom", headers={"x-tenant": "2"}, timeout=5)
    assert a.headers["x-cache"] == "HIT" and b.headers["x-cache"] == "HIT"
    assert a.json()["tenant"] == "1" and b.json()["tenant"] == "2"
    assert _hits(upstream_base) == 2


# ---------- 共享判定纯函数：Vary 解析 / * 拒绝 / 无 Vary 可共享 ----------
def test_sharing_allowed_rules():
    ok = SharedResult(
        200, [(b"vary", b"accept-encoding, x-tenant")], b"x"
    )
    star = SharedResult(200, [(b"vary", b"*")], b"x")
    no_vary = SharedResult(200, [(b"content-type", b"text/plain")], b"x")

    assert vary_fields_of(ok.headers) == ["accept-encoding", "x-tenant"]
    assert vary_fields_of(star.headers) == ["*"]
    assert vary_fields_of(no_vary.headers) == []

    leader = {"accept-encoding": "gzip", "x-tenant": "1"}
    same = {"accept-encoding": "gzip", "x-tenant": "1", "user-agent": "curl"}
    diff = {"accept-encoding": "identity", "x-tenant": "1"}

    allowed, why = sharing_allowed(leader, same, ok)
    assert allowed and why == "match"  # Vary 之外的头不同不影响共享
    allowed, why = sharing_allowed(leader, diff, ok)
    assert not allowed and why == "vary_mismatch:accept-encoding"
    assert not sharing_allowed(leader, leader, star)[0]
    assert sharing_allowed(leader, diff, no_vary)[0]
    assert not sharing_allowed(None, leader, ok)[0]
