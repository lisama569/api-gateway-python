"""回源合并（single-flight）：同键并发未命中只回源一次。

日志可观察：coalesce key=... decision=leader/wait/shared/wait_timeout。
"""
from __future__ import annotations

import concurrent.futures

import httpx


def _fire(gb, path, n, headers=None, method="GET"):
    """并发发 n 个相同请求，返回响应列表。"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as pool:
        futs = [
            pool.submit(
                lambda: httpx.request(method, f"{gb}{path}", headers=headers or {}, timeout=10)
            )
            for _ in range(n)
        ]
        return [f.result() for f in futs]


# ---------- 同键并发未命中：仅一个回源，其余共享 ----------
def test_concurrent_miss_coalesced(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 300})
    cfg = make_config(
        [
            make_config.route(
                id="c",
                prefix="/c",
                upstreams=make_config.ups(("a",)),
                cache_ttl=30,
                timeout=5.0,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    responses = _fire(gb, "/c/hot", 8)
    assert all(r.status_code == 200 for r in responses)
    # 上游只被打了一次
    assert httpx.get(f"{upstream_base}/a/state").json()["hits"] == 1
    # 所有请求拿到同一份结果（body 中 hits 相同）
    bodies = {r.json()["hits"] for r in responses}
    assert len(bodies) == 1
    # 等待方能看出是共享结果
    assert any(r.headers.get("x-coalesced") == "shared" for r in responses)


# ---------- 不同键互不合并 ----------
def test_different_keys_not_coalesced(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 200})
    cfg = make_config(
        [
            make_config.route(
                id="c",
                prefix="/c",
                upstreams=make_config.ups(("a",)),
                cache_ttl=30,
                timeout=5.0,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futs = [
            pool.submit(lambda i=i: httpx.get(f"{gb}/c/k?q={i}", timeout=10))
            for i in range(4)
        ]
        responses = [f.result() for f in futs]
    assert all(r.status_code == 200 for r in responses)
    # 每个键各自回源一次
    assert httpx.get(f"{upstream_base}/a/state").json()["hits"] == 4


# ---------- 共享回源失败：等待方各自拿到可判断失败，之后可重新发起 ----------
def test_shared_failure_then_retry(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 2000})
    cfg = make_config(
        [
            make_config.route(
                id="c",
                prefix="/c",
                upstreams=make_config.ups(("a",)),
                cache_ttl=30,
                timeout=0.4,  # 领导者必然超时失败
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    responses = _fire(gb, "/c/slow", 5)
    # 每个等待方都拿到可判断的 504，而不是挂死
    assert all(r.status_code == 504 for r in responses)
    assert all(r.json()["error"] == "upstream_timeout" for r in responses)
    # 航班已关闭：上游恢复后可重新发起并成功
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 0})
    cfg2 = make_config(
        [
            make_config.route(
                id="c2",
                prefix="/c2",
                upstreams=make_config.ups(("a",)),
                cache_ttl=30,
                timeout=5.0,
            )
        ]
    )
    gb2 = start(cfg2)
    r = httpx.get(f"{gb2}/c2/ok", timeout=5)
    assert r.status_code == 200


# ---------- 等待超时有上限：超时后自行回源 ----------
def test_wait_timeout_falls_back_to_own_fetch(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 800})
    cfg = make_config(
        [
            make_config.route(
                id="c",
                prefix="/c",
                upstreams=make_config.ups(("a",)),
                cache_ttl=30,
                timeout=5.0,
                coalesce_wait=0.2,  # 等待上限小于上游耗时
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    responses = _fire(gb, "/c/wait", 2)
    assert all(r.status_code == 200 for r in responses)
    # 等待方超时后自行回源：上游共 2 次命中
    assert httpx.get(f"{upstream_base}/a/state").json()["hits"] == 2


# ---------- Vary 变体不互相复用 ----------
def test_vary_variants_not_shared(make_config, gateway_server, upstream_base):
    cfg = make_config(
        [
            make_config.route(
                id="v",
                prefix="/v",
                upstreams=make_config.ups(("a",)),
                cache_ttl=30,
                timeout=5.0,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        fg = pool.submit(
            lambda: httpx.get(f"{gb}/v/vary", headers={"accept-encoding": "gzip"}, timeout=10)
        )
        fi = pool.submit(
            lambda: httpx.get(
                f"{gb}/v/vary", headers={"accept-encoding": "identity"}, timeout=10
            )
        )
        g, i = fg.result(), fi.result()
    # 各自拿到自己的变体，互不串味
    assert g.json()["variant"] == "gzip"
    assert i.json()["variant"] == "identity"
    # 两个变体各自回源，不错误共享
    assert httpx.get(f"{upstream_base}/a/state").json()["hits"] == 2


# ---------- 不可缓存与流式请求不参与合并 ----------
def test_uncacheable_not_coalesced(make_config, gateway_server, upstream_base):
    httpx.post(f"{upstream_base}/a/state/slow", params={"ms": 200})
    cfg = make_config(
        [
            make_config.route(
                id="nc",
                prefix="/nc",
                upstreams=make_config.ups(("a",)),
                cache_ttl=0,  # 不可缓存
                timeout=5.0,
            ),
            make_config.route(
                id="s",
                prefix="/s",
                upstreams=make_config.ups(("a",)),
                streaming=True,
                timeout=5.0,
            ),
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    responses = _fire(gb, "/nc/x", 4)
    assert all(r.status_code == 200 for r in responses)
    streams = _fire(gb, "/s", 3)
    assert all(r.status_code == 200 for r in streams)
    # 每个请求都真实回源：4 次普通 + 3 次流式
    assert httpx.get(f"{upstream_base}/a/state").json()["hits"] == 7


# ---------- 合并不改变 TTL 行为：过期后重新回源 ----------
def test_coalesce_respects_ttl(make_config, gateway_server, upstream_base):
    import time

    cfg = make_config(
        [
            make_config.route(
                id="c",
                prefix="/c",
                upstreams=make_config.ups(("a",)),
                cache_ttl=1,
                timeout=5.0,
            )
        ]
    )
    _, start = gateway_server
    gb = start(cfg)
    r1 = httpx.get(f"{gb}/c/t", timeout=5)
    assert r1.headers["x-cache"] == "MISS"
    r2 = httpx.get(f"{gb}/c/t", timeout=5)
    assert r2.headers["x-cache"] == "HIT"
    time.sleep(1.2)
    r3 = httpx.get(f"{gb}/c/t", timeout=5)
    assert r3.headers["x-cache"] == "MISS"
    assert httpx.get(f"{upstream_base}/a/state").json()["hits"] == 2
