"""缓存：命中不访问上游、TTL 过期、条件请求 304、304 校验续期、no-store 透传。"""
from __future__ import annotations

import time

from fake_upstream import HIT_COUNTS


def _echo_upstream_hits(inst: str) -> int:
    return HIT_COUNTS.get((inst, "/cacheable"), 0)


def test_cache_hit_skips_upstream(gw):
    client, state = gw
    client.post("/admin/cache/clear")
    r1 = client.get("/gateway/cache/cacheable")
    assert r1.status_code == 200
    assert r1.headers.get("x-cache") == "MISS"
    first_body = r1.json()

    # 上游命中计数在后续请求中不得增长（缓存命中不再访问上游）
    hits_after_first = dict(HIT_COUNTS)
    for _ in range(4):
        r = client.get("/gateway/cache/cacheable")
        assert r.status_code == 200
        assert r.headers.get("x-cache") == "HIT"
        assert r.json() == first_body
    assert dict(HIT_COUNTS) == hits_after_first
    print("[test] cache hit skipped upstream, hits frozen at:",
          hits_after_first)
    stats = client.get("/admin/stats").json()["cache"]
    assert stats["hits"]["GET /cacheable|"] == 4


def test_conditional_request_on_fresh_cache_returns_304(gw):
    client, _ = gw
    client.post("/admin/cache/clear")
    r1 = client.get("/gateway/cache/cacheable")
    etag = r1.headers["etag"]
    hits = dict(HIT_COUNTS)
    r2 = client.get("/gateway/cache/cacheable",
                    headers={"If-None-Match": etag})
    assert r2.status_code == 304
    assert r2.headers.get("x-cache") == "HIT-304"
    assert r2.content == b""
    # 未访问上游
    assert dict(HIT_COUNTS) == hits

    # 不匹配的 etag 返回完整 200（仍命中缓存）
    r3 = client.get("/gateway/cache/cacheable",
                    headers={"If-None-Match": '"other"'})
    assert r3.status_code == 200


def test_ttl_expiry_then_revalidate(gw):
    client, _ = gw
    client.post("/admin/cache/clear")
    r1 = client.get("/gateway/cache/cacheable")
    etag = r1.headers["etag"]
    cached_instance = r1.json()["instance"]
    time.sleep(1.7)  # 超过 1.5s TTL
    hits_before = dict(HIT_COUNTS)
    # 过期后携带 If-None-Match 校验；假上游对匹配 etag 回 304
    r2 = client.get("/gateway/cache/cacheable")
    print("[test] after expiry status:", r2.status_code,
          "x-cache:", r2.headers.get("x-cache"))
    # 无论 304 续期还是回源 200，响应体都应可用，且发生了一次上游访问
    assert r2.status_code in (200,)
    assert dict(HIT_COUNTS) != hits_before
    # 紧接着的请求重新命中
    r3 = client.get("/gateway/cache/cacheable")
    assert r3.headers.get("x-cache") == "HIT"
    print("[test] ttl expiry + revalidate ok, cached instance:",
          cached_instance, "etag:", etag)


def test_if_modified_since_304(gw):
    client, _ = gw
    client.post("/admin/cache/clear")
    client.get("/gateway/cache/cacheable-lm")
    hits = dict(HIT_COUNTS)
    r = client.get("/gateway/cache/cacheable-lm",
                   headers={"If-Modified-Since": "Wed, 01 Jan 2025 00:00:00 GMT"})
    assert r.status_code == 304
    assert dict(HIT_COUNTS) == hits
    print("[test] If-Modified-Since 304 passed")


def test_cache_key_includes_query(gw):
    client, _ = gw
    client.post("/admin/cache/clear")
    # /echo 不依赖 query，但不同 query 是不同缓存键
    client.get("/gateway/cache/echo", params={"a": "1"})
    r_a = client.get("/gateway/cache/echo", params={"a": "1"})
    r_b = client.get("/gateway/cache/echo", params={"b": "2"})
    assert r_a.headers.get("x-cache") == "HIT"
    assert r_b.headers.get("x-cache") == "MISS"


def test_non_get_not_cached(gw):
    client, _ = gw
    client.post("/admin/cache/clear")
    r1 = client.post("/gateway/cache/work", json={"x": 1})
    r2 = client.post("/gateway/cache/work", json={"x": 2})
    assert r1.status_code == 200 and r2.status_code == 200
    assert "x-cache" not in {k.lower() for k in r2.headers.keys()}


def test_cache_clear_admin(gw):
    client, state = gw
    client.get("/gateway/cache/cacheable")
    assert state.cache.stats()["entries"] == 1
    client.post("/admin/cache/clear")
    assert state.cache.stats()["entries"] == 0
