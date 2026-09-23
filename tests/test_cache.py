"""缓存单测：TTL、新鲜度、条件请求校验、Vary 键、no-store。"""
from __future__ import annotations

import logging

from starlette.datastructures import Headers

from gateway.cache import CachedResponse, ResponseCache


def make_cached(ttl=5, now=1000.0, etag='"v1"', vary=None):
    headers = [(b"etag", etag.encode()), (b"last-modified", b"Mon, 22 Sep 2026 10:00:00 GMT")]
    if vary:
        headers.append((b"vary", vary.encode()))
    return CachedResponse(200, headers, b"hello", now, ttl)


def test_fresh_hit_then_expire():
    clock = [1000.0]
    cache = ResponseCache(clock=lambda: clock[0])
    cache.put("k", make_cached(ttl=5))
    assert cache.get("k") is not None
    clock[0] = 1004.9
    assert cache.get("k") is not None
    clock[0] = 1005.0
    assert cache.get("k") is None  # 过期条目被惰性删除


def test_make_key_includes_parts():
    h = Headers({"accept-encoding": "gzip"})
    k1 = ResponseCache.make_key("r", "GET", "/a", "x=1", h)
    k2 = ResponseCache.make_key("r", "GET", "/a", "x=2", h)
    k3 = ResponseCache.make_key("r", "GET", "/a", "x=1", h, ["accept-encoding"])
    k4 = ResponseCache.make_key(
        "r", "GET", "/a", "x=1", Headers({"accept-encoding": "br"}), ["accept-encoding"]
    )
    assert k1 != k2
    assert k1 != k3
    assert k3 != k4


def test_vary_fields():
    cr = make_cached(vary="Accept-Encoding, Accept-Language")
    assert ResponseCache.vary_fields(cr) == ["Accept-Encoding", "Accept-Language"]
    assert ResponseCache.vary_fields(make_cached()) == []


def test_etag_validators():
    cr = make_cached()
    assert cr.etag == '"v1"'
    assert cr.last_modified.startswith("Mon, 22 Sep 2026")


def test_cache_log(caplog):
    caplog.set_level(logging.INFO, logger="gateway.cache-test")
    from gateway.logging_utils import log_cache

    log_cache(logging.getLogger("gateway.cache-test"), "kk", "HIT", "fresh")
    assert "key=kk" in caplog.text and "decision=HIT" in caplog.text


def test_lookup_matches_variant():
    from starlette.datastructures import Headers
    cache = ResponseCache(clock=lambda: 1000.0)
    gzip_resp = CachedResponse(
        200, [(b"vary", b"accept-encoding"), (b"etag", b'"gzip"')], b"gz", 1000.0, 10
    )
    id_resp = CachedResponse(
        200, [(b"vary", b"accept-encoding"), (b"etag", b'"id"')], b"id", 1000.0, 10
    )
    cache.store("r", "GET", "/v", "", Headers({"accept-encoding": "gzip"}), gzip_resp)
    cache.store("r", "GET", "/v", "", Headers({"accept-encoding": "identity"}), id_resp)

    _, hit = cache.lookup("r", "GET", "/v", "", Headers({"accept-encoding": "gzip"}))
    assert hit.body == b"gz"
    _, hit2 = cache.lookup("r", "GET", "/v", "", Headers({"accept-encoding": "identity"}))
    assert hit2.body == b"id"
    miss_key, miss = cache.lookup("r", "GET", "/v", "", Headers({"accept-encoding": "deflate"}))
    assert miss is None and miss_key is None
