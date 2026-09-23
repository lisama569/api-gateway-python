from __future__ import annotations

import httpx


def test_smoke_round_robin_and_cache(gateway_server):
    base, _ = gateway_server
    r1 = httpx.get(f"{base}/api/users/1", timeout=5)
    r2 = httpx.get(f"{base}/api/users/1", timeout=5)
    assert r1.status_code == 200
    assert r1.headers.get("x-cache") == "MISS"
    assert r2.headers.get("x-cache") == "HIT"
    # 缓存命中时上游只被访问一次
    assert r1.json()["hits"] == 1
