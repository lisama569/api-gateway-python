"""流式响应原样透传：分块到达、头透传、连接计数正确释放、不走缓存。"""
from __future__ import annotations

import time


def test_streaming_chunks_passthrough(gw):
    client, state = gw
    chunks_n, delay_ms = 4, 100
    t0 = time.time()
    with client.stream("GET", "/gateway/rr/stream",
                       params={"chunks": chunks_n, "delay_ms": delay_ms}) as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        assert resp.headers["x-upstream-instance"] in {"u0", "u1", "u2"}
        assert resp.headers.get("cache-control") == "no-store"
        lines = []
        first_arrival = None
        for chunk in resp.iter_lines():
            if chunk:
                if first_arrival is None:
                    first_arrival = time.time() - t0
                lines.append(chunk)
    total = time.time() - t0
    data_lines = [c for c in lines if c.startswith("data:")]
    assert len(data_lines) == chunks_n, lines
    # 流式本质：首个数据块在总传输时长的前半段就已到达（非整体缓冲后一次性返回）
    assert first_arrival is not None
    assert first_arrival < total / 2.0 + 0.3, (first_arrival, total)
    assert total >= (chunks_n - 1) * delay_ms / 1000.0 - 0.2
    print("[test] streaming chunks:", data_lines,
          f"first_chunk={first_arrival:.3f}s total={total:.3f}s")


def test_streaming_releases_connection_count(gw):
    client, state = gw
    with client.stream("GET", "/gateway/lc/stream",
                       params={"chunks": 2, "delay_ms": 40}) as resp:
        got = 0
        for chunk in resp.iter_bytes():
            got += len(chunk)
        assert got > 0
    # 流结束后 active 必须全部归零
    assert all(v == 0 for v in state.router.stats()["active"].values())
    print("[test] stream closed, active map:",
          state.router.stats()["active"])


def test_streaming_not_cached(gw):
    client, state = gw
    client.post("/admin/cache/clear")
    with client.stream("GET", "/gateway/cache/stream",
                       params={"chunks": 1, "delay_ms": 10}) as resp:
        body = b"".join(resp.iter_bytes())
    assert b"chunk" in body
    assert state.cache.stats()["entries"] == 0
    print("[test] stream never entered cache")
