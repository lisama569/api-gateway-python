"""本地假上游：模拟多个后端实例。

每个实例提供：
  GET  /echo、/count        返回实例 id 与该实例处理计数
  GET  /slow?ms=N           延迟 N 毫秒（模拟超时）
  GET  /fail                500 失败
  POST /work                回显 body
  GET  /stream              text/event-stream 分块流式输出
  GET  /cacheable           带 ETag 的可缓存响应，支持条件请求回 304
  GET  /cacheable-lm        带 Last-Modified 的可缓存响应
"""
from __future__ import annotations

import asyncio
import itertools
import time
from email.utils import formatdate

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

# (instance_id, path) -> 处理次数
HIT_COUNTS: dict[tuple[str, str], int] = {}


def _hit(instance_id: str, path: str) -> int:
    key = (instance_id, path)
    HIT_COUNTS[key] = HIT_COUNTS.get(key, 0) + 1
    return HIT_COUNTS[key]


def create_fake_app(instance_id: str) -> FastAPI:
    app = FastAPI(title=f"fake-upstream-{instance_id}")
    counters = itertools.count(1)

    @app.get("/echo")
    async def echo(request: Request) -> dict[str, object]:
        n = _hit(instance_id, "/echo")
        return {"instance": instance_id, "seq": next(counters), "hits": n,
                "x-request-id": request.headers.get("x-request-id")}

    @app.get("/count")
    async def count() -> dict[str, object]:
        return {"instance": instance_id,
                "echo_hits": HIT_COUNTS.get((instance_id, "/echo"), 0)}

    @app.get("/slow")
    async def slow(ms: int = 2000) -> dict[str, object]:
        await asyncio.sleep(ms / 1000.0)
        return {"instance": instance_id, "slept_ms": ms}

    @app.get("/fail")
    async def fail() -> Response:
        _hit(instance_id, "/fail")
        return Response(status_code=500,
                        content=b'{"error":"boom"}',
                        media_type="application/json")

    @app.post("/work")
    async def work(request: Request) -> dict[str, object]:
        body = await request.body()
        return {"instance": instance_id, "body": body.decode("utf-8", "replace"),
                "length": len(body)}

    @app.get("/stream")
    async def stream(chunks: int = 5, delay_ms: int = 50) -> StreamingResponse:
        async def gen():
            for i in range(chunks):
                await asyncio.sleep(delay_ms / 1000.0)
                yield f"event: chunk\ndata: {instance_id}-{i}\n\n".encode()

        return StreamingResponse(
            gen(), media_type="text/event-stream",
            headers={"X-Upstream-Instance": instance_id,
                     "Cache-Control": "no-store"},
        )

    @app.get("/cacheable")
    async def cacheable(request: Request) -> Response:
        n = _hit(instance_id, "/cacheable")
        etag = f'"v1-{instance_id}"'
        inm = request.headers.get("if-none-match")
        if inm and etag in [t.strip() for t in inm.split(",")]:
            return Response(status_code=304, headers={"ETag": etag,
                                                       "X-Hits": str(n)})
        return Response(
            content=f'{{"instance":"{instance_id}","hits":{n}}}'.encode(),
            media_type="application/json",
            headers={"ETag": etag,
                     "Cache-Control": f"max-age={60}",
                     "X-Hits": str(n)},
        )

    @app.get("/cacheable-lm")
    async def cacheable_lm(request: Request) -> Response:
        n = _hit(instance_id, "/cacheable-lm")
        lm = "Mon, 01 Jan 2024 00:00:00 GMT"
        ims = request.headers.get("if-modified-since")
        if ims:
            return Response(status_code=304, headers={"Last-Modified": lm,
                                                       "X-Hits": str(n)})
        return Response(
            content=f'{{"instance":"{instance_id}","hits":{n}}}'.encode(),
            media_type="application/json",
            headers={"Last-Modified": lm, "Cache-Control": "max-age=60",
                     "Date": formatdate(time.time(), usegmt=True),
                     "X-Hits": str(n)},
        )

    return app
