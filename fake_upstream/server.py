"""本地假上游：在独立端口上模拟多个后端实例。

所有实例由同一个 ASGI 应用承载，路径前缀 /{instance}/... 区分实例：
- GET  /{instance}/api/users[...]   返回实例名、命中次数、可选延迟与 ETag
- GET  /{instance}/api/orders[...]  同上（不缓存）
- GET  /{instance}/**/*vary         按 Accept-Encoding 出变体并声明 Vary
- GET  /{instance}/**/*varyh        按自定义头 X-Variant 出变体并声明 Vary
- GET  /{instance}/stream           SSE 风格分块流式响应，每 0.05s 一帧
- POST /{instance}/state/fail?on=1  全局故障开关：开启后连接层失败（用于熔断/502）
- POST /{instance}/state/slow?ms=N  设置实例延迟（用于 504 超时）
- GET  /{instance}/state            查看计数与状态
"""
from __future__ import annotations

import asyncio
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

# 进程级共享状态（多 worker 不考虑；本地单进程验证）
STATE: dict[str, dict] = {}


def _instance_state(instance: str) -> dict:
    return STATE.setdefault(
        instance,
        {"hits": 0, "fail": False, "slow_ms": 0, "active": 0, "etag": '"v1"'},
    )


def create_upstream(instance: str = "x", latency: float = 0.0) -> FastAPI:
    app = FastAPI(title=f"fake-upstream-{instance}")
    st = _instance_state(instance)
    if latency:
        st["slow_ms"] = int(latency * 1000)

    async def _delay(st: dict) -> None:
        ms = st["slow_ms"]
        if ms:
            await asyncio.sleep(ms / 1000.0)

    @app.api_route("/{instance}/state", methods=["GET"])
    async def get_state(instance: str):
        s = _instance_state(instance)
        return {"instance": instance, **s}

    @app.api_route("/{instance}/state/fail", methods=["POST"])
    async def set_fail(instance: str, on: int = 0):
        s = _instance_state(instance)
        s["fail"] = bool(on)
        return {"instance": instance, "fail": s["fail"]}

    @app.api_route("/{instance}/state/slow", methods=["POST"])
    async def set_slow(instance: str, ms: int = 0):
        s = _instance_state(instance)
        s["slow_ms"] = ms
        return {"instance": instance, "slow_ms": ms}

    @app.api_route("/{instance}/api/{rest:path}", methods=["GET", "POST", "HEAD"])
    async def api(instance: str, rest: str, request: Request):
        return await _echo(instance, rest, request)

    @app.api_route("/{instance}/stream", methods=["GET"])
    async def stream(instance: str, request: Request):
        s = _instance_state(instance)
        s["hits"] += 1

        async def gen():
            for i in range(5):
                yield f"event: chunk\ndata: {instance}-{i}\n\n".encode()
                await asyncio.sleep(0.05)

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.api_route("/{instance}/slow/{ms:int}", methods=["GET"])
    async def slow(instance: str, ms: int):
        # 专用睡眠端点：配合网关 route.timeout 稳定复现 504
        await asyncio.sleep(ms / 1000.0)
        return {"instance": instance, "slept_ms": ms}

    @app.api_route("/{instance}/vary", methods=["GET"])
    async def vary(instance: str, request: Request):
        # 依据 Accept-Encoding 返回不同变体并声明 Vary
        s = _instance_state(instance)
        s["hits"] += 1
        enc = request.headers.get("accept-encoding", "")
        variant = "gzip" if "gzip" in enc else "identity"
        return JSONResponse(
            {"instance": instance, "variant": variant, "hits": s["hits"]},
            headers={
                "vary": "accept-encoding",
                "etag": f'"{variant}"',
                "last-modified": "Mon, 22 Sep 2026 10:00:00 GMT",
            },
        )

    @app.api_route(
        "/{instance}/{rest:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    async def passthrough(instance: str, rest: str, request: Request):
        return await _echo(instance, rest, request)

    async def _echo(instance: str, rest: str, request: Request):
        s = _instance_state(instance)
        s["hits"] += 1  # 到达即计数（包括故障期返回 5xx 的请求）
        await _delay(s)  # 故障期间延迟同样生效，可稳定复现“慢且故障”
        if s["fail"]:
            # 上游应用故障：返回 5xx，由网关原样透传（区别于连接失败的 502）
            return PlainTextResponse("boom", status_code=500)
        # 兼容任意网关前缀下的 /vary（末段为 vary）
        if rest.rstrip("/").endswith("vary"):
            enc = request.headers.get("accept-encoding", "")
            variant = "gzip" if "gzip" in enc else "identity"
            return JSONResponse(
                {"instance": instance, "variant": variant, "hits": s["hits"]},
                headers={
                    "vary": "accept-encoding",
                    "etag": f'"{variant}"',
                    "last-modified": "Mon, 22 Sep 2026 10:00:00 GMT",
                },
            )
        # 末段为 varyh：按自定义头 X-Variant 出不同变体（不在网关保守候选集内，
        # 用于验证“共享结果 Vary 校验不通过 -> 等待方自己回源”的兜底路径）
        if rest.rstrip("/").endswith("varyh"):
            variant = request.headers.get("x-variant", "none")
            return JSONResponse(
                {"instance": instance, "variant": variant, "hits": s["hits"]},
                headers={
                    "vary": "x-variant",
                    "etag": f'"{variant}"',
                    "last-modified": "Mon, 22 Sep 2026 10:00:00 GMT",
                },
            )
        # 兼容任意路径前缀下的 slow/{ms}（网关路由前缀可能不是 /）
        parts = rest.strip("/").split("/")
        if "slow" in parts:
            idx = parts.index("slow")
            if idx + 1 < len(parts):
                try:
                    await asyncio.sleep(int(parts[idx + 1]) / 1000.0)
                except ValueError:
                    pass
        s["active"] += 1
        try:
            payload = {
                "instance": instance,
                "path": rest,
                "hits": s["hits"],
                "query": str(request.url.query),
                "method": request.method,
                "ts": round(time.time(), 3),
            }
            headers = {"etag": s["etag"], "last-modified": "Mon, 22 Sep 2026 10:00:00 GMT"}
            return JSONResponse(payload, headers=headers)
        finally:
            s["active"] -= 1

    return app
