"""上游 HTTP 客户端：普通转发、超时/失败区分、流式透传。"""
from __future__ import annotations

from typing import AsyncIterator

import httpx

from gateway.config import UpstreamConfig
from gateway.errors import UpstreamTimeoutError, UpstreamUnavailableError

HOP_BY_HOP = {
    b"connection", b"keep-alive", b"proxy-authenticate", b"proxy-authorization",
    b"te", b"trailers", b"transfer-encoding", b"upgrade",
}


def _url(upstream: UpstreamConfig, path: str) -> str:
    return upstream.base_url.rstrip("/") + "/" + path.lstrip("/")


class BufferedResponse:
    def __init__(self, status: int,
                 headers: list[tuple[bytes, bytes]], body: bytes) -> None:
        self.status = status
        self.headers = headers
        self.body = body


class StreamResponse:
    def __init__(self, status: int, headers: list[tuple[bytes, bytes]],
                 stream: AsyncIterator[bytes]) -> None:
        self.status = status
        self.headers = headers
        self.stream = stream


def _filter_headers(headers) -> list[tuple[bytes, bytes]]:
    return [(k, v) for k, v in headers
            if k.lower() not in HOP_BY_HOP]


class UpstreamClient:
    def __init__(self, timeout_seconds: float = 5.0) -> None:
        self._timeout = timeout_seconds
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _request_headers(self, headers, body: bytes):
        # 转发除 hop-by-hop 与 content-length（httpx 自行设置）外的头部
        skip = HOP_BY_HOP | {b"content-length", b"host"}
        result = [(k, v) for k, v in headers if k.lower() not in skip]
        return result

    async def send_buffered(self, upstream: UpstreamConfig, method: str, path: str,
                            headers, body: bytes) -> BufferedResponse:
        await self.start()
        assert self._client is not None
        url = _url(upstream, path)
        try:
            resp = await self._client.request(
                method, url, content=body,
                headers=self._request_headers(headers, body),
            )
        except httpx.TimeoutException as exc:
            print(f"[upstream] TIMEOUT upstream={upstream.id} url={url} {exc}")
            raise UpstreamTimeoutError(
                f"upstream {upstream.id} timed out after {self._timeout}s",
                retry_after=self._timeout,
            ) from exc
        except httpx.TransportError as exc:
            print(f"[upstream] UNAVAILABLE upstream={upstream.id} url={url} {exc}")
            raise UpstreamUnavailableError(
                f"upstream {upstream.id} unavailable: {exc.__class__.__name__}",
            ) from exc
        print(f"[upstream] OK upstream={upstream.id} status={resp.status_code} "
              f"bytes={len(resp.content)}")
        return BufferedResponse(
            resp.status_code, _filter_headers(resp.headers.raw), resp.content,
        )

    async def stream(self, upstream: UpstreamConfig, method: str, path: str,
                     headers, body: bytes) -> StreamResponse:
        """打开流式连接，失败立即区分超时/不可用；chunk 由调用方透传。"""
        await self.start()
        assert self._client is not None
        url = _url(upstream, path)
        req = self._client.build_request(
            method, url, content=body or None,
            headers=self._request_headers(headers, body),
        )
        try:
            cr = await self._client.send(req, stream=True)
        except httpx.TimeoutException as exc:
            print(f"[upstream] STREAM TIMEOUT upstream={upstream.id} url={url}")
            raise UpstreamTimeoutError(
                f"upstream {upstream.id} timed out opening stream",
                retry_after=self._timeout,
            ) from exc
        except httpx.TransportError as exc:
            print(f"[upstream] STREAM UNAVAILABLE upstream={upstream.id} {exc}")
            raise UpstreamUnavailableError(
                f"upstream {upstream.id} unavailable: {exc.__class__.__name__}",
            ) from exc
        print(f"[upstream] STREAM OPEN upstream={upstream.id} "
              f"status={cr.status_code}")

        async def chunks() -> AsyncIterator[bytes]:
            try:
                async for chunk in cr.aiter_raw():
                    if chunk:
                        yield chunk
            finally:
                await cr.aclose()
                print(f"[upstream] STREAM CLOSE upstream={upstream.id}")

        return StreamResponse(
            cr.status_code, _filter_headers(cr.headers.raw), chunks(),
        )
