"""代理主流程：串联 ACL、路由、限流、缓存与上游转发，并打印判定日志。"""
from __future__ import annotations

from fastapi import Request
from fastapi.responses import Response, StreamingResponse

from gateway.cache import CachedResponse, make_cache_key, now_http
from gateway.errors import GatewayError, error_response
from gateway.proxy_headers import (
    client_ip,
    filtered_request_headers,
    get_header,
)

# 全局运行时状态，由 main.py 在启动时注入
state: "object" = None


def _strip_route_prefix(route_path: str, strip: str, path: str) -> str:
    if strip:
        prefix = strip.rstrip("/")
        if path == prefix:
            return "/"
        if path.startswith(prefix + "/"):
            return path[len(prefix):]
    # 默认剥掉路由挂载点 /gateway
    if path.startswith("/gateway"):
        rest = path[len("/gateway"):]
        return rest or "/"
    return path


def _upstream_path(route, path: str) -> str:
    """默认剥掉网关挂载点 /gateway 与路由前缀，得到上游真实路径。"""
    if route.strip_prefix:
        return _strip_route_prefix(route.path_prefix, route.strip_prefix, path)
    rest = path[len("/gateway"):] if path.startswith("/gateway") else path
    # rest 形如 /rr/echo，再剥掉路由前缀 /rr
    route_suffix = route.path_prefix
    if route_suffix.startswith("/gateway"):
        route_suffix = route_suffix[len("/gateway"):]
    route_suffix = route_suffix.rstrip("/")
    if rest == route_suffix:
        return "/"
    if rest.startswith(route_suffix + "/"):
        rest = rest[len(route_suffix):]
    return rest or "/"


def _client_ip(request: Request) -> str:
    return client_ip(request)


def _cache_raw_headers(entry: CachedResponse,
                       extra: list[tuple[bytes, bytes]] | None = None
                       ) -> list[tuple[bytes, bytes]]:
    import time
    headers = list(entry.headers)
    headers.append((b"age", str(max(0, int(time.time() - entry.stored_at))).encode()))
    headers.append((b"x-cache", b"HIT"))
    if extra:
        headers.extend(extra)
    return headers


def _build_304(entry: CachedResponse) -> Response:
    keep = [(k, v) for k, v in entry.headers
            if k.lower() in (b"etag", b"last-modified", b"cache-control",
                             b"date", b"expires", b"vary")]
    keep.append((b"x-cache", b"HIT-304"))
    return _response(304, b"", keep)


async def handle_proxy_request(request: Request) -> Response:
    assert state is not None, "gateway state not initialized"
    ip = _client_ip(request)

    try:
        # 1) 来源白/黑名单
        state.acl.check(ip)

        # 2) 路由匹配
        route = state.router.match(request.method, request.url.path)

        # 3) 限流：优先路由级配置，其次全局默认
        rl_cfg = route.rate_limit or state.config.default_rate_limit
        state.limiter.check(ip, route.id, rl_cfg)

        # 4) 流式判定：带 stream 标记的上游路径，或客户端要求 SSE
        upstream_path = _upstream_path(route, request.url.path)
        if request.url.query:
            upstream_path = upstream_path + "?" + request.url.query
        wants_stream = (
            upstream_path.startswith("/stream")
            or get_header(request.headers, "x-gateway-stream") == "1"
        )

        if wants_stream:
            return await _stream_passthrough(route, request, upstream_path, ip)

        # 5) 缓存（仅 GET/HEAD 且配置了 TTL）
        cacheable_route = request.method in ("GET", "HEAD") and route.cache_ttl > 0
        if cacheable_route:
            cached = await _serve_from_cache_or_upstream(
                route, request, upstream_path, ip)
            return cached

        return await _forward_buffered(route, request, upstream_path, ip,
                                       cache=False)

    except GatewayError as exc:
        return error_response(exc)


async def _read_body(request: Request) -> bytes:
    return await request.body()


async def _serve_from_cache_or_upstream(route, request: Request,
                                        upstream_path: str,
                                        ip: str) -> Response:
    key = make_cache_key(request.method, upstream_path, {})  # 含 query
    entry = state.cache.get(key)

    client_etags = tuple(
        t.strip() for t in
        (get_header(request.headers, "if-none-match") or "").split(",") if t.strip()
    )
    ims = get_header(request.headers, "if-modified-since")

    if entry is not None:
        # 新鲜命中：不再访问上游
        if client_etags and state.cache.not_modified(key, entry, client_etags):
            return _build_304(entry)
        if ims and state.cache.not_modified_since(entry, ims):
            print(f"[cache] CONDITIONAL key={key!r} If-Modified-Since -> 304")
            return _build_304(entry)
        return _response(entry.status, entry.body,
                         _cache_raw_headers(entry))

    # 未命中或过期：访问上游；若有旧条目则附带校验头
    stale = state.cache.get_stale(key)
    extra_headers = list(filtered_request_headers(request.headers))
    if stale is not None:
        if stale.etag:
            extra_headers.append(("if-none-match", stale.etag))
        if stale.last_modified:
            extra_headers.append(("if-modified-since", stale.last_modified))

    body = await _read_body(request)
    selected = state.router.select(route)
    print(f"[proxy] forward cache-lookup MISS route={route.id} "
          f"upstream={selected.id} path={upstream_path} client={ip}")
    try:
        raw = [(k.encode(), v.encode()) for k, v in extra_headers]
        resp = await state.upstream.send_buffered(
            selected, request.method, upstream_path, raw, body,
        )
    finally:
        state.router.release(selected.id)

    # 上游确认缓存仍有效：复用旧缓存体并续期
    if resp.status == 304 and stale is not None:
        refreshed = state.cache.revalidate(key, route.cache_ttl)
        if refreshed is not None:
            return _response(refreshed.status, refreshed.body,
                             _cache_raw_headers(refreshed))

    if _is_cacheable(resp.status, resp.headers):
        etag = _header_str(resp.headers, b"etag")
        lm = _header_str(resp.headers, b"last-modified")
        import time
        now = time.time()
        entry = CachedResponse(
            status=resp.status, headers=list(resp.headers),
            body=resp.body, etag=etag, last_modified=lm,
            stored_at=now, expires_at=now + route.cache_ttl,
        )
        state.cache.put(key, entry)

    # 客户端的新鲜条件请求直接打到上游时的 304
    if resp.status == 304:
        return _response(304, b"", resp.headers)

    miss_headers = list(resp.headers) + [(b"x-cache", b"MISS")]
    return _response(resp.status, resp.body, miss_headers)


async def _forward_buffered(route, request: Request, upstream_path: str,
                            ip: str, cache: bool) -> Response:
    body = await _read_body(request)
    headers = filtered_request_headers(request.headers)
    selected = state.router.select(route)
    print(f"[proxy] forward route={route.id} upstream={selected.id} "
          f"path={upstream_path} method={request.method} client={ip} "
          f"bytes={len(body)}")
    try:
        raw = [(k.encode(), v.encode()) for k, v in headers]
        resp = await state.upstream.send_buffered(
            selected, request.method, upstream_path, raw, body,
        )
    finally:
        state.router.release(selected.id)

    return _response(resp.status, resp.body, resp.headers)


async def _stream_passthrough(route, request: Request, upstream_path: str,
                              ip: str) -> StreamingResponse:
    body = await _read_body(request)
    headers = filtered_request_headers(request.headers)
    selected = state.router.select(route)
    print(f"[proxy] STREAM passthrough route={route.id} "
          f"upstream={selected.id} path={upstream_path} client={ip}")
    try:
        raw = [(k.encode(), v.encode()) for k, v in headers]
        sresp = await state.upstream.stream(
            selected, request.method, upstream_path, raw, body,
        )
    except Exception:
        state.router.release(selected.id)
        raise

    media_type = _content_type(sresp.headers)

    async def wrapped():
        try:
            async for chunk in sresp.stream:
                yield chunk
        finally:
            state.router.release(selected.id)

    streaming = StreamingResponse(wrapped(), status_code=sresp.status,
                                  media_type=media_type)
    streaming.raw_headers = list(sresp.headers)
    return streaming



def _response(status: int, body: bytes,
              raw_headers: list[tuple[bytes, bytes]]) -> Response:
    """用上游原始字节头构造响应；content-length 按 body 重新计算。"""
    resp = Response(content=body, status_code=status)
    # 此刻 raw_headers 含 Starlette 计算的 content-length / content-type
    star_ct = next((v for k, v in resp.raw_headers
                    if k.lower() == b"content-type"), None)
    cl = next((v for k, v in resp.raw_headers
               if k.lower() == b"content-length"), None)
    kept = [(k, v) for k, v in raw_headers
            if k.lower() not in (b"content-length", b"transfer-encoding")]
    if not any(k.lower() == b"content-type" for k, _ in kept) and star_ct:
        kept.append((b"content-type", star_ct))
    if cl is not None and status >= 200 and status != 204:
        kept.append((b"content-length", cl))
    resp.raw_headers = kept
    return resp


def _content_type(headers: list[tuple[bytes, bytes]]) -> str | None:
    for k, v in headers:
        if k.lower() == b"content-type":
            return v.decode()
    return None


def _header_str(headers: list[tuple[bytes, bytes]], name: bytes) -> str:
    for k, v in headers:
        if k.lower() == name:
            return v.decode()
    return ""


def _is_cacheable(status: int, headers) -> bool:
    if status not in (200, 203, 204, 301, 308, 404, 405, 410, 414, 501):
        return False
    cc = _header_str(headers, b"cache-control").lower()
    if "no-store" in cc or "private" in cc:
        print("[cache] upstream response marked no-store/private -> bypass")
        return False
    return True
