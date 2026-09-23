"""反向代理核心：鉴权 -> 限流 -> 缓存 -> 转发。

失败判定（HTTP 状态码与 JSON error 码均可区分）：
- 404 no_route          无匹配路由
- 403 ip_blocked        命中黑名单
- 403 ip_not_allowed    白名单非空且来源不在其中
- 429 rate_limited      限流（含 Retry-After 与 scope）
- 503 no_upstream       路由存在但全部实例不健康（失败会触发熔断标记）
- 504 upstream_timeout  上游超时
- 502 upstream_error    上游连接/协议错误

流式路由（route.streaming=True）与不可缓存方法一律透传：不读缓存、不写缓存，
响应体以 chunked 方式原样回传。
"""
from __future__ import annotations

from email.utils import parsedate_to_datetime

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from .access import AccessController
from .cache import CACHEABLE_METHODS, HOP_BY_HOP, CachedResponse, ResponseCache
from .logging_utils import log_cache, log_rate_limit, log_route
from .ratelimit import RateLimiter
from .router import Router
from .upstream import NoHealthyUpstreamError, UpstreamSelector


def error_response(status: int, code: str, message: str, headers=None, **extra) -> JSONResponse:
    body = {"error": code, "message": message}
    body.update(extra)
    return JSONResponse(status_code=status, content=body, headers=headers or {})


def filter_response_headers(headers) -> list[tuple[bytes, bytes]]:
    return [
        (k, v)
        for k, v in headers
        if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"
    ]


def cache_control_no_store(headers) -> bool:
    cc = headers.get("cache-control", "")
    tokens = {t.strip().lower() for t in cc.split(",") if t.strip()}
    return bool(tokens & {"no-store", "private"})


def parse_http_date(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).timestamp()
    except (TypeError, ValueError):
        return None


class Gateway:
    def __init__(
        self,
        router: Router,
        selector: UpstreamSelector,
        limiter: RateLimiter,
        cache: ResponseCache,
        client: httpx.AsyncClient,
        logger=None,
    ) -> None:
        from .logging_utils import get_logger

        self.router = router
        self.selector = selector
        self.limiter = limiter
        self.cache = cache
        self.client = client
        self.logger = logger or get_logger("gateway.proxy")

    # ---- 入口 -----------------------------------------------------------
    async def handle(self, request: Request) -> Response:
        client_ip = request.client.host if request.client else "unknown"

        route, snap, _route_reason = self.router.match(request.url.path)
        if route is None:
            self.logger.info("route path=%s decision=none reason=no_route", request.url.path)
            return error_response(404, "no_route", "no route matched", path=request.url.path)

        allowed, ac_reason = AccessController(snap.allowlist, snap.blocklist).decide(client_ip)
        if not allowed:
            code = "ip_blocked" if ac_reason == "blocked" else "ip_not_allowed"
            self.logger.info("access client=%s decision=deny reason=%s", client_ip, ac_reason)
            return error_response(403, code, f"client {ac_reason}", client=client_ip)

        denied = self._check_limits(snap, route, client_ip)
        if denied is not None:
            return denied

        streaming = bool(route.streaming)
        cacheable = request.method in CACHEABLE_METHODS and not streaming and route.cache_ttl > 0

        if cacheable:
            served = self._try_cache(request, route)
            if served is not None:
                return served

        return await self._forward(request, route, client_ip, streaming, cacheable)

    # ---- 限流 -----------------------------------------------------------
    def _check_limits(self, snap, route, client_ip):
        # 维度：全局聚合 / 每客户端（全局规则）/ 路由聚合 / 每客户端在该路由
        checks = [
            ("global", "global", snap.global_rate_limit),
            ("client", client_ip, snap.global_rate_limit),
            ("route", route.id, route.rate_limit),
            ("client_route", f"{client_ip}|{route.id}", route.rate_limit),
        ]
        for scope, key, rule in checks:
            if rule is None:
                continue
            allowed = self.limiter.check(scope, key, rule)
            remaining, capacity = self.limiter.state(scope, key) or (0.0, rule.capacity)
            log_rate_limit(self.logger, scope, key, remaining, capacity, allowed)
            if not allowed:
                retry = self.limiter.retry_after(scope, key)
                if retry == float("inf"):
                    retry_header = "3600"  # 速率为 0，令牌永不补充
                    retry_value = 3600.0
                else:
                    retry_header = str(max(1, int(retry + 0.999)))
                    retry_value = round(retry, 3)
                return error_response(
                    429,
                    "rate_limited",
                    f"rate limit exceeded at scope={scope}",
                    headers={"retry-after": retry_header},
                    scope=scope,
                    retry_after=retry_value,
                )
        return None

    # ---- 缓存 -----------------------------------------------------------
    def _try_cache(self, request: Request, route) -> Response | None:
        cache_key, cached = self.cache.lookup(
            route.id,
            request.method,
            request.url.path,
            request.url.query,
            request.headers,
        )
        if cached is None or cache_key is None:
            self.cache.misses += 1
            log_cache(
                self.logger,
                self.cache.base_key(
                    route.id, request.method, request.url.path, request.url.query
                ),
                "MISS",
                "no fresh variant",
            )
            return None

        # 新鲜命中：本地处理条件请求，绝不访问上游
        if self._not_modified(request, cached):
            self.cache.validated += 1
            log_cache(self.logger, cache_key, "304", "conditional validators match")
            return Response(status_code=304, headers=self._validator_headers(cached))

        self.cache.hits += 1
        log_cache(self.logger, cache_key, "HIT", f"fresh ttl={cached.ttl}")
        headers = {
            k.decode("latin1"): v.decode("latin1") for k, v in cached.headers
        }
        headers["x-cache"] = "HIT"
        return Response(status_code=cached.status, content=cached.body, headers=headers)

    @staticmethod
    def _not_modified(request: Request, cached: CachedResponse) -> bool:
        inm = request.headers.get("if-none-match")
        if inm:
            etag = cached.etag
            if etag is None:
                return False
            tags = {t.strip().strip('"').removeprefix("W/").strip('"') for t in inm.split(",")}
            return "*" in tags or etag.strip('"').removeprefix("W/").strip('"') in tags
        ims = parse_http_date(request.headers.get("if-modified-since"))
        if ims is not None:
            lm = parse_http_date(cached.last_modified)
            if lm is None:
                return False
            return lm <= ims
        return False

    @staticmethod
    def _validator_headers(cached: CachedResponse) -> dict[str, str]:
        out = {}
        if cached.etag:
            out["etag"] = cached.etag
        if cached.last_modified:
            out["last-modified"] = cached.last_modified
        return out

    # ---- 转发 -----------------------------------------------------------
    async def _forward(self, request: Request, route, client_ip: str, streaming: bool, cacheable: bool) -> Response:
        try:
            upstream = self.selector.acquire(route)
        except NoHealthyUpstreamError:
            log_route(self.logger, route.id, None, "all upstreams down")
            return error_response(
                503, "no_upstream", "no healthy upstream", route=route.id
            )

        target_url = upstream.base_url.rstrip("/") + request.url.path
        if request.url.query:
            target_url += "?" + request.url.query
        log_route(
            self.logger,
            route.id,
            upstream.name,
            f"strategy={route.strategy.value} streaming={streaming} target={target_url}",
        )

        forward_headers = [
            (k, v)
            for k, v in request.headers.raw
            if k.lower() not in HOP_BY_HOP and k.lower() != "host"
        ]
        forward_headers.append((b"x-forwarded-for", client_ip.encode()))
        body = await request.body()

        try:
            if streaming:
                return await self._stream(
                    route, upstream, request, target_url, forward_headers, body
                )

            upstream_resp = await self.client.request(
                request.method,
                target_url,
                headers=forward_headers,
                content=body if body else None,
                timeout=route.timeout,
            )
        except httpx.TimeoutException as exc:
            self.selector.mark_down(route, upstream)
            self.selector.release(route.id, upstream)
            return error_response(
                504, "upstream_timeout", str(exc), upstream=upstream.name
            )
        except httpx.TransportError as exc:
            self.selector.mark_down(route, upstream)
            self.selector.release(route.id, upstream)
            return error_response(
                502, "upstream_error", str(exc), upstream=upstream.name
            )

        self.selector.release(route.id, upstream)
        resp_headers = filter_response_headers(upstream_resp.headers.raw)

        # 缓存写入：仅 200 且上游未声明 no-store/private，且 Vary 不含 *
        if (
            cacheable
            and upstream_resp.status_code == 200
            and not cache_control_no_store(upstream_resp.headers)
        ):
            stored = CachedResponse(
                status=upstream_resp.status_code,
                headers=resp_headers,
                body=upstream_resp.content,
                stored_at=self.cache._clock(),
                ttl=route.cache_ttl,
            )
            if not ResponseCache.is_vary_star(stored):
                key = self.cache.store(
                    route.id,
                    request.method,
                    request.url.path,
                    request.url.query,
                    request.headers,
                    stored,
                )
                log_cache(
                    self.logger,
                    key,
                    "STORE",
                    f"ttl={route.cache_ttl} bytes={len(stored.body)}",
                )
            resp_headers = [(b"x-cache", b"MISS")] + resp_headers

        headers = {
            k.decode("latin1"): v.decode("latin1") for k, v in resp_headers
        }
        return Response(
            status_code=upstream_resp.status_code,
            content=upstream_resp.content,
            headers=headers,
        )

    async def _stream(self, route, upstream, request, target_url, forward_headers, body):
        try:
            req = self.client.build_request(
                request.method,
                target_url,
                headers=forward_headers,
                content=body if body else None,
                timeout=route.timeout,
            )
            upstream_resp = await self.client.send(req, stream=True)
        except httpx.TimeoutException as exc:
            self.selector.mark_down(route, upstream)
            self.selector.release(route.id, upstream)
            return error_response(504, "upstream_timeout", str(exc), upstream=upstream.name)
        except httpx.TransportError as exc:
            self.selector.mark_down(route, upstream)
            self.selector.release(route.id, upstream)
            return error_response(502, "upstream_error", str(exc), upstream=upstream.name)

        async def body_iter():
            try:
                async for chunk in upstream_resp.aiter_raw():
                    yield chunk
            finally:
                await upstream_resp.aclose()
                self.selector.release(route.id, upstream)

        return StreamingResponse(
            content=body_iter(),
            status_code=upstream_resp.status_code,
            headers={
                k.decode("latin1"): v.decode("latin1")
                for k, v in filter_response_headers(upstream_resp.headers.raw)
            },
        )
