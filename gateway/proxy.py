"""反向代理核心：鉴权 -> 限流 -> 缓存 -> 转发（含重试/故障转移）。

失败判定（HTTP 状态码与 JSON error 码均可区分）：
- 404 no_route          无匹配路由
- 403 ip_blocked        命中黑名单
- 403 ip_not_allowed    白名单非空且来源不在其中
- 429 rate_limited      限流（含 Retry-After 与 scope）
- 503 no_upstream       路由存在但全部实例不健康（失败会触发熔断标记）
- 504 upstream_timeout  上游超时（重试耗尽时附加 attempts/tried_upstreams）
- 502 upstream_error    上游连接/协议错误（重试耗尽时附加 attempts/tried_upstreams）

重试与故障转移（route.retry 配置，仅幂等安全方法 GET/HEAD/OPTIONS 且非流式）：
- 每次尝试都是干净等价的请求：重新选择实例（排除本请求已试过的）、重建请求头、
  复用同一份请求体字节，不带上一跳残留状态。
- 上限：max_attempts（总次数）、per_try_timeout（单次）、budget（整体）。
- 写操作与流式请求不重试：按原有单次失败结果返回。
- 全部失败：状态码与 error 码沿用最后一跳的类型（504/502），
  附加 attempts 与 tried_upstreams；若最后一跳是上游 5xx 响应，则原样透传该响应，
  并附 x-gateway-tried 头标明实际试过的实例。

流式路由（route.streaming=True）与不可缓存方法一律透传：不读缓存、不写缓存，
响应体以 chunked 方式原样回传。
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from .access import AccessController
from .cache import CACHEABLE_METHODS, HOP_BY_HOP, CachedResponse, ResponseCache
from .coalesce import SharedResult, SingleFlight
from .logging_utils import log_cache, log_rate_limit, log_route
from .ratelimit import RateLimiter
from .router import Router
from .upstream import NoHealthyUpstreamError, UpstreamSelector

# 仅这些幂等安全方法允许重试；写操作（POST/PUT/PATCH/DELETE）重放有风险，不重试
SAFE_RETRY_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


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


@dataclass
class _AttemptFailure:
    """单次尝试的失败记录（用于重试耗尽后的可区分失败原因）。"""

    kind: str  # "timeout" | "transport" | "status"
    upstream: str
    message: str
    status: int | None = None


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
        self.flights = SingleFlight()
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
            if route.coalesce_wait > 0:
                # 同键并发请求合并回源：只放行一个，其余等待共享结果
                key = self.cache.coalesce_key(
                    route.id,
                    request.method,
                    request.url.path,
                    request.url.query,
                    request.headers,
                )
                return await self._coalesced_forward(
                    key, route.coalesce_wait, request, route, client_ip
                )

        return await self._forward(request, route, client_ip, streaming, cacheable)

    # ---- 合并回源 -------------------------------------------------------
    async def _coalesced_forward(
        self, key: str, wait_timeout: float, request: Request, route, client_ip: str
    ) -> Response:
        is_leader, flight = self.flights.acquire(key)
        if not is_leader:
            result = await self.flights.wait(flight, wait_timeout)
            if result is None:
                self.logger.info(
                    "coalesce key=%s role=waiter decision=timeout wait=%.1fs",
                    key, wait_timeout,
                )
                return error_response(
                    504,
                    "coalesce_wait_timeout",
                    f"timed out waiting for shared origin fetch after {wait_timeout}s",
                    route=route.id,
                )
            if result == "aborted":
                # leader 异常退出：不共享、不挂起，自己重新发起一次
                self.logger.info(
                    "coalesce key=%s role=waiter decision=leader_aborted -> self fetch", key
                )
                return await self._forward(request, route, client_ip, False, True)
            if not self._vary_compatible(result, request):
                # 共享结果的 Vary 与本请求头对不上（如冷启动时键里还没有该字段）：
                # 不能套用别人的结果，自己回源拿与请求头匹配的响应
                self.logger.info(
                    "coalesce key=%s role=waiter decision=vary_mismatch -> self fetch", key
                )
                return await self._forward(request, route, client_ip, False, True)
            self.logger.info(
                "coalesce key=%s role=waiter decision=shared status=%d",
                key, result.status,
            )
            headers = {
                k.decode("latin1"): v.decode("latin1") for k, v in result.headers
            }
            headers["x-coalesce"] = "shared"
            return Response(status_code=result.status, content=result.body, headers=headers)

        self.logger.info("coalesce key=%s role=leader decision=fetch", key)
        try:
            resp = await self._forward(request, route, client_ip, False, True)
        except Exception:
            self.flights.abort(flight)
            raise
        # 先共享原始结果，再给 leader 自己的响应打标记，避免污染 waiter 副本
        self.flights.complete(
            flight,
            SharedResult(
                resp.status_code,
                list(resp.headers.raw),
                resp.body,
                {
                    k.decode("latin1").lower(): v.decode("latin1")
                    for k, v in request.headers.raw
                },
            ),
        )
        resp.headers["x-coalesce"] = "leader"
        return resp

    @staticmethod
    def _vary_compatible(result: SharedResult, request: Request) -> bool:
        """共享结果的 Vary 字段是否与当前请求头一致。

        响应未声明 Vary 时视为可共享；Vary: * 永远不可共享；
        否则逐字段比较 leader 与当前请求的请求头值。
        """
        raw_vary: str | None = None
        for k, v in result.headers:
            if k.lower() == b"vary":
                raw_vary = v.decode("latin1")
                break
        if raw_vary is None:
            return True
        if raw_vary.strip() == "*":
            return False
        for name in (token.strip().lower() for token in raw_vary.split(",") if token.strip()):
            if result.req_headers.get(name, "") != request.headers.get(name, ""):
                return False
        return True

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

    # ---- 转发（含重试/故障转移）-----------------------------------------
    async def _forward(
        self,
        request: Request,
        route,
        client_ip: str,
        streaming: bool,
        cacheable: bool,
    ) -> Response:
        body = await request.body()

        retry = route.retry
        retryable = (
            retry is not None
            and retry.max_attempts > 1
            and request.method in SAFE_RETRY_METHODS
            and not streaming
        )
        max_attempts = retry.max_attempts if retryable else 1
        per_try = (retry.per_try_timeout or route.timeout) if retry else route.timeout
        budget = retry.budget if retryable else None
        retry_on_5xx = bool(retry and retry.retry_on_5xx) and retryable
        deadline = (time.monotonic() + budget) if budget else None

        tried: list[str] = []
        failures: list[_AttemptFailure] = []
        excluded: set[str] = set()
        last_status_resp: httpx.Response | None = None

        for attempt in range(1, max_attempts + 1):
            if attempt > 1 and deadline is not None and time.monotonic() >= deadline:
                self.logger.info(
                    "retry route=%s decision=stop reason=budget_exhausted attempts=%d",
                    route.id, len(tried),
                )
                break
            try:
                upstream = self.selector.acquire(route, exclude=frozenset(excluded))
            except NoHealthyUpstreamError:
                if not failures:
                    log_route(self.logger, route.id, None, "all upstreams down")
                    return error_response(
                        503, "no_upstream", "no healthy upstream", route=route.id
                    )
                self.logger.info(
                    "retry route=%s decision=stop reason=no_more_healthy_upstream tried=%s",
                    route.id, ",".join(tried),
                )
                break

            tried.append(upstream.name)
            probe = self.selector.state_of(route.id, upstream.name) == "half_open"
            target_url = upstream.base_url.rstrip("/") + request.url.path
            if request.url.query:
                target_url += "?" + request.url.query
            attempt_timeout = per_try
            if deadline is not None:
                attempt_timeout = max(0.05, min(per_try, deadline - time.monotonic()))
            log_route(
                self.logger,
                route.id,
                upstream.name,
                f"strategy={route.strategy.value} streaming={streaming} "
                f"attempt={attempt}/{max_attempts} probe={probe} target={target_url}",
            )

            if streaming:
                # 流式请求不重试：单次透传，按原有失败语义返回
                return await self._stream(
                    route, upstream, request, target_url, client_ip, body
                )

            result = await self._request_once(
                request, route, upstream, client_ip, target_url, body, attempt_timeout
            )
            if isinstance(result, _AttemptFailure):
                was_probe = self.selector.report_failure(route, upstream)
                excluded.add(upstream.name)
                failures.append(result)
                decision = "retry" if attempt < max_attempts else "stop"
                self.logger.info(
                    "retry route=%s upstream=%s attempt=%d decision=%s "
                    "reason=%s was_probe=%s",
                    route.id, upstream.name, attempt, decision, result.kind, was_probe,
                )
                continue

            # 拿到上游响应
            if (
                result.status_code >= 500
                and retry_on_5xx
                and attempt < max_attempts
            ):
                self.selector.report_5xx(route, upstream)
                excluded.add(upstream.name)
                failures.append(
                    _AttemptFailure("status", upstream.name, f"status={result.status_code}", result.status_code)
                )
                last_status_resp = result
                self.logger.info(
                    "retry route=%s upstream=%s attempt=%d decision=retry reason=status_%d",
                    route.id, upstream.name, attempt, result.status_code,
                )
                continue

            if result.status_code < 500:
                if self.selector.report_success(route, upstream):
                    self.logger.info(
                        "recovery route=%s upstream=%s decision=half_open_to_up",
                        route.id, upstream.name,
                    )
            else:
                self.selector.report_5xx(route, upstream)
            response = self._finalize(request, route, result, cacheable)
            if len(tried) > 1:
                response.headers["x-gateway-tried"] = ",".join(tried)
            return response

        return self._retry_exhausted(route, tried, failures, last_status_resp, cacheable, request)

    async def _request_once(
        self, request, route, upstream, client_ip, target_url, body, timeout
    ):
        """单次干净等价的尝试：每次重建请求头，复用同一份请求体字节。

        返回 httpx.Response；失败返回 _AttemptFailure。连接计数在此释放。
        """
        forward_headers = [
            (k, v)
            for k, v in request.headers.raw
            if k.lower() not in HOP_BY_HOP and k.lower() != "host"
        ]
        forward_headers.append((b"x-forwarded-for", client_ip.encode()))
        try:
            resp = await self.client.request(
                request.method,
                target_url,
                headers=forward_headers,
                content=body if body else None,
                timeout=timeout,
            )
            return resp
        except httpx.TimeoutException as exc:
            return _AttemptFailure("timeout", upstream.name, str(exc))
        except httpx.TransportError as exc:
            return _AttemptFailure("transport", upstream.name, str(exc))
        finally:
            self.selector.release(route.id, upstream)

    def _finalize(self, request, route, upstream_resp: httpx.Response, cacheable: bool) -> Response:
        """把最终上游响应转换为对客户端的响应，并按需写缓存。"""
        resp_headers = filter_response_headers(upstream_resp.headers.raw)
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

        headers = {k.decode("latin1"): v.decode("latin1") for k, v in resp_headers}
        return Response(
            status_code=upstream_resp.status_code,
            content=upstream_resp.content,
            headers=headers,
        )

    def _retry_exhausted(
        self, route, tried, failures, last_status_resp, cacheable, request
    ) -> Response:
        """重试全部失败：沿用最后一跳的错误类型，附加实际尝试信息。"""
        last = failures[-1]
        extra = {
            "attempts": len(tried),
            "tried_upstreams": list(tried),
            "route": route.id,
        }
        self.logger.info(
            "retry route=%s decision=exhausted attempts=%d tried=%s last=%s",
            route.id, len(tried), ",".join(tried), last.kind,
        )
        if last.kind == "status" and last_status_resp is not None:
            # 最后一跳是上游 5xx：原样透传该响应，附加 tried 头
            response = self._finalize(request, route, last_status_resp, cacheable)
            response.headers["x-gateway-tried"] = ",".join(tried)
            return response
        if last.kind == "timeout":
            return error_response(
                504, "upstream_timeout", last.message, upstream=last.upstream, **extra
            )
        return error_response(
            502, "upstream_error", last.message, upstream=last.upstream, **extra
        )

    async def _stream(self, route, upstream, request, target_url, client_ip, body):
        forward_headers = [
            (k, v)
            for k, v in request.headers.raw
            if k.lower() not in HOP_BY_HOP and k.lower() != "host"
        ]
        forward_headers.append((b"x-forwarded-for", client_ip.encode()))
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
            self.selector.report_failure(route, upstream)
            self.selector.release(route.id, upstream)
            return error_response(504, "upstream_timeout", str(exc), upstream=upstream.name)
        except httpx.TransportError as exc:
            self.selector.report_failure(route, upstream)
            self.selector.release(route.id, upstream)
            return error_response(502, "upstream_error", str(exc), upstream=upstream.name)

        if upstream_resp.status_code < 500:
            if self.selector.report_success(route, upstream):
                self.logger.info(
                    "recovery route=%s upstream=%s decision=half_open_to_up",
                    route.id, upstream.name,
                )
        else:
            self.selector.report_5xx(route, upstream)

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
