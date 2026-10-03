"""反向代理核心：鉴权 -> 限流 -> 缓存 -> 回源合并 -> 重试/故障转移 -> 转发。

失败判定（HTTP 状态码与 JSON error 码均可区分）：
- 404 no_route          无匹配路由
- 403 ip_blocked        命中黑名单
- 403 ip_not_allowed    白名单非空且来源不在其中
- 429 rate_limited      限流（含 Retry-After 与 scope）
- 503 no_upstream       路由存在但全部实例不健康且没有空闲探测槽
- 504 upstream_timeout  上游超时（重试耗尽后带 attempts 明细）
- 502 upstream_error    上游连接/协议错误（重试耗尽后带 attempts 明细）

重试与故障转移（route.retry 配置后启用）：
- 仅安全方法（GET/HEAD/OPTIONS）且非流式路由参与重试；写操作与流式请求
  不重放，按原有单次失败结果返回。
- 每次尝试重新 acquire 实例（排除本请求已试过的），重建干净等价的请求；
  单次耗时受 per_attempt_timeout（缺省 route.timeout）限制，整体受 budget 限制。
- 全部失败后，错误体带 attempts（试过哪些实例、各自结局），仍与
  no_route / rate_limited / no_upstream 可区分。

回源合并（single-flight）：
- 可缓存请求缓存未命中时，同一个键（路由+方法+路径+查询+相关请求头）
  只放行一个请求回源，其余等待并共享结果；等待上限为 route.coalesce_wait，
  超时后自行回源。不可缓存与流式请求不参与合并。

流式路由（route.streaming=True）与不可缓存方法一律透传：不读缓存、不写缓存，
响应体以 chunked 方式原样回传。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from .access import AccessController
from .cache import CACHEABLE_METHODS, HOP_BY_HOP, CachedResponse, ResponseCache
from .coalesce import SingleFlight
from .logging_utils import log_cache, log_rate_limit, log_route
from .ratelimit import RateLimiter
from .router import Router
from .upstream import NoHealthyUpstreamError, UpstreamSelector

# 可安全重放的方法：写操作（POST/PUT/PATCH/DELETE）一律不重试
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


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
class FetchOutcome:
    """一次（可能含多次尝试的）回源结局；非流式请求的统一中间表示。"""

    status: int
    headers: list[tuple[bytes, bytes]] = field(default_factory=list)
    body: bytes = b""
    error: str | None = None          # 网关错误码（upstream_timeout 等）
    message: str = ""
    upstream: str | None = None       # 最后一跳命中的实例
    extra: dict = field(default_factory=dict)  # 并入错误 JSON 的额外字段
    attempts: list[dict] | None = None         # 重试明细（仅重试后失败/成功附）
    vary_fields: tuple[str, ...] = ()
    vary_suffix: str = ""             # 领导者请求在 Vary 字段下的取值


class Gateway:
    def __init__(
        self,
        router: Router,
        selector: UpstreamSelector,
        limiter: RateLimiter,
        cache: ResponseCache,
        client: httpx.AsyncClient,
        logger=None,
        flights: SingleFlight | None = None,
    ) -> None:
        from .logging_utils import get_logger

        self.router = router
        self.selector = selector
        self.limiter = limiter
        self.cache = cache
        self.client = client
        self.logger = logger or get_logger("gateway.proxy")
        self.flights = flights or SingleFlight()

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
        body = await request.body()

        if cacheable:
            served = self._try_cache(request, route)
            if served is not None:
                return served
            return await self._coalesced(request, route, client_ip, body)

        result = await self._fetch(request, route, client_ip, streaming, False, body)
        if isinstance(result, Response):  # 流式透传
            return result
        return self._outcome_to_response(result)

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

    # ---- 回源合并 -------------------------------------------------------
    async def _coalesced(self, request: Request, route, client_ip: str, body: bytes) -> Response:
        known_vary = self.cache.known_vary_fields(
            route.id, request.method, request.url.path, request.url.query
        )
        flight_key = ResponseCache.make_key(
            route.id,
            request.method,
            request.url.path,
            request.url.query,
            request.headers,
            sorted(known_vary),
        )

        async def leader_fn() -> FetchOutcome:
            outcome = await self._fetch(request, route, client_ip, False, True, body)
            assert isinstance(outcome, FetchOutcome)
            return outcome

        result = await self.flights.run(flight_key, leader_fn, route.coalesce_wait)

        if result.is_leader:
            if result.error is not None:
                return error_response(502, "upstream_error", f"fetch failed: {result.error!r}")
            return self._outcome_to_response(result.value)

        if result.timed_out:
            # 等待超时：自行回源，保证有明确结果
            outcome = await self._fetch(request, route, client_ip, False, True, body)
            assert isinstance(outcome, FetchOutcome)
            return self._outcome_to_response(outcome)

        if result.error is not None:
            # 领导者代码级异常：跟随者各自拿到可判断的失败
            return error_response(502, "upstream_error", f"shared fetch failed: {result.error!r}")

        outcome: FetchOutcome = result.value

        # 1) 领导者已写缓存：按自己的请求头重新查（变体各自命中，含 304）
        if outcome.error is None and outcome.status == 200:
            served = self._try_cache(request, route)
            if served is not None:
                served.headers["x-coalesced"] = "shared"
                log_cache(self.logger, flight_key, "SHARED", "served from leader's store")
                return served

        # 2) 领导者失败：跟随者各自构造同样的可区分失败结果
        if outcome.error is not None:
            log_cache(self.logger, flight_key, "SHARED_ERROR", outcome.error)
            return self._outcome_to_response(outcome)

        # 3) 变体匹配（含无 Vary）：共享领导者这一份结果
        if self._vary_matches(request, outcome):
            log_cache(self.logger, flight_key, "SHARED", f"status={outcome.status}")
            headers = {
                k.decode("latin1"): v.decode("latin1") for k, v in outcome.headers
            }
            headers["x-coalesced"] = "shared"
            return Response(
                status_code=outcome.status, content=outcome.body, headers=headers
            )

        # 4) 变体不匹配：本请求的变体需自行回源
        log_cache(self.logger, flight_key, "VARY_MISMATCH", "fetch own variant")
        own = await self._fetch(request, route, client_ip, False, True, body)
        assert isinstance(own, FetchOutcome)
        return self._outcome_to_response(own)

    @staticmethod
    def _vary_matches(request: Request, outcome: FetchOutcome) -> bool:
        if not outcome.vary_fields:
            return True
        suffix = ResponseCache.variant_suffix(
            request.headers, list(outcome.vary_fields)
        )
        return suffix == outcome.vary_suffix

    # ---- 转发（重试循环）------------------------------------------------
    async def _fetch(
        self,
        request: Request,
        route,
        client_ip: str,
        streaming: bool,
        cacheable: bool,
        body: bytes,
    ):
        retry_cfg = route.retry
        retryable = (
            retry_cfg is not None
            and not streaming
            and request.method in IDEMPOTENT_METHODS
            and retry_cfg.max_attempts > 1
        )
        max_attempts = retry_cfg.max_attempts if retryable else 1
        deadline = time.monotonic() + retry_cfg.budget if retryable else None
        attempts: list[dict] = []
        tried: set[str] = set()
        last: FetchOutcome | None = None

        for attempt_no in range(1, max_attempts + 1):
            if last is not None and deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.logger.info(
                        "retry route=%s decision=stop reason=budget_exhausted", route.id
                    )
                    break
            try:
                upstream = self.selector.acquire(route, exclude=frozenset(tried))
            except NoHealthyUpstreamError:
                if last is None:
                    log_route(self.logger, route.id, None, "all upstreams down")
                    return FetchOutcome(
                        status=503,
                        error="no_upstream",
                        message="no healthy upstream",
                        extra={"route": route.id},
                    )
                self.logger.info(
                    "retry route=%s decision=stop reason=no_more_healthy tried=%s",
                    route.id,
                    sorted(tried),
                )
                break

            timeout = route.timeout
            if retryable:
                per = retry_cfg.per_attempt_timeout or route.timeout
                remaining = deadline - time.monotonic()
                timeout = max(0.01, min(per, remaining))

            outcome = await self._attempt(
                request, route, upstream, client_ip, streaming, cacheable,
                body, timeout, attempt_no,
            )
            if isinstance(outcome, Response):
                return outcome  # 流式：直接透传，不重试
            tried.add(upstream.name)
            attempts.append(self._attempt_entry(upstream.name, outcome))

            if not retryable or not self._retryable_outcome(outcome, retry_cfg):
                if retryable and len(attempts) > 1:
                    outcome.attempts = attempts
                return outcome

            self.logger.info(
                "retry route=%s decision=retry attempt=%d/%d upstream=%s reason=%s",
                route.id,
                attempt_no,
                max_attempts,
                upstream.name,
                attempts[-1]["outcome"],
            )
            last = outcome

        # 重试耗尽（次数/预算/无更多实例）：返回最后一次失败并附明细
        last.attempts = attempts
        return last

    @staticmethod
    def _attempt_entry(upstream_name: str, outcome: FetchOutcome) -> dict:
        if outcome.error == "upstream_timeout":
            desc = "timeout"
        elif outcome.error == "upstream_error":
            desc = "transport_error"
        elif outcome.error:
            desc = outcome.error
        else:
            desc = f"status_{outcome.status}"
        return {"upstream": upstream_name, "outcome": desc}

    @staticmethod
    def _retryable_outcome(outcome: FetchOutcome, retry_cfg) -> bool:
        if outcome.error in ("upstream_timeout", "upstream_error"):
            return True
        return outcome.error is None and outcome.status in retry_cfg.retry_on_statuses

    # ---- 单次尝试 -------------------------------------------------------
    async def _attempt(
        self,
        request: Request,
        route,
        upstream,
        client_ip: str,
        streaming: bool,
        cacheable: bool,
        body: bytes,
        timeout: float,
        attempt_no: int,
    ):
        target_url = upstream.base_url.rstrip("/") + request.url.path
        if request.url.query:
            target_url += "?" + request.url.query
        log_route(
            self.logger,
            route.id,
            upstream.name,
            f"strategy={route.strategy.value} streaming={streaming} "
            f"attempt={attempt_no} target={target_url}",
        )

        # 每次尝试都重新构造干净等价的请求：同样的头与体、新的目标实例
        forward_headers = [
            (k, v)
            for k, v in request.headers.raw
            if k.lower() not in HOP_BY_HOP and k.lower() != "host"
        ]
        forward_headers.append((b"x-forwarded-for", client_ip.encode()))
        is_probe = self.selector.is_probe(route.id, upstream.name)

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
                timeout=timeout,
            )
        except httpx.TimeoutException as exc:
            self.selector.report_failure(route, upstream)
            self.selector.release(route.id, upstream)
            return FetchOutcome(
                status=504,
                error="upstream_timeout",
                message=str(exc),
                upstream=upstream.name,
            )
        except httpx.TransportError as exc:
            self.selector.report_failure(route, upstream)
            self.selector.release(route.id, upstream)
            return FetchOutcome(
                status=502,
                error="upstream_error",
                message=str(exc),
                upstream=upstream.name,
            )

        status = upstream_resp.status_code
        retry_cfg = route.retry
        if status < 500:
            self.selector.report_success(route, upstream)
        elif is_probe or (
            retry_cfg is not None and status in retry_cfg.retry_on_statuses
        ):
            # 探测失败 / 可重试的 5xx：熔断该实例（探测则重新冷却）
            self.selector.report_failure(route, upstream)
        else:
            self.selector.report_success(route, upstream)  # 非探测：维持原语义
        self.selector.release(route.id, upstream)

        resp_headers = filter_response_headers(upstream_resp.headers.raw)

        # 缓存写入：仅 200 且上游未声明 no-store/private，且 Vary 不含 *
        if (
            cacheable
            and status == 200
            and not cache_control_no_store(upstream_resp.headers)
        ):
            stored = CachedResponse(
                status=status,
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

        vary_fields = tuple(ResponseCache.vary_fields(stored_like(upstream_resp)))
        return FetchOutcome(
            status=status,
            headers=resp_headers,
            body=upstream_resp.content,
            upstream=upstream.name,
            vary_fields=vary_fields,
            vary_suffix=ResponseCache.variant_suffix(request.headers, list(vary_fields)),
        )

    def _outcome_to_response(self, outcome: FetchOutcome) -> Response:
        if outcome.error is not None:
            extra = dict(outcome.extra)
            if outcome.upstream:
                extra["upstream"] = outcome.upstream
            if outcome.attempts:
                extra["attempts"] = outcome.attempts
            return error_response(outcome.status, outcome.error, outcome.message, **extra)
        headers = {
            k.decode("latin1"): v.decode("latin1") for k, v in outcome.headers
        }
        if outcome.attempts:
            headers["x-gateway-attempts"] = ",".join(
                f"{a['upstream']}:{a['outcome']}" for a in outcome.attempts
            )
        return Response(
            status_code=outcome.status, content=outcome.body, headers=headers
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
            self.selector.report_failure(route, upstream)
            self.selector.release(route.id, upstream)
            return error_response(504, "upstream_timeout", str(exc), upstream=upstream.name)
        except httpx.TransportError as exc:
            self.selector.report_failure(route, upstream)
            self.selector.release(route.id, upstream)
            return error_response(502, "upstream_error", str(exc), upstream=upstream.name)

        # 响应头已拿到：实例存活，探测视为成功
        self.selector.report_success(route, upstream)

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


def stored_like(resp) -> CachedResponse:
    """仅为读取 Vary 头而构造的轻量视图。"""
    return CachedResponse(
        status=resp.status_code,
        headers=list(resp.headers.raw),
        body=b"",
        stored_at=0.0,
        ttl=0,
    )
