"""网关错误类型与可区分的失败原因。"""
from __future__ import annotations

from fastapi.responses import JSONResponse


class GatewayError(Exception):
    status_code: int = 500
    code: str = "gateway_error"
    retryable: bool = False

    def __init__(self, message: str = "", *, retry_after: float | None = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.retry_after = retry_after


class RouteNotFoundError(GatewayError):
    status_code, code = 404, "route_not_found"


class IPBlacklistedError(GatewayError):
    status_code, code = 403, "ip_blacklisted"


class IPNotAllowedError(GatewayError):
    status_code, code = 403, "ip_not_allowed"


class RateLimitedError(GatewayError):
    status_code, code, retryable = 429, "rate_limited", True


class UpstreamUnavailableError(GatewayError):
    status_code, code, retryable = 502, "upstream_unavailable", True


class UpstreamTimeoutError(GatewayError):
    status_code, code, retryable = 504, "upstream_timeout", True


def error_response(exc: GatewayError) -> JSONResponse:
    """把网关错误转换为带可区分 code 的 JSON 响应。

    429 额外带 Retry-After；retryable 字段提示客户端是否可重试。
    """
    headers: dict[str, str] = {}
    if exc.retry_after is not None:
        headers["Retry-After"] = str(max(1, int(exc.retry_after)))
    body: dict[str, object] = {
        "error": exc.code,
        "message": exc.message,
        "retryable": exc.retryable,
    }
    return JSONResponse(status_code=exc.status_code, content=body, headers=headers)
