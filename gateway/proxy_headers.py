"""请求头处理与客户端 IP 提取工具。"""
from __future__ import annotations

from urllib.parse import unquote

HOP_BY_HOP_REQUEST = {
    "connection", "keep-alive", "proxy-connection", "te", "trailer",
    "transfer-encoding", "upgrade", "host", "content-length",
}

# 条件头由缓存层按需重新构造，不透传原始值（避免与网关缓存语义冲突）
CONDITIONAL = {"if-none-match", "if-modified-since", "if-match",
               "if-unmodified-since", "if-range"}


def get_header(headers, name: str) -> str | None:
    return headers.get(name)


def filtered_request_headers(headers) -> list[tuple[str, str]]:
    """headers 可以是 Starlette Headers，也可以是已构造的 (k,v) 列表。"""
    items = headers.items() if hasattr(headers, "items") else list(headers)
    out: list[tuple[str, str]] = []
    for k, v in items:
        kl = k.lower()
        if kl in HOP_BY_HOP_REQUEST or kl in CONDITIONAL:
            continue
        out.append((k, v))
    return out


def client_ip(request) -> str:
    # 本地验证用：X-Forwarded-For 优先，便于在白/黑名单测试中模拟来源
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return unquote(xff.split(",")[0].strip())
    if request.client is not None:
        return request.client.host
    return "unknown"
