"""带 TTL 的响应缓存，支持条件请求与新鲜度判定。

缓存键：route + method + path + query + Vary 声明的请求头。
缓存条件：仅 GET/HEAD、状态 200、且 Cache-Control 不含 no-store/private。
新鲜度：stored_at + ttl；过期条目视为未命中（不做 stale-while-revalidate）。
条件请求：命中新鲜缓存时用 If-None-Match / If-Modified-Since 本地校验，
通过则返回 304，不再访问上游。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from starlette.datastructures import Headers

CACHEABLE_METHODS = frozenset({"GET", "HEAD"})

# 冷启动（该资源还没缓存过任何变体、上游 Vary 未知）时，默认按标准内容协商头
# 预先区分合并键，避免不同变体在冷启动瞬间共享同一次回源。
# 这只是“保守预分流”，真正权威的依据仍是上游响应的 Vary（见 coalesce 的回包校验）；
# 自定义 Vary 头即便不在此列，也会在 leader 回包后被二次校验拦住。
COLD_START_VARY = ("accept", "accept-encoding", "accept-language")
HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)


@dataclass
class CachedResponse:
    status: int
    headers: list[tuple[bytes, bytes]]
    body: bytes
    stored_at: float
    ttl: int

    def is_fresh(self, now: float) -> bool:
        return now < self.stored_at + self.ttl

    def header(self, name: str) -> str | None:
        name = name.lower().encode()
        for k, v in self.headers:
            if k.lower() == name:
                return v.decode("latin-1")
        return None

    @property
    def etag(self) -> str | None:
        return self.header("etag")

    @property
    def last_modified(self) -> str | None:
        return self.header("last-modified")



class ResponseCache:
    def __init__(self, clock=time.time) -> None:
        self._store: dict[str, CachedResponse] = {}
        # 基键 -> 该资源已缓存的变体：[(vary 字段, 完整键), ...]
        self._groups: dict[str, list[tuple[frozenset[str], str]]] = {}
        self._lock = threading.Lock()
        self._clock = clock
        self.hits = 0
        self.misses = 0
        self.validated = 0

    @staticmethod
    def base_key(route_id: str, method: str, path: str, query: str) -> str:
        return "|".join([route_id, method, path, query or ""])

    @staticmethod
    def make_key(
        route_id: str,
        method: str,
        path: str,
        query: str,
        request_headers: Headers,
        vary: list[str] | None = None,
    ) -> str:
        base = ResponseCache.base_key(route_id, method, path, query)
        suffix = ResponseCache.variant_suffix(request_headers, vary)
        return f"{base}|{suffix}" if suffix else base

    @staticmethod
    def variant_suffix(request_headers: Headers, vary: list[str] | None) -> str:
        return "|".join(
            f"{name.lower()}={request_headers.get(name, '')}" for name in vary or []
        )

    @staticmethod
    def vary_fields(cached: CachedResponse) -> list[str]:
        raw = cached.header("vary")
        if not raw or raw.strip() == "*":
            return []
        return [v.strip() for v in raw.split(",") if v.strip()]

    @staticmethod
    def is_vary_star(cached: CachedResponse) -> bool:
        raw = cached.header("vary")
        return bool(raw) and raw.strip() == "*"

    # ---- 变体感知 API（代理使用）----
    def coalesce_key(
        self,
        route_id: str,
        method: str,
        path: str,
        query: str,
        request_headers: Headers,
    ) -> str:
        """合并回源用的键：基键 + 影响结果的请求头值。

        - 该资源已缓存过变体：用已知 Vary 字段的并集（精确）。
        - 冷启动（尚无任何变体，上游 Vary 未知）：用 COLD_START_VARY 里的标准
          内容协商头做保守预分流，先把最常见的不同变体拆开，避免冷启动瞬间
          把带/不带压缩等请求合进同一次回源。
        只影响“哪些并发请求共享一次回源”，不改变缓存键与 TTL 行为。
        """
        base = self.base_key(route_id, method, path, query)
        with self._lock:
            variants = self._groups.get(base, [])
            if variants:
                vary: list[str] = sorted(
                    {name for vary_set, _ in variants for name in vary_set}
                )
            else:
                vary = list(COLD_START_VARY)
        suffix = self.variant_suffix(request_headers, vary)
        return f"{base}|{suffix}" if suffix else base

    def lookup(
        self,
        route_id: str,
        method: str,
        path: str,
        query: str,
        request_headers: Headers,
        now: float | None = None,
    ) -> tuple[str | None, CachedResponse | None]:
        """按请求的 Vary 头值在资源的变体集合中找新鲜条目。

        返回 (完整键, 缓存条目)；未命中返回 (None, None)。
        """
        now = self._clock() if now is None else now
        base = self.base_key(route_id, method, path, query)
        with self._lock:
            variants = self._groups.get(base, [])
            alive: list[tuple[frozenset[str], str]] = []
            for vary_set, full_key in variants:
                entry = self._store.get(full_key)
                if entry is not None and entry.is_fresh(now):
                    alive.append((vary_set, full_key))
                    expected = self.make_key(
                        route_id, method, path, query, request_headers,
                        sorted(vary_set),
                    )
                    if expected == full_key:
                        return full_key, entry
                else:
                    self._store.pop(full_key, None)
            if len(alive) != len(variants):
                if alive:
                    self._groups[base] = alive
                else:
                    self._groups.pop(base, None)
            return None, None

    def store(
        self,
        route_id: str,
        method: str,
        path: str,
        query: str,
        request_headers: Headers,
        value: CachedResponse,
    ) -> str:
        vary = self.vary_fields(value)
        full_key = self.make_key(
            route_id, method, path, query, request_headers, vary
        )
        base = self.base_key(route_id, method, path, query)
        with self._lock:
            self._store[full_key] = value
            variants = self._groups.setdefault(base, [])
            if not any(k == full_key for _, k in variants):
                variants.append((frozenset(vary), full_key))
        return full_key

    # ---- 底层键值 API（单测与清理使用）----
    def get(self, key: str, now: float | None = None) -> CachedResponse | None:
        now = self._clock() if now is None else now
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if not entry.is_fresh(now):
                del self._store[key]
                return None
            return entry

    def put(self, key: str, value: CachedResponse) -> None:
        with self._lock:
            self._store[key] = value

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()
            self._groups.clear()
