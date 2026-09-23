"""带 TTL 的响应缓存与条件请求（If-None-Match / If-Modified-Since）判定。

- 仅对可缓存响应（默认按路由配置的 cache_ttl）缓存；
- 命中且新鲜：直接返回缓存，不再访问上游；
- 命中但过期：带上 If-None-Match/If-Modified-Since 去上游校验，
  上游 304 时复用缓存体并续期。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from email.utils import formatdate, parsedate_to_datetime


@dataclass
class CachedResponse:
    status: int
    headers: list[tuple[bytes, bytes]]
    body: bytes
    etag: str = ""
    last_modified: str = ""
    stored_at: float = 0.0
    expires_at: float = 0.0


def make_cache_key(method: str, path: str, vary: dict[str, str]) -> str:
    suffix = "|".join(f"{k.lower()}={vary[k]}" for k in sorted(vary))
    return f"{method.upper()} {path}|{suffix}"


def now_http() -> str:
    return formatdate(time.time(), usegmt=True)


def _http_to_ts(value: str) -> float:
    return parsedate_to_datetime(value).timestamp()


class ResponseCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._store: dict[str, CachedResponse] = {}
        # key -> 命中次数（供日志/测试断言）
        self.hits: dict[str, int] = {}
        self.misses: dict[str, int] = {}

    def get(self, key: str) -> CachedResponse | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self.misses[key] = self.misses.get(key, 0) + 1
                print(f"[cache] key={key!r} MISS (no entry)")
                return None
            if time.time() >= entry.expires_at:
                age = time.time() - entry.stored_at
                print(f"[cache] key={key!r} STALE age={age:.2f}s "
                      f"will validate upstream")
                return None
            self.hits[key] = self.hits.get(key, 0) + 1
            print(f"[cache] key={key!r} HIT fresh ttl_left="
                  f"{entry.expires_at - time.time():.2f}s "
                  f"hit_count={self.hits[key]}")
            return entry

    def get_stale(self, key: str) -> CachedResponse | None:
        with self._lock:
            return self._store.get(key)

    def put(self, key: str, entry: CachedResponse) -> None:
        with self._lock:
            self._store[key] = entry
        print(f"[cache] STORE key={key!r} status={entry.status} "
              f"bytes={len(entry.body)} ttl="
              f"{entry.expires_at - entry.stored_at:.1f}s "
              f'etag={entry.etag or "-"}')

    def revalidate(self, key: str, ttl: float) -> CachedResponse | None:
        """304 校验成功后续期并返回新条目。"""
        with self._lock:
            old = self._store.get(key)
            if old is None:
                return None
            now = time.time()
            fresh = CachedResponse(
                status=old.status, headers=list(old.headers), body=old.body,
                etag=old.etag, last_modified=old.last_modified,
                stored_at=now, expires_at=now + ttl,
            )
            self._store[key] = fresh
        print(f"[cache] REVALIDATED key={key!r} ttl refreshed to {ttl}s")
        return fresh

    def not_modified(self, key: str, entry: CachedResponse,
                     client_etags: tuple[str, ...]) -> bool:
        """判断缓存是否满足客户端的条件请求。"""
        if client_etags:
            matched = "*" in client_etags or entry.etag in client_etags
            print(f"[cache] CONDITIONAL key={key!r} If-None-Match="
                  f"{client_etags} etag={entry.etag} -> "
                  f"{'304' if matched else '200'}")
            return matched
        return False

    @staticmethod
    def not_modified_since(entry: CachedResponse, ims: str) -> bool:
        try:
            ims_ts = _http_to_ts(ims)
        except (TypeError, ValueError):
            return False
        lm_ts = _http_to_ts(entry.last_modified) if entry.last_modified else None
        base = lm_ts if lm_ts is not None else entry.stored_at
        return base <= ims_ts + 1.0

    def clear(self) -> None:
        with self._lock:
            self._store.clear()
            self.hits.clear()
            self.misses.clear()
        print("[cache] cleared")

    def stats(self) -> dict[str, object]:
        with self._lock:
            return {"hits": dict(self.hits), "misses": dict(self.misses),
                    "entries": len(self._store)}
