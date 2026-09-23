"""令牌桶限流（按全局 / 客户端 / 路由维度）。

并发安全：每个桶自带锁，取令牌与惰性补充在同一临界区内完成，
并发压力下不会多放行受限请求。
"""
from __future__ import annotations

import threading
import time

from .config import RateLimitRule


class TokenBucket:
    def __init__(self, capacity: int, rate: float, clock=time.monotonic) -> None:
        self.capacity = capacity
        self.rate = rate
        self._tokens = float(capacity)
        self._last = clock()
        self._clock = clock
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(float(self.capacity), self._tokens + elapsed * self.rate)
            self._last = now

    def take(self, tokens: float = 1.0) -> bool:
        with self._lock:
            self._refill()
            if self._tokens + 1e-9 >= tokens:
                self._tokens -= tokens
                return True
            return False

    def remaining(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens

    def retry_after(self, tokens: float = 1.0) -> float:
        """距离攒够 tokens 还需多少秒。"""
        with self._lock:
            self._refill()
            deficit = tokens - self._tokens
            if deficit <= 0:
                return 0.0
            return max(0.0, deficit / self.rate) if self.rate > 0 else float("inf")


class RateLimiter:
    """scope 可取 global / client:{ip} / route:{id} 等；按 (scope,key,规则) 惰性建桶。"""

    def __init__(self, clock=time.monotonic) -> None:
        self._buckets: dict[tuple[str, str], TokenBucket] = {}
        self._clock = clock
        self._lock = threading.Lock()

    def _bucket(self, scope: str, key: str, rule: RateLimitRule) -> TokenBucket:
        full = (scope, key)
        bucket = self._buckets.get(full)
        if bucket is None:
            with self._lock:
                bucket = self._buckets.get(full)
                if bucket is None:
                    bucket = TokenBucket(rule.capacity, rule.rate, self._clock)
                    self._buckets[full] = bucket
        return bucket

    def check(self, scope: str, key: str, rule: RateLimitRule | None, tokens: float = 1.0) -> bool:
        if rule is None:
            return True
        return self._bucket(scope, key, rule).take(tokens)

    def state(self, scope: str, key: str) -> tuple[float, int] | None:
        """返回 (当前可用令牌, 容量)，供日志与 Retry-After 使用。"""
        bucket = self._buckets.get((scope, key))
        if bucket is None:
            return None
        return bucket.remaining(), bucket.capacity

    def retry_after(self, scope: str, key: str, tokens: float = 1.0) -> float:
        bucket = self._buckets.get((scope, key))
        return 0.0 if bucket is None else bucket.retry_after(tokens)
