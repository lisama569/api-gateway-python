"""并发限流测试：令牌桶正确性、并发压力不多放行、维度区分、Retry-After。"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from gateway.config import RateLimitRule
from gateway.logging_utils import log_rate_limit
from gateway.ratelimit import RateLimiter, TokenBucket


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0
        self.lock = threading.Lock()

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        with self.lock:
            self.t += dt


def test_bucket_capacity_and_refill():
    clock = FakeClock()
    bucket = TokenBucket(capacity=3, rate=2.0, clock=clock)
    assert [bucket.take() for _ in range(4)] == [True, True, True, False]
    clock.advance(1.0)  # 每秒补 2 个
    assert [bucket.take() for _ in range(3)] == [True, True, False]


def test_concurrent_pressure_never_over_admit():
    """200 线程同时抢 10 个令牌，成功数必须恰好为 10。"""
    clock = FakeClock()
    bucket = TokenBucket(capacity=10, rate=0.0, clock=clock)
    results: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        ok = bucket.take()
        with lock:
            results.append(ok)

    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(lambda _: worker(), range(200)))

    assert sum(results) == 10
    assert results.count(False) == 190


def test_limiter_dimensions_are_independent():
    clock = FakeClock()
    limiter = RateLimiter(clock=clock)
    rule = RateLimitRule(1, 1.0)
    assert limiter.check("client", "1.1.1.1", rule)
    assert not limiter.check("client", "1.1.1.1", rule)
    # 不同客户端独立桶
    assert limiter.check("client", "2.2.2.2", rule)
    # 不同 scope 独立桶
    assert limiter.check("route", "users", rule)


def test_retry_after_hint():
    clock = FakeClock()
    limiter = RateLimiter(clock=clock)
    rule = RateLimitRule(1, 4.0)
    limiter.check("client", "1.1.1.1", rule)
    assert not limiter.check("client", "1.1.1.1", rule)
    assert limiter.retry_after("client", "1.1.1.1") == pytest.approx(0.25, abs=1e-6)


def test_log_includes_scope_and_count(caplog):
    caplog.set_level(logging.INFO, logger="gateway.rl-test")
    logger = logging.getLogger("gateway.rl-test")
    log_rate_limit(logger, "client", "9.9.9.9", used=0.0, capacity=2, allowed=False)
    assert "scope=client" in caplog.text
    assert "key=9.9.9.9" in caplog.text
    assert "decision=deny" in caplog.text
    assert "capacity=2" in caplog.text
