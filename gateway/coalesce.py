"""同键请求合并回源（single-flight / request coalescing）。

缓存未命中（或刚好过期）时，对同一个键（路由+方法+路径+查询+影响结果的请求头）
并发的多个请求，只允许第一个（leader）真正回源，其余（waiter）等待并共享结果。

- leader 完成（无论成功还是失败响应）后，还要拿上游响应的 Vary 做一次校验：
  只有 waiter 与 leader 在所有 Vary 字段上的请求头值一致时才共享；不一致
  （或 Vary: *）的 waiter 不允许套用别人的结果，自己重新回源。
  这是防止“冷启动时尚不知道上游按哪些头区分变体”的权威防线。
- 等待有上限（wait_timeout）：超时返回 None，由调用方给出明确结果，不会一直挂。
- 仅作用于可缓存请求；不同键、不可缓存、流式请求互不共享。
"""
from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field


@dataclass
class SharedResult:
    """一次回源的可共享结果（成功响应或失败响应都算）。"""

    status: int
    headers: list[tuple[bytes, bytes]]
    body: bytes


@dataclass
class _Flight:
    key: str
    event: asyncio.Event = field(default_factory=asyncio.Event)
    result: SharedResult | None = None
    done: bool = False
    # leader 发起回源时的请求头快照（小写键），用于回包后按 Vary 校验 waiter
    leader_headers: dict[str, str] | None = None


def vary_fields_of(headers: list[tuple[bytes, bytes]]) -> list[str]:
    """从原始响应头中解析 Vary 声明的字段；Vary: * 用 ["*"] 表示。"""
    raw = None
    for k, v in headers:
        if k.lower() == b"vary":
            raw = v.decode("latin-1")
            break
    if raw is None:
        return []
    raw = raw.strip()
    if raw == "*":
        return ["*"]
    return [part.strip().lower() for part in raw.split(",") if part.strip()]


def sharing_allowed(
    leader_headers: dict[str, str] | None,
    waiter_headers: dict[str, str],
    result: SharedResult,
) -> tuple[bool, str]:
    """按 leader 响应的 Vary 判定 waiter 能否套用该结果。

    返回 (是否可共享, 原因)。Vary 未声明时，能在同一航班上即视为可共享；
    Vary: * 一律不共享；任一 Vary 字段的请求头值与 leader 不同则不共享。
    """
    fields = vary_fields_of(result.headers)
    if fields == ["*"]:
        return False, "vary_star"
    if leader_headers is None:
        return False, "no_leader_headers"
    for name in fields:
        if waiter_headers.get(name, "") != leader_headers.get(name, ""):
            return False, f"vary_mismatch:{name}"
    return True, "match"


class SingleFlight:
    """键 -> 在途航班。线程安全登记；等待方用 asyncio 等待。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._flights: dict[str, _Flight] = {}

    def acquire(
        self, key: str, leader_headers: dict[str, str] | None = None
    ) -> tuple[bool, _Flight]:
        """返回 (是否 leader, 航班)。leader 负责回源并 complete。"""
        with self._lock:
            flight = self._flights.get(key)
            if flight is not None:
                return False, flight
            flight = _Flight(key=key, leader_headers=leader_headers)
            self._flights[key] = flight
            return True, flight

    def complete(self, flight: _Flight, result: SharedResult) -> None:
        """leader 完成回源：记录结果、唤醒全部 waiter、结束航班。"""
        with self._lock:
            flight.result = result
            flight.done = True
            self._flights.pop(flight.key, None)
            flight.event.set()

    def abort(self, flight: _Flight) -> None:
        """leader 异常退出：结束航班但不给结果，waiter 需自行重新发起。"""
        with self._lock:
            flight.done = True
            self._flights.pop(flight.key, None)
            flight.event.set()

    async def wait(self, flight: _Flight, timeout: float) -> SharedResult | None | str:
        """等待航班结果。

        返回 SharedResult；超时返回 None；leader 异常（无结果）返回 "aborted"。
        """
        try:
            await asyncio.wait_for(flight.event.wait(), timeout)
        except (asyncio.TimeoutError, TimeoutError):
            return None
        if flight.result is None:
            return "aborted"
        return flight.result
