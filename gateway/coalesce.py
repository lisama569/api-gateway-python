"""同键请求合并回源（single-flight / request coalescing）。

缓存未命中（或刚好过期）时，对同一个键（路由+方法+路径+查询+影响结果的请求头）
并发的多个请求，只允许第一个（leader）真正回源，其余（waiter）等待并共享结果。

- leader 完成（无论成功还是失败响应）后，waiter 各自拿到同一份结果；
  失败结果同样共享，等待方能拿到可判断的失败，且航班随即结束，后续请求可重新发起。
- 等待有上限（wait_timeout）：超时返回 None，由调用方给出明确结果，不会一直挂。
- 仅作用于可缓存请求；不同键、不可缓存、流式请求互不共享。
"""
from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field


@dataclass
class SharedResult:
    """一次回源的可共享结果（成功响应或失败响应都算）。

    req_headers 是 leader 自己请求头的小写快照（名字 -> 值），
    供等待方按响应 Vary 字段校验“这份结果是否真与自己的请求头对得上”。
    """

    status: int
    headers: list[tuple[bytes, bytes]]
    body: bytes
    req_headers: dict[str, str] = field(default_factory=dict)


@dataclass
class _Flight:
    key: str
    event: asyncio.Event = field(default_factory=asyncio.Event)
    result: SharedResult | None = None
    done: bool = False


class SingleFlight:
    """键 -> 在途航班。线程安全登记；等待方用 asyncio 等待。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._flights: dict[str, _Flight] = {}

    def acquire(self, key: str) -> tuple[bool, _Flight]:
        """返回 (是否 leader, 航班)。leader 负责回源并 complete。"""
        with self._lock:
            flight = self._flights.get(key)
            if flight is not None:
                return False, flight
            flight = _Flight(key=key)
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
