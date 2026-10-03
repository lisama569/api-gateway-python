"""回源合并（single-flight）：同一个缓存键的并发未命中只回源一次。

- 领导者（第一个到达的请求）真正执行回源；跟随者等待其结果。
- 领导者结束后（无论成败）航班即关闭并移除，后续请求可重新发起，
  不会永远挂在失败的航班上。
- 跟随者等待有上限（wait_timeout）；超时后自行回源（成为新的领导者），
  保证有明确结果而不是无限等待。
- 仅作用于"同一个键"（路由+方法+路径+查询+相关请求头）；不同键、
  不可缓存请求、流式请求互不共享。
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FlightResult:
    """一次合并回源的结局。"""

    is_leader: bool
    value: Any = None          # 领导者/共享成功时的结果对象
    error: BaseException | None = None  # 领导者失败时的异常（跟随者也能拿到）
    timed_out: bool = False    # 跟随者等待超时（应自行回源）
    shared: bool = False       # 跟随者且成功共享了领导者结果


@dataclass
class _Flight:
    event: asyncio.Event = field(default_factory=asyncio.Event)
    value: Any = None
    error: BaseException | None = None


class SingleFlight:
    """按键合并并发执行；领导者协程异常不会拖死跟随者。"""

    def __init__(self, clock=time.monotonic, logger=None) -> None:
        self._flights: dict[str, _Flight] = {}
        self._lock = asyncio.Lock()
        self._clock = clock
        self._logger = logger

    def _log(self, msg: str, *args) -> None:
        if self._logger is not None:
            self._logger.info(msg, *args)

    async def run(
        self,
        key: str,
        fn: Callable[[], Awaitable[Any]],
        wait_timeout: float,
    ) -> FlightResult:
        """以 key 合并执行 fn。

        领导者：执行 fn 并返回其结果/异常（异常以 FlightResult.error 携带，
        不向外抛，保证跟随者能拿到结局）。
        跟随者：等待领导者完成并共享结局；等待超时返回 timed_out=True。
        """
        async with self._lock:
            flight = self._flights.get(key)
            if flight is None:
                flight = _Flight()
                self._flights[key] = flight
                leader = True
            else:
                leader = False

        if leader:
            self._log("coalesce key=%s decision=leader", key)
            try:
                flight.value = await fn()
            except asyncio.CancelledError:
                raise  # 取消也走 finally 唤醒跟随者
            except Exception as exc:  # noqa: BLE001 领导者任何失败都要唤醒跟随者
                flight.error = exc
                self._log(
                    "coalesce key=%s decision=leader_failed reason=%r", key, exc
                )
            finally:
                async with self._lock:
                    self._flights.pop(key, None)
                    flight.event.set()
            return FlightResult(is_leader=True, value=flight.value, error=flight.error)

        self._log("coalesce key=%s decision=wait", key)
        try:
            await asyncio.wait_for(flight.event.wait(), timeout=wait_timeout)
        except TimeoutError:
            self._log(
                "coalesce key=%s decision=wait_timeout wait_timeout=%.2f",
                key,
                wait_timeout,
            )
            return FlightResult(is_leader=False, timed_out=True)
        if flight.error is None and flight.value is None:
            # 领导者被取消等无结局场景：跟随者拿到可判断失败而不是空结果
            flight.error = RuntimeError("leader finished without result")
        if flight.error is not None:
            self._log("coalesce key=%s decision=shared_error", key)
            return FlightResult(is_leader=False, error=flight.error)
        self._log("coalesce key=%s decision=shared", key)
        return FlightResult(is_leader=False, value=flight.value, shared=True)

    def in_flight(self, key: str) -> bool:
        return key in self._flights
