"""按客户端+路由维度的限流，以及来源白/黑名单。

限流器使用单锁 + 固定窗口计数，保证并发压力下绝不多放行：
同一窗口内第 N+1 个请求被拒，状态 429 并带 Retry-After。
"""
from __future__ import annotations

import ipaddress
import threading
import time

from gateway.config import RateLimitConfig
from gateway.errors import (
    IPBlacklistedError,
    IPNotAllowedError,
    RateLimitedError,
)


class RateLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # key=(ip, route, limit, window) -> (window_start, count)
        self._buckets: dict[tuple[str, str, int, float], tuple[float, int]] = {}

    def check(self, client_ip: str, route_id: str,
              cfg: RateLimitConfig | None) -> None:
        if cfg is None or cfg.requests <= 0:
            return
        now = time.monotonic()
        key = (client_ip, route_id, cfg.requests, cfg.window_seconds)
        with self._lock:
            start, count = self._buckets.get(key, (now, 0))
            if now - start >= cfg.window_seconds:
                start, count = now, 0
            count += 1
            self._buckets[key] = (start, count)
            allowed = count <= cfg.requests
        retry_after = max(0.0, cfg.window_seconds - (now - start))
        print(f"[ratelimit] ip={client_ip} route={route_id} count={count}/"
              f"{cfg.requests} window={cfg.window_seconds}s "
              f"decision={'ALLOW' if allowed else 'REJECT'}")
        if not allowed:
            raise RateLimitedError(
                f"rate limit {cfg.requests}/{cfg.window_seconds}s exceeded "
                f"for {client_ip} on {route_id}",
                retry_after=retry_after,
            )

    def reset(self) -> None:
        with self._lock:
            self._buckets.clear()
        print("[ratelimit] all counters reset")


class ACL:
    """白名单（非空时仅放行名单内来源）与黑名单优先拒绝，支持 CIDR。"""

    def __init__(self, whitelist: tuple[str, ...] = (),
                 blacklist: tuple[str, ...] = ()) -> None:
        self._lock = threading.Lock()
        self.whitelist = whitelist
        self.blacklist = blacklist

    def update(self, whitelist: tuple[str, ...],
               blacklist: tuple[str, ...]) -> None:
        with self._lock:
            self.whitelist = tuple(whitelist)
            self.blacklist = tuple(blacklist)
        print(f"[acl] updated whitelist={self.whitelist} "
              f"blacklist={self.blacklist}")

    @staticmethod
    def _match(client_ip: str, entries: tuple[str, ...]) -> bool:
        try:
            ip = ipaddress.ip_address(client_ip)
        except ValueError:
            return client_ip in entries
        for entry in entries:
            try:
                if "/" in entry:
                    if ip in ipaddress.ip_network(entry, strict=False):
                        return True
                elif ip == ipaddress.ip_address(entry):
                    return True
            except ValueError:
                if entry == client_ip:
                    return True
        return False

    def check(self, client_ip: str) -> None:
        with self._lock:
            whitelist, blacklist = self.whitelist, self.blacklist
        if self._match(client_ip, blacklist):
            print(f"[acl] ip={client_ip} REJECT blacklisted")
            raise IPBlacklistedError(f"{client_ip} is blacklisted")
        if whitelist and not self._match(client_ip, whitelist):
            print(f"[acl] ip={client_ip} REJECT not in whitelist")
            raise IPNotAllowedError(f"{client_ip} is not in whitelist")
        print(f"[acl] ip={client_ip} ALLOW")
