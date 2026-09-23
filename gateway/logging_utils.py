"""网关诊断日志：路由选择、限流计数、缓存判定。

单测通过 ``caplog`` 或直接观察 stderr 即可看到：
- route   : 路由匹配与上游选择依据
- rl      : 限流维度、已用/容量、判定
- cache   : 缓存键、命中/未命中/校验、新鲜度
"""
from __future__ import annotations

import logging
import sys

_CONFIGURED = False


def get_logger(name: str) -> logging.Logger:
    """获取带统一样式的 logger，只配置一次，避免重复 handler。"""
    global _CONFIGURED
    logger = logging.getLogger(name)
    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        )
        root = logging.getLogger("gateway")
        if not root.handlers:
            root.addHandler(handler)
            root.setLevel(logging.INFO)
            root.propagate = False
        _CONFIGURED = True
    return logger


def log_route(logger: logging.Logger, route_id: str, upstream: str | None, reason: str) -> None:
    logger.info("route route=%s upstream=%s reason=%s", route_id, upstream or "-", reason)


def log_rate_limit(
    logger: logging.Logger,
    scope: str,
    key: str,
    used: float,
    capacity: int,
    allowed: bool,
) -> None:
    decision = "allow" if allowed else "deny"
    logger.info(
        "rl scope=%s key=%s decision=%s remaining=%.2f capacity=%d",
        scope,
        key,
        decision,
        used,
        capacity,
    )


def log_cache(logger: logging.Logger, key: str, decision: str, reason: str) -> None:
    logger.info("cache key=%s decision=%s reason=%s", key, decision, reason)
