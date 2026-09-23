"""测试夹具：真实启动 3 个假上游 uvicorn 实例 + 内存网关。"""
from __future__ import annotations

import socket
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from fake_upstream import HIT_COUNTS, create_fake_app
from gateway import proxy
from gateway.config import (
    GatewayConfig,
    RateLimitConfig,
    RouteConfig,
    UpstreamConfig,
)
from gateway.state import GatewayState
from main import app as fastapi_app


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _UpstreamServer:
    def __init__(self, instance_id: str, port: int) -> None:
        self.config = uvicorn.Config(
            create_fake_app(instance_id), host="127.0.0.1", port=port,
            log_level="error", lifespan="off",
        )
        self.server = uvicorn.Server(self.config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.port = port

    def start(self) -> None:
        self.thread.start()
        deadline = time.time() + 5
        while not self.server.started and time.time() < deadline:
            time.sleep(0.02)

    def stop(self) -> None:
        self.server.should_exit = True


@pytest.fixture(scope="session")
def upstreams():
    ports = [_free_port() for _ in range(3)]
    servers = [_UpstreamServer(f"u{i}", p) for i, p in enumerate(ports)]
    for s in servers:
        s.start()
    yield {f"u{i}": f"http://127.0.0.1:{p}" for i, p in enumerate(ports)}
    for s in servers:
        s.stop()


def make_config(upstreams: dict[str, str], *, timeout: float = 0.8,
                whitelist=(), blacklist=()) -> GatewayConfig:
    ups = tuple(UpstreamConfig(id=k, base_url=v) for k, v in upstreams.items())
    ids = tuple(upstreams.keys())
    routes = (
        RouteConfig(id="rr", path_prefix="/gateway/rr",
                    upstream_ids=ids, strategy="round_robin"),
        RouteConfig(id="lc", path_prefix="/gateway/lc",
                    upstream_ids=ids, strategy="least_connections"),
        RouteConfig(id="cache", path_prefix="/gateway/cache",
                    upstream_ids=ids, strategy="round_robin",
                    cache_ttl=1.5),
        RouteConfig(id="rl", path_prefix="/gateway/rl",
                    upstream_ids=ids, strategy="round_robin",
                    rate_limit=RateLimitConfig(requests=5, window_seconds=1.0)),
        RouteConfig(id="rl2", path_prefix="/gateway/rl2",
                    upstream_ids=ids, strategy="round_robin",
                    rate_limit=RateLimitConfig(requests=2, window_seconds=1.0)),
        RouteConfig(id="broken", path_prefix="/gateway/broken",
                    upstream_ids=("ghost",), strategy="round_robin"),
    )
    return GatewayConfig(
        upstreams=ups, routes=routes, timeout_seconds=timeout,
        whitelist=tuple(whitelist), blacklist=tuple(blacklist),
    )


@pytest.fixture()
def gw(upstreams):
    HIT_COUNTS.clear()
    from main import build_local_config, set_runtime_state
    state = GatewayState(make_config(upstreams))
    set_runtime_state(state)
    with TestClient(fastapi_app) as client:
        yield client, state
    set_runtime_state(GatewayState(build_local_config()))


@pytest.fixture()
def dead_upstream_config(upstreams):
    """一个指向已关闭端口的上游，用于 502 验证。"""
    port = _free_port()  # 无人监听
    cfg = GatewayConfig(
        upstreams=(UpstreamConfig(id="dead", base_url=f"http://127.0.0.1:{port}"),),
        routes=(RouteConfig(id="d", path_prefix="/gateway/dead",
                            upstream_ids=("dead",), strategy="round_robin"),),
        timeout_seconds=0.8,
    )
    return cfg
