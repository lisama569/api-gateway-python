"""pytest 夹具：真实本地 uvicorn 跑假上游与网关，httpx 走真实 TCP。"""
from __future__ import annotations

import socket
import threading
import time

import httpx
import pytest
import uvicorn

from fake_upstream.server import STATE, create_upstream
from gateway.app import create_app
from gateway.config import (
    GatewayConfig,
    RateLimitRule,
    RetryConfig,
    RouteConfig,
    Strategy,
    UpstreamConfig,
)


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class ServerHandle:
    def __init__(self, app, port: int):
        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        self.server = uvicorn.Server(config)
        self.port = port
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> ServerHandle:
        self.thread.start()
        while not self.server.started:
            time.sleep(0.005)
        return self

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


@pytest.fixture(scope="session")
def upstream_base():
    port = _free_port()
    handle = ServerHandle(create_upstream("x"), port).start()
    yield f"http://127.0.0.1:{port}"
    handle.stop()


@pytest.fixture(autouse=True)
def _reset_upstream(upstream_base):
    """每个用例前清空假上游状态与故障开关。"""
    STATE.clear()
    for name in ("a", "b", "c"):
        httpx.post(f"{upstream_base}/{name}/state/fail", params={"on": 0})
        httpx.post(f"{upstream_base}/{name}/state/slow", params={"ms": 0})
    yield


@pytest.fixture
def make_config(upstream_base):
    def _make(
        routes,
        *,
        global_limit=None,
        allowlist=frozenset(),
        blocklist=frozenset(),
        fail_timeout=5.0,
        half_open_probes=1,
    ) -> GatewayConfig:
        return GatewayConfig(
            routes=tuple(routes),
            global_rate_limit=global_limit,
            allowlist=allowlist,
            blocklist=blocklist,
            fail_timeout=fail_timeout,
            half_open_probes=half_open_probes,
        )

    def ups(names):
        return tuple(UpstreamConfig(n, f"{upstream_base}/{n}") for n in names)

    _make.ups = ups
    _make.route = RouteConfig
    _make.rule = RateLimitRule
    _make.retry = RetryConfig
    _make.strategy = Strategy
    return _make


@pytest.fixture
def gateway_server(make_config):
    """启动一个默认配置的网关；用例可用 custom_config 覆盖。"""
    handles: list[ServerHandle] = []

    def _start(config: GatewayConfig):
        app = create_app(config)
        port = _free_port()
        handle = ServerHandle(app, port).start()
        handles.append(handle)
        return f"http://127.0.0.1:{port}"

    default = make_config(
        [
            make_config.route(
                id="users",
                prefix="/api/users",
                upstreams=make_config.ups(("a", "b")),
                strategy=Strategy.ROUND_ROBIN,
                rate_limit=RateLimitRule(3, 1.0),
                cache_ttl=30,
            ),
            make_config.route(
                id="orders",
                prefix="/api/orders",
                upstreams=make_config.ups(("a", "b", "c")),
                strategy=Strategy.LEAST_CONN,
                rate_limit=RateLimitRule(4, 2.0),
            ),
            make_config.route(
                id="stream",
                prefix="/stream",
                upstreams=make_config.ups(("a",)),
                streaming=True,
                timeout=10.0,
            ),
        ],
        global_limit=RateLimitRule(100, 100),
    )
    base = _start(default)
    yield base, _start
    for h in handles:
        h.stop()
