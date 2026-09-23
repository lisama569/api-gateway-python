"""本地一键复现：同进程内多端口启动 3 个假上游实例 + 网关。

用法：
    uv run python run_local.py
然后：
    curl -s localhost:8080/gateway/rr/echo   # 多次调用观察轮询
    curl -s localhost:8080/admin/stats
"""
from __future__ import annotations

import threading
import time

import uvicorn

from fake_upstream import create_fake_app
from main import app as gateway_app

UPSTREAM_PORTS = (9001, 9002, 9003)
GATEWAY_PORT = 8080


def _serve(instance_id: str, port: int) -> None:
    server = uvicorn.Config(
        create_fake_app(instance_id), host="127.0.0.1", port=port,
        log_level="warning", lifespan="off",
    )
    uvicorn.Server(server).run()


def main() -> None:
    threads = []
    for i, port in enumerate(UPSTREAM_PORTS):
        t = threading.Thread(
            target=_serve, args=(f"u{i}", port), daemon=True,
            name=f"upstream-{port}",
        )
        t.start()
        threads.append(t)
        print(f"[run_local] fake upstream u{i} on http://127.0.0.1:{port}")
    time.sleep(0.8)
    print(f"[run_local] gateway on http://127.0.0.1:{GATEWAY_PORT}")
    cfg = uvicorn.Config(gateway_app, host="127.0.0.1",
                         port=GATEWAY_PORT, log_level="info")
    uvicorn.Server(cfg).run()


if __name__ == "__main__":
    main()
