"""本地一键启动：假上游（9101）+ 网关（8000）。

用法：uv run python run_local.py
"""
from __future__ import annotations

import threading
import time

import uvicorn

from fake_upstream.server import create_upstream
from main import app


def _serve(app, port: int) -> None:
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="info")
    uvicorn.Server(config).run()


def main() -> None:
    threading.Thread(
        target=_serve, args=(create_upstream("x"), 9101), daemon=True
    ).start()
    time.sleep(0.3)
    print("fake upstreams: http://127.0.0.1:9101/{a,b,c}/...")
    print("gateway:         http://127.0.0.1:8000")
    _serve(app, 8000)


if __name__ == "__main__":
    main()
