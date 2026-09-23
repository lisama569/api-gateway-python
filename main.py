"""仓库现有 HTTP 服务入口：启动本地反向代理网关。"""
from __future__ import annotations

import uvicorn

from gateway.app import create_app
from gateway.local_config import build_local_config

app = create_app(build_local_config())


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
