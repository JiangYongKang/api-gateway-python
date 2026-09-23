"""本地演示用假上游。

用法:
    python demo/upstream_server.py users-1 18081
    python demo/upstream_server.py users-2 18082

控制接口（无需重启即可模拟故障/恢复）：
    POST /_control/fail      之后所有业务请求返回 500
    POST /_control/hang      之后所有业务请求 sleep（模拟超时）
    POST /_control/ok        恢复正常
    GET  /healthz            健康探测端点
"""

from __future__ import annotations

import os
import sys
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()
STATE = {"mode": "ok"}
NAME = os.environ.get("UP_NAME", sys.argv[1] if len(sys.argv) > 1 else "upstream")


@app.post("/_control/{mode}")
async def control(mode: str):
    STATE["mode"] = mode
    return {"name": NAME, "mode": STATE["mode"]}


@app.get("/healthz")
async def healthz():
    return {"name": NAME, "status": "ok", "mode": STATE["mode"], "ts": time.time()}


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def catch_all(path: str, request: Request):
    if STATE["mode"] == "fail":
        return JSONResponse(status_code=500, content={"name": NAME, "error": "forced"})
    if STATE["mode"] == "hang":
        time.sleep(5)
    return {
        "name": NAME,
        "path": f"/{path}",
        "method": request.method,
        "client_header": request.headers.get("x-client-id"),
        "ts": time.time(),
    }
