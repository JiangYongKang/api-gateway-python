"""本地演示：从 JSON 配置启动网关，并开启后台健康探测。

    python demo/run_gateway.py demo/gateway_config.json
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import uvicorn

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gateway.app import create_app  # noqa: E402
from gateway.config_loader import load_config  # noqa: E402


def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "demo/gateway_config.json"
    with open(config_path, encoding="utf-8") as f:
        config = load_config(json.load(f))
    app = create_app(config)

    @app.on_event("startup")
    async def _start_probe() -> None:  # pragma: no cover - 演示脚本
        app.state.proxy.start_background_probe()

    @app.on_event("shutdown")
    async def _stop_probe() -> None:  # pragma: no cover - 演示脚本
        await app.state.proxy.stop_background_probe()

    uvicorn.run(
        app,
        host=os.environ.get("GW_HOST", "127.0.0.1"),
        port=int(os.environ.get("GW_PORT", "18080")),
        log_level="warning",
    )


if __name__ == "__main__":
    main()
