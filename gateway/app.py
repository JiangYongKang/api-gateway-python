"""FastAPI 入口：装配网关并把内部错误映射为可区分的 HTTP 响应。"""

from __future__ import annotations

import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from .config import GatewayConfig
from .config_loader import default_demo_config
from .errors import GatewayError
from .logging_utils import explain, get_gateway_logger
from .proxy import GatewayProxy
from .upstream import HttpxSender, UpstreamClient

_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
}


def create_app(
    config: GatewayConfig | None = None,
    proxy: GatewayProxy | None = None,
) -> FastAPI:
    config = config or default_demo_config()
    logger = get_gateway_logger()
    app = FastAPI(title="local-api-gateway")

    if proxy is None:
        sender = HttpxSender()
        proxy = GatewayProxy(config, UpstreamClient(sender), probe_sender=sender)

    app.state.proxy = proxy
    app.state.logger = logger

    @app.get("/_gateway/health")
    async def gateway_health() -> dict:
        return {"status": "ok", "routes": proxy.explain_snapshot()}

    @app.api_route(
        "/{path:path}",
        methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
    )
    async def gateway_entry(request: Request) -> Response:
        rid = request.headers.get("X-Request-Id") or uuid.uuid4().hex[:12]
        body = await request.body()
        started = time.monotonic()
        try:
            outcome = await proxy.handle(
                method=request.method,
                path=request.url.path,
                headers={k: v for k, v in request.headers.items()},
                body=body,
                request_id=rid,
            )
        except GatewayError as exc:
            explain(
                logger, rid, "error",
                error_code=exc.error_type.value,
                message=exc.message,
                details=exc.details,
                elapsed_ms=round((time.monotonic() - started) * 1000, 2),
            )
            return JSONResponse(
                status_code=exc.status_code,
                content=exc.to_body(),
                headers={"X-Request-Id": rid},
            )
        headers = {
            k: v
            for k, v in outcome.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        headers["X-Upstream-Id"] = outcome.upstream_id
        headers["X-Request-Id"] = rid
        explain(
            logger, rid, "response",
            upstream=outcome.upstream_id,
            status_code=outcome.status_code,
            attempts=outcome.attempts,
            elapsed_ms=round((time.monotonic() - started) * 1000, 2),
        )
        return Response(
            content=outcome.body,
            status_code=outcome.status_code,
            headers=headers,
        )

    return app
