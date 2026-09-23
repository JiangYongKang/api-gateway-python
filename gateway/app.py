"""FastAPI ASGI adapter for the gateway core.

One catch-all route feeds every request through the deterministic gateway
pipeline. Gateway failures are rendered here with the documented status
codes and ``error.code`` body; framework-level 404s never happen silently.
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import FastAPI
from fastapi import Request as FastRequest
from fastapi.responses import Response as FastResponse

from gateway.errors import ErrorCode, GatewayError
from gateway.gateway import Gateway
from gateway.models import Request as GwRequest
from gateway.models import Response as GwResponse


def create_app(gateway: Gateway) -> FastAPI:
    app = FastAPI(title="local-api-gateway", version="0.1.0")

    @app.get("/_gateway/health")
    def gateway_health() -> dict[str, Any]:
        """Gateway introspection endpoint (not forwarded upstream)."""
        return {
            "status": "ok",
            "upstreams": gateway.health.snapshot(),
            "limiter": gateway.limiter.snapshot(),
            "lb": gateway.lb.snapshot(),
        }

    @app.get("/_gateway/routes/explain")
    async def routes_explain(request: FastRequest) -> dict[str, Any]:
        body = await request.body()
        gw_request = _to_gateway_request(request, body)
        return gateway.explain(gw_request)

    @app.api_route(
        "/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    async def catch_all(path: str, request: FastRequest) -> FastResponse:
        body = await request.body()
        if len(body) > gateway.config_max_body_bytes:
            error = GatewayError(
                ErrorCode.UPSTREAM_ERROR,
                f"request body exceeds {gateway.config_max_body_bytes} bytes",
                details={"reason": "body_too_large"},
            )
            return _to_http_response(_error_response(error, 413))
        gw_request = _to_gateway_request(request, body)
        try:
            gw_response = gateway.handle(gw_request)
        except GatewayError as exc:
            gw_response = _error_response(exc)
        return _to_http_response(gw_response)

    return app


def _to_gateway_request(req: FastRequest, body: bytes) -> GwRequest:
    query = {k: v for k, v in req.query_params.items()}
    return GwRequest(
        method=req.method,
        path=req.url.path,
        headers={k: v for k, v in req.headers.items()},
        query=query,
        body=body,
    )


def _to_http_response(resp: GwResponse) -> FastResponse:
    content_type = resp.headers.get("content-type", "application/octet-stream")
    headers = {k: v for k, v in resp.headers.items() if k.lower() != "content-type"}
    return FastResponse(
        content=resp.body,
        status_code=resp.status,
        headers=headers,
        media_type=content_type,
    )


def _error_response(error: GatewayError, status: int | None = None) -> GwResponse:
    code = error.http_status if status is None else status
    return GwResponse(
        status=code,
        headers={"content-type": "application/json"},
        body=json.dumps(error.to_body(), ensure_ascii=False).encode("utf-8"),
    )
