"""Local demo wiring: in-process stub upstreams behind one gateway.

Run with ``uv run uvicorn main:app`` and exercise routes using curl.
The stub transport means no external service is needed: every upstream
behaves deterministically inside the one process.
"""
from __future__ import annotations

import json
import time

from gateway.config import GatewayConfig, demo_config
from gateway.gateway import Gateway
from gateway.models import Request, Response
from gateway.transport import StubTransport


def _json_response(status: int, payload: dict) -> Response:
    return Response(
        status=status,
        headers={"content-type": "application/json"},
        body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )


def _make_echo(upstream_id: str, *, delay: float = 0.0, fail: bool = False):
    def handler(request: Request) -> Response:
        if delay:
            time.sleep(delay)
        if fail:
            return _json_response(500, {"error": f"{upstream_id} forced failure"})
        return _json_response(200, {
            "upstream": upstream_id,
            "method": request.method,
            "path": request.path,
            "query": request.query,
            "body": request.body.decode("utf-8", "replace"),
            "served_at": round(time.monotonic(), 6),
        })

    return handler


def _make_slow(delay: float):
    def handler(request: Request) -> Response:
        time.sleep(delay)  # longer than the route timeout
        return _json_response(200, {"upstream": "slow"})

    return handler


def _make_health(upstream_id: str, *, healthy: bool = True):
    # Health probes get a dedicated handler so they never mutate business state.
    def handler(request: Request) -> Response:
        if healthy:
            return _json_response(200, {"upstream": upstream_id, "health": "ok"})
        return _json_response(503, {"upstream": upstream_id, "health": "bad"})

    return handler


def build_demo_gateway(config: GatewayConfig | None = None, *, start_probes: bool = True) -> Gateway:
    config = config or demo_config()
    # flaky and slow serve /health OK so probe-driven recovery is observable.
    transport = StubTransport({
        "echo-a": _route_handler(_make_echo("echo-a"), _make_health("echo-a")),
        "echo-b": _route_handler(_make_echo("echo-b"), _make_health("echo-b")),
        "echo-c": _route_handler(_make_echo("echo-c"), _make_health("echo-c")),
        "echo-v2": _route_handler(_make_echo("echo-v2"), _make_health("echo-v2")),
        "flaky": _route_handler(
            _make_echo("flaky", fail=True), _make_health("flaky")
        ),
        "slow": _route_handler(_make_slow(2.0), _make_health("slow")),
    })
    gateway = Gateway(config, transport=transport)
    if start_probes:
        gateway.start_probe_loop(interval=0.05)
    return gateway


def _route_handler(business, health):
    """Dispatch /health probes to the health handler, rest to business."""
    def handler(request: Request) -> Response:
        if request.path.rstrip("/").endswith("/health"):
            return health(request)
        return business(request)

    return handler
