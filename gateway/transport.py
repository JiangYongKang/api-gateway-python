"""Transport abstraction and the HTTP + in-process stub implementations.

Timeout is enforced by the transport itself, so business traffic and
health probes share the same ``send()`` contract with independent callers.
"""
from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from urllib.parse import urlencode

import httpx

from gateway.config import UpstreamConfig
from gateway.models import Request, Response

# Header hop-by-hop list (RFC 7230 §6.1 plus Host/Content-Length which the
# outbound client sets itself).
_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
}

Handler = Callable[[Request], Response]


class TransportError(Exception):
    """Connection-level failure (refused, reset, DNS, ...)."""


class TransportTimeout(Exception):
    """Upstream did not respond within the deadline."""


class UpstreamTransportProto:
    def send(self, upstream: UpstreamConfig, request: Request, *, timeout: float) -> Response:
        raise NotImplementedError


class HttpTransport:
    """Real outbound HTTP transport built on a shared httpx client."""

    def __init__(self, client: httpx.Client | None = None) -> None:
        self._client = client or httpx.Client()

    def send(self, upstream: UpstreamConfig, request: Request, *, timeout: float) -> Response:
        url = upstream.base_url.rstrip("/") + request.path
        if request.query:
            url = f"{url}?{urlencode(request.query)}"
        headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP_BY_HOP}
        httpx_request = self._client.build_request(
            request.method,
            url,
            headers=headers,
            content=request.body or None,
        )
        try:
            upstream_response = self._client.send(httpx_request, timeout=timeout)
        except httpx.TimeoutException as exc:
            raise TransportTimeout(str(exc)) from exc
        except httpx.TransportError as exc:
            raise TransportError(str(exc)) from exc
        return Response(
            status=upstream_response.status_code,
            headers={
                k: v for k, v in upstream_response.headers.items()
                if k.lower() not in _HOP_BY_HOP
            },
            body=upstream_response.content,
            upstream_id=upstream.id,
        )

    def close(self) -> None:
        self._client.close()


class StubTransport:
    """Dispatches ``stub://`` upstreams to in-process handlers.

    Handlers are plain ``Request -> Response`` callables. The deadline is
    enforced on a daemon worker thread, so a deliberately slow handler
    produces a real :class:`TransportTimeout` without blocking the caller
    (the worker may finish late and its response is discarded).
    """

    def __init__(self, handlers: dict[str, Handler] | None = None) -> None:
        self._handlers: dict[str, Handler] = dict(handlers or {})

    def register(self, upstream_id: str, handler: Handler) -> None:
        self._handlers[upstream_id] = handler

    def send(self, upstream: UpstreamConfig, request: Request, *, timeout: float) -> Response:
        handler = self._handlers.get(upstream.id)
        if handler is None:
            raise TransportError(f"no stub handler registered for upstream '{upstream.id}'")

        result_queue: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=1)

        def _work() -> None:
            try:
                result_queue.put(("response", handler(request)))
            except TransportTimeout:  # handler may self-report a timeout
                result_queue.put(("timeout", None))
            except Exception as exc:  # noqa: BLE001 - surfaced to caller below
                result_queue.put(("error", exc))

        worker = threading.Thread(target=_work, daemon=True)
        worker.start()
        try:
            kind, payload = result_queue.get(timeout=timeout)
        except queue.Empty:
            raise TransportTimeout(
                f"upstream '{upstream.id}' did not respond within {timeout:.3f}s"
            )
        if kind == "response":
            response = payload
            assert isinstance(response, Response)
            response.upstream_id = upstream.id
            return response
        if kind == "timeout":
            raise TransportTimeout(f"upstream '{upstream.id}' self-reported timeout")
        assert isinstance(payload, Exception)
        raise TransportError(str(payload)) from payload
