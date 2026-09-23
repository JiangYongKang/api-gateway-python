"""Gateway facade: match -> limit -> forward -> map errors.

Also owns the active health-probe loop. Probe requests are built by the
gateway and sent *directly* via the transport: they bypass routing,
rate limiting, concurrency slots and the load balancer, so probing can
never disturb business traffic accounting.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

from gateway.config import GatewayConfig
from gateway.errors import ErrorCode, GatewayError
from gateway.forwarder import Forwarder
from gateway.health import HealthRegistry
from gateway.loadbalancer import LoadBalancer
from gateway.logging_setup import decision_logger, log_event
from gateway.models import Request, Response
from gateway.ratelimiter import ClientLimiter
from gateway.router import Router
from gateway.transport import (
    HttpTransport,
    TransportError,
    TransportTimeout,
    UpstreamTransportProto,
)


class Gateway:
    def __init__(
        self,
        config: GatewayConfig,
        transport: UpstreamTransportProto | None = None,
        logger: logging.Logger | None = None,
        *,
        clock=time.monotonic,
    ) -> None:
        self._config = config
        self.config_max_body_bytes = config.max_body_bytes
        self._log = logger or decision_logger("gateway")
        self._transport = transport or HttpTransport()
        self._clock = clock

        self.router = Router(config.routes)
        self.health = HealthRegistry(config.upstreams, clock=clock)
        self.lb = LoadBalancer()
        self.limiter = ClientLimiter(config.rate, clock=clock)
        self._forwarder = Forwarder(
            config, self.health, self.lb, self._transport,
            logger=self._log, clock=clock,
        )
        self._probe_stop = threading.Event()
        self._probe_thread: threading.Thread | None = None

    # -- lifecycle --------------------------------------------------------
    def start_probe_loop(self, interval: float = 0.05) -> None:
        if self._probe_thread is not None:
            return
        self._probe_stop.clear()
        self._probe_thread = threading.Thread(
            target=self._probe_loop, args=(interval,), daemon=True, name="gw-probes"
        )
        self._probe_thread.start()

    def stop_probe_loop(self) -> None:
        self._probe_stop.set()
        if self._probe_thread is not None:
            self._probe_thread.join(timeout=2.0)
            self._probe_thread = None

    def run_probe_tick(self) -> int:
        """Execute one probe sweep synchronously (used by tests / scripts).

        Returns the number of probes dispatched.
        """
        jobs = self.health.on_probe_tick(self._clock())
        for upstream_id, probe_path, info in jobs:
            self._run_one_probe(info["route_id"], upstream_id, probe_path, info["timeout"])
        return len(jobs)

    # -- main request path ------------------------------------------------
    def handle(self, request: Request) -> Response:
        try:
            rule = self.router.match(request)
        except GatewayError as error:
            log_event(
                self._log, "gateway_error",
                method=request.method, path=request.path,
                client=request.client_id, code=error.code.value,
            )
            return _error_response(error)
        client_id = request.client_id

        allowed, reason, limit_snapshot = self.limiter.acquire(client_id, self._clock())
        log_event(
            self._log, "request",
            method=request.method, path=request.path, query=request.query,
            client=client_id, rule=rule.id,
            lb_policy=rule.lb_policy.value,
            upstreams=list(rule.upstream_ids),
            limit_decision=limit_snapshot,
        )
        if not allowed:
            if reason == "rate":
                code = ErrorCode.RATE_LIMITED
                message = f"client '{client_id}' exceeded request quota"
                retry_after = int(max(1, self._config.rate.window_seconds))
            else:
                code = ErrorCode.CONCURRENCY_LIMITED
                message = f"client '{client_id}' exceeded concurrency limit"
                retry_after = 0
            log_event(
                self._log, "rejected",
                reason=reason, client=client_id, path=request.path,
                limit=limit_snapshot,
            )
            error = GatewayError(
                code, message, details={"client_id": client_id, "limit": limit_snapshot}
            )
            return _error_response(error, retry_after=retry_after)

        try:
            result = self._forwarder.forward(request, rule)
        finally:
            self.limiter.release(client_id)

        if result.response is not None:
            log_event(
                self._log, "response",
                client=client_id, path=request.path, rule=rule.id,
                status=result.response.status,
                upstream=result.response.upstream_id,
                attempts=[(a.upstream_id, a.reason) for a in result.attempts],
            )
            return result.response

        assert result.error is not None
        log_event(
            self._log, "gateway_error",
            client=client_id, path=request.path, rule=rule.id,
            code=result.error.code.value,
            attempts=[(a.upstream_id, a.reason) for a in result.attempts],
            health=self.health.snapshot(),
        )
        return _error_response(result.error)

    def explain(self, request: Request) -> dict[str, Any]:
        return self.router.explain(request)

    # -- probes -----------------------------------------------------------
    def _probe_loop(self, interval: float) -> None:
        while not self._probe_stop.wait(interval):
            try:
                self.run_probe_tick()
            except Exception as exc:  # noqa: BLE001 - probe loop must survive
                log_event(self._log, "probe_loop_error", error=repr(exc))

    def _run_one_probe(
        self, route_id: str, upstream_id: str, probe_path: str, timeout: float
    ) -> None:
        upstream = next((u for u in self._config.upstreams if u.id == upstream_id), None)
        if upstream is None:
            return
        probe_request = Request(method="GET", path=probe_path)
        started = self._clock()
        try:
            response = self._transport.send(upstream, probe_request, timeout=timeout)
            ok = 200 <= response.status < 400
            reason = f"status_{response.status}"
        except TransportTimeout as exc:
            ok = False
            reason = f"timeout:{exc}"
        except TransportError as exc:
            ok = False
            reason = f"connect_error:{exc}"
        elapsed_ms = round((self._clock() - started) * 1000, 2)
        self.health.record_probe_result(route_id, upstream_id, ok=ok, now=self._clock())
        log_event(
            self._log, "probe",
            route=route_id, upstream=upstream_id, path=probe_path,
            ok=ok, reason=reason, elapsed_ms=elapsed_ms,
            health=self.health.snapshot(),
        )


def _error_response(error: GatewayError, *, retry_after: int = 0) -> Response:
    import json

    headers = {"content-type": "application/json"}
    if retry_after:
        headers["retry-after"] = str(retry_after)
    return Response(
        status=error.http_status,
        headers=headers,
        body=json.dumps(error.to_body(), ensure_ascii=False).encode("utf-8"),
    )

