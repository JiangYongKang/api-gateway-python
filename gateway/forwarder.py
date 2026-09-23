"""Forwarding orchestration: LB selection, retries, timeout/error mapping.

Deterministic retry rule:
  * at most ``1 + max_retries`` attempts per request;
  * a new attempt is allowed only for idempotent methods when the *last*
    attempt failed via connect error, timeout, or a configured retryable
    status (non-retryable 4xx stops immediately);
  * already-ejected upstreams are excluded (no request is ever sent to an
    ejected instance), and the same upstream is never retried twice for
    one request;
  * rate-limit/concurrency rejections happen before forwarding and never
    reach here, so retries cannot create extra side effects for a request
    the gateway refused.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from gateway.config import GatewayConfig, RouteRule
from gateway.errors import ErrorCode, GatewayError
from gateway.health import HealthRegistry
from gateway.loadbalancer import LoadBalancer
from gateway.logging_setup import decision_logger, log_event
from gateway.models import Request, Response
from gateway.transport import (
    TransportError,
    TransportTimeout,
    UpstreamTransportProto,
)

_IDEMPOTENT = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})


@dataclass
class Attempt:
    upstream_id: str
    ok: bool
    reason: str
    status: int | None = None
    elapsed: float = 0.0
    retryable: bool = False


@dataclass
class ForwardResult:
    response: Response | None
    attempts: list[Attempt] = field(default_factory=list)
    error: GatewayError | None = None


class Forwarder:
    def __init__(
        self,
        config: GatewayConfig,
        health: HealthRegistry,
        lb: LoadBalancer,
        transport: UpstreamTransportProto,
        logger: logging.Logger | None = None,
        *,
        clock=time.monotonic,
    ) -> None:
        self._config = config
        self._upstreams = {u.id: u for u in config.upstreams}
        self._health = health
        self._lb = lb
        self._transport = transport
        self._log = logger or decision_logger("forwarder")
        self._clock = clock

    def forward(self, request: Request, rule: RouteRule) -> ForwardResult:
        attempts: list[Attempt] = []
        tried: set[str] = set()

        max_attempts = 1 + max(0, rule.max_retries)
        for attempt_no in range(1, max_attempts + 1):
            candidates = self._health.healthy_candidates(rule)
            selected, lb_explain = self._lb.acquire(
                request, rule, candidates, exclude=frozenset(tried)
            )
            if selected is None:
                if attempts:
                    break  # all candidates tried / ejected
                reason, states = self._health.no_candidate_reason(rule)
                if reason == "all_circuits_open":
                    code = ErrorCode.UPSTREAM_CIRCUIT_OPEN
                    message = f"circuit open for every upstream of route '{rule.id}'"
                else:
                    code = ErrorCode.ALL_UPSTREAMS_UNAVAILABLE
                    message = f"all upstreams for route '{rule.id}' are unavailable"
                log_event(
                    self._log, "no_upstream",
                    rule=rule.id, path=request.path,
                    reason=reason, candidates=candidates,
                    breaker_states=states, health=self._health.snapshot(),
                )
                return ForwardResult(
                    response=None,
                    attempts=attempts,
                    error=GatewayError(
                        code,
                        message,
                        details={
                            "rule_id": rule.id,
                            "reason": reason,
                            "upstreams": list(rule.upstream_ids),
                            "breaker_states": states,
                            "health": self._health.snapshot(),
                        },
                    ),
                )

            tried.add(selected)
            upstream = self._upstreams[selected]
            started = self._clock()
            attempt: Attempt
            try:
                response = self._transport.send(
                    upstream, request, timeout=rule.timeout_seconds
                )
                elapsed = self._clock() - started
                status = response.status
                ok = 200 <= status < 400
                retryable = not ok and status in rule.retry_on_statuses
                attempt = Attempt(
                    selected, ok,
                    "success" if ok else f"status_{status}",
                    status, elapsed, retryable,
                )
                self._health.record_business_result(rule, selected, ok=ok)
                log_event(
                    self._log, "attempt",
                    rule=rule.id, attempt=attempt_no, upstream=selected,
                    policy=lb_explain.get("policy"),
                    selected=selected, candidates=lb_explain.get("candidates"),
                    result="success" if ok else "upstream_error",
                    status=status, elapsed_ms=round(elapsed * 1000, 2),
                    retryable=retryable, health=self._health.snapshot(),
                )
                if ok:
                    return ForwardResult(response=response, attempts=attempts + [attempt])
            except TransportTimeout as exc:
                elapsed = self._clock() - started
                attempt = Attempt(selected, False, "timeout", None, elapsed, True)
                self._health.record_business_result(rule, selected, ok=False)
                log_event(
                    self._log, "attempt",
                    rule=rule.id, attempt=attempt_no, upstream=selected,
                    result="timeout", elapsed_ms=round(elapsed * 1000, 2),
                    error=str(exc), health=self._health.snapshot(),
                )
            except TransportError as exc:
                elapsed = self._clock() - started
                attempt = Attempt(selected, False, "connect_error", None, elapsed, True)
                self._health.record_business_result(rule, selected, ok=False)
                log_event(
                    self._log, "attempt",
                    rule=rule.id, attempt=attempt_no, upstream=selected,
                    result="connect_error", elapsed_ms=round(elapsed * 1000, 2),
                    error=str(exc), health=self._health.snapshot(),
                )
            finally:
                self._lb.release(rule.id, selected)
            attempts.append(attempt)

            will_retry = (
                attempt.retryable
                and attempt_no < max_attempts
                and request.method.upper() in _IDEMPOTENT
            )
            log_event(
                self._log, "retry_decision",
                rule=rule.id, attempt=attempt_no,
                will_retry=will_retry,
                retries_left=max_attempts - attempt_no - 1,
                idempotent=request.method.upper() in _IDEMPOTENT,
                last_reason=attempt.reason,
            )
            if not will_retry:
                break

        # All attempts exhausted. If at least one attempt was made, the
        # caller receives that attempt's real failure (timeout/5xx);
        # ALL_UPSTREAMS_UNAVAILABLE is reserved for requests that could
        # not be sent anywhere at all.
        last = attempts[-1]
        if last.reason == "timeout":
            code = ErrorCode.UPSTREAM_TIMEOUT
            message = f"upstream timed out after {len(attempts)} attempt(s)"
        elif last.reason == "connect_error":
            code = ErrorCode.UPSTREAM_ERROR
            message = f"upstream connection failure after {len(attempts)} attempt(s)"
        else:
            code = ErrorCode.UPSTREAM_ERROR
            message = f"upstream returned error {last.status} after {len(attempts)} attempt(s)"

        return ForwardResult(
            response=None,
            attempts=attempts,
            error=GatewayError(
                code,
                message,
                upstream_status=last.status,
                details={
                    "rule_id": rule.id,
                    "attempts": [vars(a) for a in attempts],
                    "remaining_healthy": self._health.healthy_candidates(rule),
                },
            ),
        )
