"""Upstream health: failure-driven ejection, deterministic probing, recovery.

Two independent failure paths feed one per (route, upstream) breaker and
must not interfere:

* **business traffic** – a real request response is recorded via
  :meth:`HealthRegistry.record_business_result`; only upstreams admitted by
  :meth:`HealthRegistry.healthy_candidates` ever receive traffic.
* **active probes** – the registry schedules deterministic probe *jobs*
  via :meth:`HealthRegistry.on_probe_tick`; a runner sends them directly
  with the transport (never through rate limiting or the load balancer)
  and reports back via :meth:`HealthRegistry.record_probe_result`.

Breaker states per (route, upstream):

    HEALTHY (closed)
        -- consecutive failures >= failure_threshold -->
    EJECTED (open, cooling for open_state_seconds, no traffic)
        -- cool-down elapsed, on next probe tick -->
    PROBING (half-open; up to half_open_max_probe_requests trial jobs)
        -- half_open_success_threshold consecutive probe successes --> HEALTHY
        -- any failure --> EJECTED (cool-down restarts)
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from gateway.config import RouteRule, UpstreamConfig


class HealthState(str, Enum):
    HEALTHY = "HEALTHY"
    EJECTED = "EJECTED"
    PROBING = "PROBING"


@dataclass
class UpstreamHealth:
    """Aggregated, explainable view of one upstream across routes."""

    upstream_id: str
    state: HealthState = HealthState.HEALTHY
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    ejected_at: float | None = None
    trial_in_flight: int = 0
    probes_attempted: int = 0
    last_changed_at: float = 0.0
    last_result: str | None = None
    routes: dict[str, dict[str, Any]] = field(default_factory=dict)


class _Breaker:
    """Circuit breaker for one (route, upstream) pair."""

    def __init__(self, route_id: str, upstream_id: str) -> None:
        self.route_id = route_id
        self.upstream_id = upstream_id
        self.state = HealthState.HEALTHY
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.ejected_at: float | None = None
        self.last_changed_at = 0.0
        # Half-open bookkeeping:
        self.trial_in_flight = 0
        self.probes_attempted = 0
        self.last_probe_dispatch: float | None = None
        # Only set while half-open; source of truth for trial accounting.
        self.rule: RouteRule | None = None

    def trial_budget(self) -> int:
        """Total half-open trial attempts (business + probe) allowed.

        With the defaults (capacity 1, success threshold 1) exactly one
        trial is possible. Requiring N consecutive successes needs N total
        attempts to even be achievable, hence the ``+ threshold - 1`` term.
        """
        if self.rule is None:
            return 1
        return (
            self.rule.half_open_max_probe_requests
            + self.rule.half_open_success_threshold
            - 1
        )

    def trial_slots_used(self) -> int:
        return self.trial_in_flight + self.probes_attempted


class HealthRegistry:
    def __init__(
        self,
        upstreams: tuple[UpstreamConfig, ...],
        *,
        clock=time.monotonic,
    ) -> None:
        self._clock = clock
        self._upstreams = {u.id: u for u in upstreams}
        self._lock = threading.Lock()
        # key -> breaker, created lazily on first use of a (route, upstream)
        self._breakers: dict[tuple[str, str], _Breaker] = {}
        self._global_probes = {uid: 0 for uid in self._upstreams}
        self._global_attempted = {uid: 0 for uid in self._upstreams}
        self._last_result: dict[str, str] = {}

    # ------------------------------------------------------------------ #
    # Business traffic path
    # ------------------------------------------------------------------ #
    def _breaker(self, rule: RouteRule, upstream_id: str) -> _Breaker:
        key = (rule.id, upstream_id)
        breaker = self._breakers.get(key)
        if breaker is None:
            breaker = _Breaker(rule.id, upstream_id)
            self._breakers[key] = breaker
        breaker.rule = rule
        return breaker

    def healthy_candidates(self, rule: RouteRule, now: float | None = None) -> list[str]:
        """Admit upstreams for business traffic, in configuration order.

        HEALTHY breakers always pass. EJECTED breakers never pass. A
        PROBING breaker passes only while trial capacity remains; this
        method itself consumes one half-open trial slot atomically, so a
        half-open upstream can never receive more than
        ``half_open_max_probe_requests`` concurrent trial requests.
        """
        now = self._clock() if now is None else now
        admitted: list[str] = []
        with self._lock:
            for uid in rule.upstream_ids:
                if uid not in self._upstreams:
                    continue
                breaker = self._breaker(rule, uid)
                self._advance(breaker, now)
                if breaker.state is HealthState.HEALTHY:
                    admitted.append(uid)
                # Business trials share the concurrent in-flight cap with
                # probes; a probe occupying a slot blocks business traffic.
                elif (
                    breaker.state is HealthState.PROBING
                    and breaker.trial_in_flight < rule.half_open_max_probe_requests
                ):
                    breaker.trial_in_flight += 1
                    self._global_probes[uid] = self._global_probes.get(uid, 0) + 1
                    admitted.append(uid)
        return admitted

    def no_candidate_reason(
        self, rule: RouteRule, now: float | None = None
    ) -> tuple[str, dict[str, Any]]:
        """Explain why :meth:`healthy_candidates` returned an empty list.

        Returns ``(reason, per_upstream_states)`` where reason is:
          * ``"no_upstream_configured"`` – the route lists no known upstream;
          * ``"all_circuits_open"`` – every known upstream is EJECTED
            (genuine circuit-open condition);
          * ``"half_open_capacity_exhausted"`` – upstreams exist only in
            PROBING and all trial slots are occupied.
        """
        now = self._clock() if now is None else now
        states: dict[str, str] = {}
        known = [u for u in rule.upstream_ids if u in self._upstreams]
        if not known:
            return "no_upstream_configured", states
        with self._lock:
            # Read-only diagnostic: do NOT call _advance here, otherwise just
            # explaining a failure would perform the EJECTED -> PROBING
            # transition and consume (or expose) half-open state.
            for uid in known:
                breaker = self._breaker(rule, uid)
                states[uid] = breaker.state.value
        if all(s == HealthState.EJECTED.value for s in states.values()):
            return "all_circuits_open", states
        return "half_open_capacity_exhausted", states

    def record_business_result(
        self,
        rule: RouteRule,
        upstream_id: str,
        ok: bool,
        now: float | None = None,
    ) -> UpstreamHealth:
        now = self._clock() if now is None else now
        with self._lock:
            breaker = self._breaker(rule, upstream_id)
            self._advance(breaker, now)
            # Release a half-open trial slot if this request occupied one.
            if breaker.trial_in_flight > 0:
                breaker.trial_in_flight -= 1
                self._global_probes[upstream_id] = max(0, self._global_probes.get(upstream_id, 0) - 1)
                breaker.probes_attempted += 1
            if ok:
                if breaker.state is HealthState.PROBING:
                    breaker.consecutive_successes += 1
                    threshold = breaker.rule.half_open_success_threshold if breaker.rule else 1
                    if breaker.consecutive_successes >= threshold:
                        self._close(breaker, now)
                else:
                    breaker.consecutive_failures = 0
                self._last_result[upstream_id] = "business_success"
            else:
                self._last_result[upstream_id] = "business_failure"
                if breaker.state is HealthState.PROBING:
                    # Trial failed: straight back to cooling-down.
                    self._open(breaker, now)
                else:
                    breaker.consecutive_failures += 1
                    if breaker.consecutive_failures >= rule.failure_threshold:
                        self._open(breaker, now)
            return self._status_locked(upstream_id)

    # ------------------------------------------------------------------ #
    # Active probe path (independent scheduler-driven jobs)
    # ------------------------------------------------------------------ #
    def on_probe_tick(self, now: float | None = None) -> list[tuple[str, str, dict[str, Any]]]:
        """Return probe jobs due now: ``(upstream_id, probe_path, info)``.

        Deterministic: a breaker in EJECTED whose cool-down has elapsed
        flips to PROBING, then one probe job is dispatched every
        ``probe_interval_seconds`` while success budget / capacity remain.
        """
        now = self._clock() if now is None else now
        jobs: list[tuple[str, str, dict[str, Any]]] = []
        with self._lock:
            for (route_id, uid), breaker in list(self._breakers.items()):
                self._advance(breaker, now)
                rule = breaker.rule
                if rule is None or breaker.state is not HealthState.PROBING:
                    continue
                capacity = rule.half_open_max_probe_requests
                if breaker.trial_in_flight >= capacity:
                    continue
                if breaker.trial_slots_used() >= breaker.trial_budget():
                    continue
                if breaker.consecutive_successes >= rule.half_open_success_threshold:
                    self._close(breaker, now)
                    continue
                if (
                    breaker.last_probe_dispatch is None
                    or now - breaker.last_probe_dispatch >= rule.probe_interval_seconds
                ):
                    breaker.trial_in_flight += 1
                    breaker.last_probe_dispatch = now
                    self._global_probes[uid] = self._global_probes.get(uid, 0) + 1
                    info = {
                        "route_id": route_id,
                        "timeout": rule.probe_timeout_seconds,
                        "state": breaker.state.value,
                    }
                    jobs.append((uid, rule.probe_path, info))
        return jobs

    def record_probe_result(
        self,
        route_id: str,
        upstream_id: str,
        ok: bool,
        now: float | None = None,
    ) -> UpstreamHealth:
        now = self._clock() if now is None else now
        with self._lock:
            breaker = self._breakers.get((route_id, upstream_id))
            if breaker is not None and breaker.state is HealthState.PROBING:
                breaker.trial_in_flight = max(0, breaker.trial_in_flight - 1)
                self._global_probes[upstream_id] = max(0, self._global_probes.get(upstream_id, 0) - 1)
                breaker.probes_attempted += 1
                self._global_attempted[upstream_id] = self._global_attempted.get(upstream_id, 0) + 1
                rule = breaker.rule
                if ok:
                    self._last_result[upstream_id] = "probe_success"
                    breaker.consecutive_successes += 1
                    threshold = rule.half_open_success_threshold if rule else 1
                    if breaker.consecutive_successes >= threshold:
                        self._close(breaker, now)
                else:
                    self._last_result[upstream_id] = "probe_failure"
                    self._open(breaker, now)
            return self._status_locked(upstream_id)

    # ------------------------------------------------------------------ #
    # State transitions (caller must hold the lock)
    # ------------------------------------------------------------------ #
    def _advance(self, breaker: _Breaker, now: float) -> None:
        """EJECTED -> PROBING once cool-down elapses."""
        if breaker.state is HealthState.EJECTED and breaker.ejected_at is not None:
            wait = breaker.rule.open_state_seconds if breaker.rule else 5.0
            if now - breaker.ejected_at >= wait:
                breaker.state = HealthState.PROBING
                breaker.consecutive_successes = 0
                breaker.trial_in_flight = 0
                breaker.probes_attempted = 0
                breaker.last_probe_dispatch = None
                breaker.last_changed_at = now

    def _open(self, breaker: _Breaker, now: float) -> None:
        breaker.state = HealthState.EJECTED
        breaker.ejected_at = now
        breaker.consecutive_failures = 0
        breaker.consecutive_successes = 0
        breaker.trial_in_flight = 0
        breaker.probes_attempted = 0
        breaker.last_changed_at = now

    def _close(self, breaker: _Breaker, now: float) -> None:
        breaker.state = HealthState.HEALTHY
        breaker.consecutive_failures = 0
        breaker.consecutive_successes = 0
        breaker.ejected_at = None
        breaker.trial_in_flight = 0
        breaker.probes_attempted = 0
        breaker.last_changed_at = now

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    def status(self, upstream_id: str) -> UpstreamHealth:
        with self._lock:
            return self._status_locked(upstream_id)

    def _status_locked(self, upstream_id: str) -> UpstreamHealth:
            breakers = [b for (_, uid), b in self._breakers.items() if uid == upstream_id]
            if any(b.state is HealthState.EJECTED for b in breakers):
                state = HealthState.EJECTED
            elif any(b.state is HealthState.PROBING for b in breakers):
                state = HealthState.PROBING
            else:
                state = HealthState.HEALTHY
            ejected_at = None
            if state is HealthState.EJECTED:
                ejected_at = min(
                    (b.ejected_at for b in breakers if b.ejected_at is not None),
                    default=None,
                )
            return UpstreamHealth(
                upstream_id=upstream_id,
                state=state,
                consecutive_failures=max((b.consecutive_failures for b in breakers), default=0),
                consecutive_successes=max((b.consecutive_successes for b in breakers), default=0),
                ejected_at=ejected_at,
                trial_in_flight=self._global_probes.get(upstream_id, 0),
                probes_attempted=self._global_attempted.get(upstream_id, 0),
                last_changed_at=max((b.last_changed_at for b in breakers), default=0.0),
                last_result=self._last_result.get(upstream_id),
                routes={
                    b.route_id: {
                        "state": b.state.value,
                        "consecutive_failures": b.consecutive_failures,
                        "consecutive_successes": b.consecutive_successes,
                        "ejected_at": b.ejected_at,
                        "trial_in_flight": b.trial_in_flight,
                        "probes_attempted": b.probes_attempted,
                    }
                    for b in breakers
                },
            )

    def snapshot(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            out: dict[str, dict[str, Any]] = {}
            for uid in self._upstreams:
                view = self._status_locked(uid)
                out[uid] = {
                    "state": view.state.value,
                    "consecutive_failures": view.consecutive_failures,
                    "consecutive_successes": view.consecutive_successes,
                    "ejected_at": view.ejected_at,
                    "trial_in_flight": view.trial_in_flight,
                    "probes_attempted": view.probes_attempted,
                    "last_changed_at": view.last_changed_at,
                    "last_result": view.last_result,
                    "routes": view.routes,
                }
            return out
