"""Weighted gray (canary) routing and client stickiness.

This controller is *additive*: a route only enters gray mode when weights are
configured (statically or at runtime) or stickiness is enabled. Routes with
neither keep the legacy ``LoadBalancer`` behaviour untouched.

Deterministic rules (all decisions are explainable and reproducible):

* **Weighted selection** – smooth weighted round robin (nginx-style) over
  the eligible upstreams (weight > 0, healthy, not excluded). Given the
  same weights and the same request sequence the picks are identical, and
  the per-pick scores are logged so every decision can be replayed.
* **Stickiness** – the first assignment of a client is hashed
  (sha256 of ``route_id|client_id``) into the weight ranges, so the same
  client maps to the same instance for the same weight table; the
  assignment is then pinned until the TTL expires, the target is ejected,
  or the target's weight drops to 0. Reassignment follows the same hash
  rule, so a recovered instance never instantly re-absorbs its old
  clients (no switch-back thundering herd).
* **Runtime re-weighting** – ``set_weights`` swaps the table atomically
  under the same lock that guards selection; in-flight requests are
  unaffected, pinned clients stay pinned while their target remains
  eligible, and one client can never be observed on two instances at
  the same time.
"""
from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass
from typing import Any

from gateway.config import RouteRule


@dataclass
class _StickyEntry:
    upstream_id: str
    expires_at: float


class GrayController:
    """Per-route weight tables, sticky pins and traffic accounting.

    A single lock guards every mutation and every selection, so a client
    pin is created/read atomically and a weight swap can never interleave
    with an assignment: concurrent requests of one client always resolve
    to exactly one instance.
    """

    def __init__(self, *, clock=time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        # route_id -> {upstream_id: weight}; presence of the key means the
        # route is in weighted mode.
        self._weights: dict[str, dict[str, int]] = {}
        # route_id -> sticky ttl seconds (>0 means stickiness enabled).
        self._sticky_ttl: dict[str, float] = {}
        # (route_id, client_id) -> pin
        self._sticky: dict[tuple[str, str], _StickyEntry] = {}
        # route_id -> {upstream_id: current smooth-WRR score}
        self._wrr_current: dict[str, dict[str, int]] = {}
        # (route_id, upstream_id) -> assigned request count
        self._served: dict[tuple[str, str], int] = {}

    # ------------------------------------------------------------------ #
    # Configuration
    # ------------------------------------------------------------------ #
    def configure_route(self, rule: RouteRule) -> None:
        """Register static config for one route (called once at startup)."""
        with self._lock:
            if rule.weights is not None:
                self._set_weights_locked(rule.id, rule.weights)
            if rule.sticky_ttl_seconds > 0:
                self._sticky_ttl[rule.id] = rule.sticky_ttl_seconds

    def is_enabled(self, route_id: str) -> bool:
        """True when the route uses weighted selection and/or stickiness."""
        with self._lock:
            return route_id in self._weights or self._sticky_ttl.get(route_id, 0) > 0

    def set_weights(self, route_id: str, weights: dict[str, int]) -> None:
        """Atomically replace the weight table of a route (runtime adjust).

        Validation (non-negative integers) happens here; upstream-id
        validation against the route is the caller's job.
        """
        with self._lock:
            self._set_weights_locked(route_id, weights)

    def _set_weights_locked(self, route_id: str, weights: dict[str, int]) -> None:
        table: dict[str, int] = {}
        for upstream_id, weight in weights.items():
            w = int(weight)
            if w < 0:
                raise ValueError(f"weight for '{upstream_id}' must be >= 0, got {weight}")
            table[upstream_id] = w
        self._weights[route_id] = table
        # Reset the smooth-WRR scores so the new table starts from a clean,
        # deterministic state (config order decides the first pick).
        self._wrr_current[route_id] = {}

    def weights(self, route_id: str) -> dict[str, int] | None:
        """Current weight table (None when the route is not weighted)."""
        with self._lock:
            table = self._weights.get(route_id)
            return dict(table) if table is not None else None

    # ------------------------------------------------------------------ #
    # Selection
    # ------------------------------------------------------------------ #
    def select(
        self,
        rule: RouteRule,
        client_id: str,
        candidates: list[str],
        exclude: frozenset[str] = frozenset(),
    ) -> tuple[str | None, dict[str, Any]]:
        """Pick one upstream for this request; returns (id, explanation).

        ``candidates`` are the healthy upstreams admitted by the health
        registry (config order); ``exclude`` holds upstreams already tried
        by this request. An upstream with weight 0 is never eligible, so
        it receives no new business traffic while its health bookkeeping
        continues elsewhere.
        """
        now = self._clock()
        with self._lock:
            weights = self._weights.get(rule.id)
            ttl = self._sticky_ttl.get(rule.id, 0.0)
            eligible = [
                u for u in candidates
                if u not in exclude and (weights is None or weights.get(u, 0) > 0)
            ]
            explain: dict[str, Any] = {
                "policy": "gray",
                "client_id": client_id,
                "candidates": list(candidates),
                "excluded": sorted(exclude),
                "weights": dict(weights) if weights is not None else None,
                "eligible": eligible,
                "sticky_ttl_seconds": ttl,
            }
            if not eligible:
                explain["selected"] = None
                explain["reason"] = (
                    "all_weights_zero" if candidates and weights is not None
                    else "no_candidate_available"
                )
                return None, explain

            if ttl > 0:
                selected, reason = self._sticky_select_locked(
                    rule.id, client_id, eligible, weights, ttl, now, explain
                )
            else:
                selected = self._weighted_pick_locked(rule.id, eligible, weights, explain)
                reason = "weighted_pick"
            explain["reason"] = reason
            explain["selected"] = selected
            key = (rule.id, selected)
            self._served[key] = self._served.get(key, 0) + 1
            return selected, explain

    def _sticky_select_locked(
        self,
        route_id: str,
        client_id: str,
        eligible: list[str],
        weights: dict[str, int] | None,
        ttl: float,
        now: float,
        explain: dict[str, Any],
    ) -> tuple[str, str]:
        """Return (upstream, reason) honouring an existing pin when valid."""
        key = (route_id, client_id)
        entry = self._sticky.get(key)
        if entry is not None:
            if entry.upstream_id in eligible and entry.expires_at > now:
                # Refresh the pin: active clients stay put (sliding TTL),
                # which keeps one user on one version for the whole session.
                entry.expires_at = now + ttl
                explain["sticky"] = {
                    "pinned_to": entry.upstream_id,
                    "expires_in_seconds": round(entry.expires_at - now, 6),
                }
                return entry.upstream_id, "sticky_hit"
            if entry.upstream_id in eligible and entry.expires_at <= now:
                reason = "sticky_expired"
            else:
                # Target ejected, weight dropped to 0, or excluded by retry.
                reason = "sticky_target_ineligible"
        else:
            reason = "sticky_assign"
        selected = self._hash_pick(route_id, client_id, eligible, weights, explain)
        self._sticky[key] = _StickyEntry(upstream_id=selected, expires_at=now + ttl)
        explain["sticky"] = {"pinned_to": selected, "expires_in_seconds": ttl}
        return selected, reason

    def _hash_pick(
        self,
        route_id: str,
        client_id: str,
        eligible: list[str],
        weights: dict[str, int] | None,
        explain: dict[str, Any],
    ) -> str:
        """Deterministically map a client into the weight ranges.

        The same (route, client, weight table) always yields the same
        instance, so the assignment is reproducible and explainable.
        """
        digest = hashlib.sha256(f"{route_id}|{client_id}".encode("utf-8")).hexdigest()
        total = sum((weights or {}).get(u, 1) for u in eligible)
        bucket = int(digest, 16) % total
        acc = 0
        ranges: dict[str, list[int]] = {}
        selected = eligible[-1]
        for uid in eligible:
            w = (weights or {}).get(uid, 1)
            ranges[uid] = [acc, acc + w]
            if acc <= bucket < acc + w:
                selected = uid
            acc += w
        explain["hash"] = {
            "algorithm": "sha256(route_id|client_id) % total_weight",
            "bucket": bucket,
            "total_weight": total,
            "ranges": ranges,
        }
        return selected

    def _weighted_pick_locked(
        self,
        route_id: str,
        eligible: list[str],
        weights: dict[str, int] | None,
        explain: dict[str, Any],
    ) -> str:
        """Smooth weighted round robin (deterministic, reproducible)."""
        current = self._wrr_current.setdefault(route_id, {})
        effective = {u: (weights or {}).get(u, 1) for u in eligible}
        total = sum(effective.values())
        scores: dict[str, int] = {}
        for uid in eligible:
            scores[uid] = current.get(uid, 0) + effective[uid]
        # max() keeps first-seen on ties -> config order breaks ties.
        selected = max(eligible, key=lambda u: scores[u])
        scores[selected] -= total
        for uid in eligible:
            current[uid] = scores[uid]
        # Drop scores of upstreams no longer eligible so a returning
        # upstream restarts from 0 instead of a stale (possibly huge) score.
        for uid in list(current):
            if uid not in effective:
                del current[uid]
        explain["wrr"] = {
            "algorithm": "smooth_weighted_round_robin",
            "scores_after": dict(scores),
            "total_weight": total,
        }
        return selected

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict[str, Any]:
        """Weights, served-traffic distribution and sticky bookkeeping."""
        now = self._clock()
        with self._lock:
            route_ids = set(self._weights) | set(self._sticky_ttl)
            route_ids |= {rid for rid, _ in self._served}
            routes: dict[str, Any] = {}
            for route_id in sorted(route_ids):
                served = {
                    uid: n for (rid, uid), n in sorted(self._served.items()) if rid == route_id
                }
                active_pins = sum(
                    1
                    for (rid, _), entry in self._sticky.items()
                    if rid == route_id and entry.expires_at > now
                )
                routes[route_id] = {
                    "weights": (
                        dict(self._weights[route_id]) if route_id in self._weights else None
                    ),
                    "sticky_ttl_seconds": self._sticky_ttl.get(route_id, 0.0),
                    "sticky_active_pins": active_pins,
                    "served": served,
                    "served_total": sum(served.values()),
                }
            return {"routes": routes}
