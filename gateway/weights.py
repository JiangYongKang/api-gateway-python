"""Weighted gray-release splitting and client stickiness.

Two cooperating pieces, both thread-safe and fully explainable:

* :class:`WeightManager` – per-route upstream weights, adjustable at
  runtime without restart. Weight 0 means "no new business traffic"
  while health maintenance (ejection/probing) keeps running, so an
  instance can be drained temporarily or ramped up gradually. Selection
  is a deterministic hash-bucket map: ``sha256(key) mod total_weight``
  picks a slot inside per-instance cumulative ranges, so every decision
  is reproducible and explainable (hash, bucket, ranges are all logged).

* :class:`StickyAssigner` – per (route, client) instance assignment
  with a sliding TTL. Lookup + (re)assignment happen atomically under
  one lock, so concurrent requests from one client can never land on
  two different instances. An assignment is reused only while it is
  unexpired *and* its target is still eligible (healthy, weight > 0);
  otherwise a deterministic reassignment happens. A recovered instance
  never steals clients back before their current assignment expires,
  so recovery causes no traffic flip-flop.

Lock ordering: StickyAssigner's lock may be held while calling into
WeightManager (``choose`` callback); WeightManager never calls back,
so the order sticky -> weights is deadlock-free.
"""
from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Callable
from typing import Any


def _stable_hash(key: str) -> int:
    """Process-independent hash (unlike builtin ``hash`` for str)."""
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


class WeightManager:
    """Runtime-adjustable per-route upstream weights.

    A route only appears here once weights are configured (statically via
    ``RouteRule.weights`` or dynamically via :meth:`set_weights`). Routes
    without an entry must be handled by the caller exactly as before this
    feature existed. When a route *has* weights, an upstream missing from
    the map is treated as weight 0 (explicit opt-in).
    """

    def __init__(
        self,
        initial: dict[str, dict[str, int]] | None = None,
        *,
        clock=time.monotonic,
    ) -> None:
        self._lock = threading.Lock()
        self._clock = clock
        self._weights: dict[str, dict[str, int]] = {}
        self._version: dict[str, int] = {}
        for route_id, weights in (initial or {}).items():
            self.set_weights(route_id, weights)

    @staticmethod
    def _validate(weights: dict[str, int]) -> None:
        for uid, w in weights.items():
            if not isinstance(w, int) or isinstance(w, bool) or w < 0:
                raise ValueError(
                    f"weight for upstream '{uid}' must be a non-negative int, got {w!r}"
                )

    def weights_for(self, route_id: str) -> dict[str, int] | None:
        """Current weights for a route, or None when never configured."""
        with self._lock:
            weights = self._weights.get(route_id)
            return dict(weights) if weights is not None else None

    def set_weights(self, route_id: str, weights: dict[str, int]) -> dict[str, Any]:
        """Atomically replace a route's weights; returns an explanation.

        Takes effect for *new* selections only: in-flight requests already
        hold their instance, and sticky clients keep their assignment
        while it stays valid (weight > 0, healthy, unexpired).
        """
        self._validate(weights)
        with self._lock:
            old = self._weights.get(route_id)
            self._weights[route_id] = dict(weights)
            self._version[route_id] = self._version.get(route_id, 0) + 1
            return {
                "route_id": route_id,
                "previous": old,
                "weights": dict(weights),
                "total_weight": sum(weights.values()),
                "version": self._version[route_id],
            }

    def pick(
        self, route_id: str, key: str, candidates: list[str]
    ) -> tuple[str | None, dict[str, Any]]:
        """Deterministically pick one candidate by weight-bucketed hash.

        ``candidates`` must already be health-filtered; this method only
        applies the weight map. The same ``key`` always lands on the same
        instance as long as weights and the candidate set are unchanged.
        """
        with self._lock:
            weights = self._weights.get(route_id)
            if weights is None:
                return None, {
                    "policy": "weighted",
                    "reason": "weights_not_configured",
                    "route_id": route_id,
                }
            eligible = [u for u in candidates if weights.get(u, 0) > 0]
            skipped = [u for u in candidates if weights.get(u, 0) <= 0]
            total = sum(weights[u] for u in eligible)
            base_explain: dict[str, Any] = {
                "policy": "weighted",
                "route_id": route_id,
                "weights": {u: weights.get(u, 0) for u in candidates},
                "weight_zero_skipped": skipped,
                "total_weight": total,
                "version": self._version.get(route_id, 0),
            }
            if not eligible or total <= 0:
                return None, {**base_explain, "selected": None, "reason": "all_weights_zero"}
            bucket = _stable_hash(f"{route_id}::{key}") % total
            ranges: dict[str, list[int]] = {}
            lo = 0
            selected = eligible[-1]
            for uid in eligible:
                hi = lo + weights[uid]
                ranges[uid] = [lo, hi]
                if lo <= bucket < hi:
                    selected = uid
                lo = hi
            return selected, {
                **base_explain,
                "hash": _stable_hash(f"{route_id}::{key}"),
                "bucket": bucket,
                "ranges": ranges,
                "selected": selected,
            }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                route_id: {
                    "weights": dict(weights),
                    "total_weight": sum(weights.values()),
                    "version": self._version.get(route_id, 0),
                }
                for route_id, weights in self._weights.items()
            }


class StickyAssigner:
    """TTL-bound per-(route, client) upstream assignments.

    The whole lookup/validate/(re)assign sequence runs under one lock,
    which is what guarantees that concurrent requests of one client
    observe exactly one assignment.
    """

    def __init__(self, *, clock=time.monotonic) -> None:
        self._lock = threading.Lock()
        self._clock = clock
        # (route_id, client_id) -> (upstream_id, expires_at)
        self._entries: dict[tuple[str, str], tuple[str, float]] = {}

    def lookup_or_assign(
        self,
        route_id: str,
        client_id: str,
        ttl_seconds: float,
        is_valid: Callable[[str], bool],
        choose: Callable[[], tuple[str | None, dict[str, Any]]],
    ) -> tuple[str | None, dict[str, Any]]:
        """Return the sticky upstream, (re)assigning atomically if needed.

        ``is_valid(upstream)`` must reflect *current* eligibility (healthy
        candidate and weight > 0). ``choose()`` performs a fresh weighted
        pick and is only called when a new assignment is needed.
        """
        now = self._clock()
        key = (route_id, client_id)
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                upstream, expires_at = entry
                if now < expires_at and is_valid(upstream):
                    # Sliding TTL: an active client keeps its instance.
                    self._entries[key] = (upstream, now + ttl_seconds)
                    return upstream, {
                        "sticky": "hit",
                        "sticky_upstream": upstream,
                        "sticky_expires_in": round(expires_at - now, 6),
                    }
                reason = "expired" if now >= expires_at else "target_ineligible"
                self._entries.pop(key, None)
            else:
                reason = "new"

            selected, info = choose()
            if selected is None:
                return None, {"sticky": reason, "sticky_upstream": None, **info}
            self._entries[key] = (selected, now + ttl_seconds)
            return selected, {
                "sticky": reason,
                "sticky_upstream": selected,
                "sticky_ttl": ttl_seconds,
                **info,
            }

    def snapshot(self) -> dict[str, Any]:
        now = self._clock()
        with self._lock:
            return {
                f"{route_id}:{client_id}": {
                    "upstream": upstream,
                    "expires_in": round(expires_at - now, 6),
                }
                for (route_id, client_id), (upstream, expires_at) in self._entries.items()
            }
