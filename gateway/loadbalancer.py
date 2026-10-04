"""Configurable, explainable load balancing policies."""
from __future__ import annotations

import random
import threading
from typing import Any

from gateway.config import LBPolicy, RouteRule
from gateway.models import Request
from gateway.weights import StickyAssigner, WeightManager


class LoadBalancer:
    """Selects an upstream among the currently healthy candidates.

    Policies (per route):
      * ``round_robin``    – monotonically increasing counter modulo the
                             candidate count; deterministic.
      * ``random``         – uniform random choice; the drawn roll is logged
                             so the decision remains explainable.
      * ``least_inflight`` – candidate with the fewest in-flight requests;
                             ties broken by configuration order.

    Gray release (only when the route has weights configured):
      * candidates with weight 0 never receive new business traffic;
      * selection is a deterministic hash-bucket pick over the weights
        (see :class:`gateway.weights.WeightManager`);
      * when ``rule.sticky_seconds > 0`` a client is pinned to its
        assigned instance for a sliding TTL (see
        :class:`gateway.weights.StickyAssigner`).

    Routes without weights take the legacy policy path above, byte for
    byte. State is keyed per route and guarded by one lock. Active health
    probes never call ``acquire``/``release`` and therefore never touch
    the in-flight counters.
    """

    def __init__(
        self,
        rng: random.Random | None = None,
        *,
        weights: WeightManager | None = None,
        sticky: StickyAssigner | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._rr_counter: dict[str, int] = {}
        self._inflight: dict[tuple[str, str], int] = {}
        self._traffic: dict[tuple[str, str], int] = {}
        self._rng = rng or random.Random()
        self._weights = weights
        self._sticky = sticky

    def acquire(
        self,
        request: Request,
        rule: RouteRule,
        candidates: list[str],
        exclude: frozenset[str] = frozenset(),
    ) -> tuple[str | None, dict[str, Any]]:
        weights = self._weights.weights_for(rule.id) if self._weights else None
        if weights is None:
            return self._acquire_legacy(request, rule, candidates, exclude)
        return self._acquire_weighted(request, rule, candidates, exclude, weights)

    # -- legacy path (routes without weights: behaviour unchanged) -------
    def _acquire_legacy(
        self,
        request: Request,
        rule: RouteRule,
        candidates: list[str],
        exclude: frozenset[str],
    ) -> tuple[str | None, dict[str, Any]]:
        with self._lock:
            ordered = [u for u in candidates if u not in exclude]
            if not ordered:
                return None, {
                    "policy": rule.lb_policy.value,
                    "candidates": candidates,
                    "excluded": sorted(exclude),
                    "selected": None,
                    "reason": "no_candidate_available",
                }

            if rule.lb_policy is LBPolicy.ROUND_ROBIN:
                idx = self._rr_counter.get(rule.id, 0)
                selected = ordered[idx % len(ordered)]
                self._rr_counter[rule.id] = idx + 1
                explain = {
                    "policy": "round_robin",
                    "counter": idx,
                    "candidate_count": len(ordered),
                    "position": idx % len(ordered),
                }
            elif rule.lb_policy is LBPolicy.RANDOM:
                position = self._rng.randrange(len(ordered))
                selected = ordered[position]
                explain = {
                    "policy": "random",
                    "roll": position,
                    "candidate_count": len(ordered),
                }
            else:  # LEAST_INFLIGHT
                counts = {u: self._inflight.get((rule.id, u), 0) for u in ordered}
                # min with stable order -> ties resolved by config order.
                selected = min(ordered, key=lambda u: counts[u])
                explain = {
                    "policy": "least_inflight",
                    "inflight": dict(counts),
                    "selected_count": counts[selected],
                }

            self._account_locked(rule.id, selected)
            explain.update({
                "candidates": ordered,
                "excluded": sorted(exclude),
                "selected": selected,
            })
            return selected, explain

    # -- gray-release path (route has weights configured) ----------------
    def _acquire_weighted(
        self,
        request: Request,
        rule: RouteRule,
        candidates: list[str],
        exclude: frozenset[str],
        weights: dict[str, int],
    ) -> tuple[str | None, dict[str, Any]]:
        assert self._weights is not None
        # Weight 0 (or missing entry) drains the instance: no new traffic,
        # while health maintenance keeps running elsewhere. ``exclude``
        # (already-tried upstreams of this request) is honoured as usual.
        eligible = [
            u for u in candidates if u not in exclude and weights.get(u, 0) > 0
        ]
        base_explain: dict[str, Any] = {
            "gray": True,
            "policy": "weighted",
            "candidates": candidates,
            "excluded": sorted(exclude),
            "eligible": eligible,
        }
        if not eligible:
            return None, {
                **base_explain,
                "weights": {u: weights.get(u, 0) for u in candidates},
                "selected": None,
                "reason": "all_weights_zero",
            }

        if rule.sticky_seconds > 0 and self._sticky is not None:
            eligible_set = frozenset(eligible)
            selected, explain = self._sticky.lookup_or_assign(
                rule.id,
                request.client_id,
                rule.sticky_seconds,
                is_valid=lambda u: u in eligible_set,
                choose=lambda: self._weights.pick(rule.id, request.client_id, eligible),
            )
        else:
            selected, explain = self._weights.pick(rule.id, request.client_id, eligible)
        explain = {**base_explain, **explain}
        if selected is None:
            return None, explain

        with self._lock:
            self._account_locked(rule.id, selected)
        return selected, explain

    # -- shared accounting -------------------------------------------------
    def _account_locked(self, rule_id: str, upstream_id: str) -> None:
        key = (rule_id, upstream_id)
        self._inflight[key] = self._inflight.get(key, 0) + 1
        self._traffic[key] = self._traffic.get(key, 0) + 1

    def release(self, rule_id: str, upstream_id: str) -> None:
        with self._lock:
            key = (rule_id, upstream_id)
            current = self._inflight.get(key, 0)
            if current > 0:
                self._inflight[key] = current - 1

    def inflight(self, rule_id: str, upstream_id: str) -> int:
        with self._lock:
            return self._inflight.get((rule_id, upstream_id), 0)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "rr_counters": dict(self._rr_counter),
                "inflight": {f"{rule_id}:{upstream_id}": n for (rule_id, upstream_id), n in self._inflight.items()},
                "traffic": {f"{rule_id}:{upstream_id}": n for (rule_id, upstream_id), n in self._traffic.items()},
            }
