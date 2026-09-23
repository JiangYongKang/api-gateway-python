"""Configurable, explainable load balancing policies."""
from __future__ import annotations

import random
import threading
from typing import Any

from gateway.config import LBPolicy, RouteRule
from gateway.models import Request


class LoadBalancer:
    """Selects an upstream among the currently healthy candidates.

    Policies (per route):
      * ``round_robin``    – monotonically increasing counter modulo the
                             candidate count; deterministic.
      * ``random``         – uniform random choice; the drawn roll is logged
                             so the decision remains explainable.
      * ``least_inflight`` – candidate with the fewest in-flight requests;
                             ties broken by configuration order.

    State is keyed per route and guarded by one lock. Active health probes
    never call ``acquire``/``release`` and therefore never touch the
    in-flight counters.
    """

    def __init__(self, rng: random.Random | None = None) -> None:
        self._lock = threading.Lock()
        self._rr_counter: dict[str, int] = {}
        self._inflight: dict[tuple[str, str], int] = {}
        self._rng = rng or random.Random()

    def acquire(
        self,
        request: Request,
        rule: RouteRule,
        candidates: list[str],
        exclude: frozenset[str] = frozenset(),
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

            key = (rule.id, selected)
            self._inflight[key] = self._inflight.get(key, 0) + 1
            explain.update({
                "candidates": ordered,
                "excluded": sorted(exclude),
                "selected": selected,
            })
            return selected, explain

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
            }
