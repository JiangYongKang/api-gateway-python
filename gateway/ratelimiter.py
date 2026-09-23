"""Per-client fixed-window rate limiting and concurrency control."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any

from gateway.config import ClientRateRule


@dataclass
class _ClientState:
    window_start: float = 0.0
    window_count: int = 0
    inflight: int = 0


class ClientLimiter:
    """Thread-safe per-client counters and concurrency slots.

    Both checks happen inside one lock acquisition, so concurrent requests
    always see consistent counts:
      * rate: fixed-window counter; the window rolls over deterministically
        at ``window_seconds`` boundaries measured from the first request;
      * concurrency: number of acquired-but-not-released slots.

    Rejected requests consume nothing (no counter slot, no concurrency slot),
    therefore a rejected request can never produce an upstream side effect.
    """

    def __init__(self, config: ClientRateRule, *, clock=time.monotonic) -> None:
        self._config = config
        self._clock = clock
        self._lock = threading.Lock()
        self._states: dict[str, _ClientState] = {}

    def _state(self, client_id: str) -> _ClientState:
        state = self._states.get(client_id)
        if state is None:
            state = _ClientState()
            self._states[client_id] = state
        return state

    def acquire(self, client_id: str, now: float | None = None) -> tuple[bool, str | None, dict[str, Any]]:
        """Return ``(allowed, reason, snapshot)``.

        ``reason`` is ``None`` when allowed, otherwise ``"rate"`` or
        ``"concurrency"``. Rate is evaluated first (client is over quota
        regardless of in-flight timing).
        """
        now = self._clock() if now is None else now
        cfg = self._config
        with self._lock:
            state = self._state(client_id)

            # Roll the fixed window if needed.
            if cfg.requests_per_window > 0 and (
                state.window_start == 0.0 or now - state.window_start >= cfg.window_seconds
            ):
                state.window_start = now
                state.window_count = 0

            snapshot = self._snapshot_locked(client_id, state)

            if cfg.requests_per_window > 0 and state.window_count >= cfg.requests_per_window:
                snapshot["decision"] = "rejected"
                snapshot["reason"] = "rate"
                return False, "rate", snapshot

            if cfg.max_concurrency > 0 and state.inflight >= cfg.max_concurrency:
                snapshot["decision"] = "rejected"
                snapshot["reason"] = "concurrency"
                return False, "concurrency", snapshot

            state.window_count += 1
            state.inflight += 1
            snapshot = self._snapshot_locked(client_id, state)
            snapshot["decision"] = "allowed"
            snapshot["reason"] = None
            return True, None, snapshot

    def release(self, client_id: str) -> None:
        with self._lock:
            state = self._states.get(client_id)
            if state is not None and state.inflight > 0:
                state.inflight -= 1

    def _snapshot_locked(self, client_id: str, state: _ClientState) -> dict[str, Any]:
        return {
            "client_id": client_id,
            "window_count": state.window_count,
            "window_start": round(state.window_start, 6),
            "inflight": state.inflight,
            "requests_per_window": self._config.requests_per_window,
            "window_seconds": self._config.window_seconds,
            "max_concurrency": self._config.max_concurrency,
        }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {cid: self._snapshot_locked(cid, st) for cid, st in self._states.items()}
