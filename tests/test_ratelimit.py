"""Per-client rate limiting and concurrency control, including concurrency
consistency under many simultaneous acquirers."""
from __future__ import annotations

import threading

import pytest

from gateway.config import ClientRateRule
from gateway.ratelimiter import ClientLimiter


class FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@pytest.fixture
def clock():
    return FakeClock()


def test_fixed_window_rate_limit(clock):
    limiter = ClientLimiter(
        ClientRateRule(requests_per_window=3, window_seconds=2.0), clock=clock
    )
    results = [limiter.acquire("c1")[0] for _ in range(3)]
    assert results == [True, True, True]
    allowed, reason, snap = limiter.acquire("c1")
    assert allowed is False and reason == "rate"
    assert snap["window_count"] == 3

    # window rolls over deterministically
    clock.advance(1.9)
    assert limiter.acquire("c1")[1] == "rate"
    clock.advance(0.1)
    allowed, reason, _ = limiter.acquire("c1")
    assert allowed is True and reason is None


def test_clients_are_isolated(clock):
    limiter = ClientLimiter(
        ClientRateRule(requests_per_window=1, window_seconds=1.0), clock=clock
    )
    assert limiter.acquire("a")[0] is True
    assert limiter.acquire("b")[0] is True
    assert limiter.acquire("a")[1] == "rate"
    assert limiter.acquire("b")[1] == "rate"


def test_concurrency_limit_and_release(clock):
    limiter = ClientLimiter(
        ClientRateRule(requests_per_window=0, max_concurrency=2), clock=clock
    )
    assert limiter.acquire("c")[1] is None
    assert limiter.acquire("c")[1] is None
    allowed, reason, _ = limiter.acquire("c")
    assert allowed is False and reason == "concurrency"
    limiter.release("c")
    assert limiter.acquire("c")[1] is None


def test_rejected_request_consumes_nothing(clock):
    limiter = ClientLimiter(
        ClientRateRule(requests_per_window=1, max_concurrency=1), clock=clock
    )
    assert limiter.acquire("c")[0] is True
    # both limits hit; rejected attempts must not change counters
    for _ in range(10):
        _, reason, _ = limiter.acquire("c")
        assert reason in {"rate", "concurrency"}
    limiter.release("c")
    clock.advance(1.0)
    assert limiter.acquire("c")[0] is True


def test_concurrent_acquires_never_exceed_limit(clock):
    """50 threads x 40 acquires with limit 400 -> exactly 400 allowed."""
    limiter = ClientLimiter(
        ClientRateRule(requests_per_window=400, window_seconds=60.0,
                       max_concurrency=400),
        clock=clock,
    )
    allowed_count = 0
    denied_count = 0
    lock = threading.Lock()

    def worker():
        nonlocal allowed_count, denied_count
        got, _, _ = limiter.acquire("hot-client")
        with lock:
            if got:
                allowed_count += 1
            else:
                denied_count += 1
        if got:
            limiter.release("hot-client")

    threads = [threading.Thread(target=worker) for _ in range(50 * 40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert allowed_count == 400
    assert denied_count == 50 * 40 - 400
    # every slot released
    assert limiter.snapshot()["hot-client"]["inflight"] == 0


def test_concurrency_peak_under_parallel_load(clock):
    """With blocking workers, observed in-flight never exceeds max."""
    limiter = ClientLimiter(
        ClientRateRule(requests_per_window=10_000, max_concurrency=8),
        clock=clock,
    )
    peak = 0
    seen_lock = threading.Lock()
    barrier = threading.Barrier(32)

    def worker():
        barrier.wait()
        got, _, _ = limiter.acquire("c")
        if got:
            nonlocal peak
            with seen_lock:
                inflight = limiter.snapshot()["c"]["inflight"]
                peak = max(peak, inflight)
            limiter.release("c")

    threads = [threading.Thread(target=worker) for _ in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert peak <= 8
