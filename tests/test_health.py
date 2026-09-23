"""Passive ejection, deterministic probe recovery and traffic isolation."""
from __future__ import annotations

import pytest

from gateway.config import RouteRule, UpstreamConfig
from gateway.health import HealthRegistry, HealthState


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def upstreams():
    return (
        UpstreamConfig(id="a", base_url="stub://a"),
        UpstreamConfig(id="b", base_url="stub://b"),
    )


def make_rule(**kw):
    defaults = {
        "id": "r", "prefix": "/v1", "upstream_ids": ("a", "b"),
        "failure_threshold": 3, "open_state_seconds": 5.0,
        "half_open_max_probe_requests": 1, "half_open_success_threshold": 2,
        "probe_interval_seconds": 1.0, "probe_path": "/health",
    }
    defaults.update(kw)
    return RouteRule(**defaults)


def test_consecutive_failures_eject_and_success_resets(clock, upstreams):
    h = HealthRegistry(upstreams, clock=clock)
    rule = make_rule()
    h.record_business_result(rule, "a", ok=False)
    h.record_business_result(rule, "a", ok=False)
    assert h.healthy_candidates(rule) == ["a", "b"]
    h.record_business_result(rule, "a", ok=False)
    assert h.status("a").state is HealthState.EJECTED
    assert h.healthy_candidates(rule) == ["b"]
    # ejected upstream never receives traffic even as b also fails
    h.record_business_result(rule, "b", ok=False)
    assert h.healthy_candidates(rule) == ["b"]


def test_recovery_requires_deterministic_probe_sequence(clock, upstreams):
    h = HealthRegistry(upstreams, clock=clock)
    rule = make_rule(half_open_success_threshold=2)
    for _ in range(3):
        h.record_business_result(rule, "a", ok=False)
    assert h.status("a").state is HealthState.EJECTED

    # no probe before cool-down
    clock.advance(4.9)
    assert h.on_probe_tick() == []
    clock.advance(0.2)  # cool-down elapsed -> half-open, first probe due
    jobs = h.on_probe_tick()
    assert [j[0] for j in jobs] == ["a"]
    assert jobs[0][1] == "/health"
    # no duplicate job while one is in flight, and no business traffic yet
    assert h.on_probe_tick() == []
    assert h.healthy_candidates(rule) == ["b"]

    h.record_probe_result("r", "a", ok=True)
    # need 2 consecutive successes; next probe only after probe interval
    assert h.status("a").state is HealthState.PROBING
    assert h.on_probe_tick() == []
    clock.advance(1.0)
    assert [j[0] for j in h.on_probe_tick()] == ["a"]
    h.record_probe_result("r", "a", ok=True)
    assert h.status("a").state is HealthState.HEALTHY
    assert h.healthy_candidates(rule) == ["a", "b"]


def test_failed_probe_reopens_circuit(clock, upstreams):
    h = HealthRegistry(upstreams, clock=clock)
    rule = make_rule(half_open_success_threshold=1)
    for _ in range(3):
        h.record_business_result(rule, "a", ok=False)
    clock.advance(5.0)
    h.on_probe_tick()
    h.record_probe_result("r", "a", ok=False)
    assert h.status("a").state is HealthState.EJECTED
    assert h.healthy_candidates(rule) == ["b"]
    # cool-down restarts: still no probe
    clock.advance(4.9)
    assert h.on_probe_tick() == []


def test_half_open_limited_trial_capacity(clock, upstreams):
    """half_open_max_probe_requests bounds simultaneous trial requests."""
    h = HealthRegistry(upstreams, clock=clock)
    rule = make_rule(half_open_max_probe_requests=2,
                      half_open_success_threshold=1)
    for _ in range(3):
        h.record_business_result(rule, "a", ok=False)
    clock.advance(5.0)
    # business trial requests also consume half-open capacity
    admitted = h.healthy_candidates(rule)
    assert admitted.count("a") == 1
    second = h.healthy_candidates(rule)
    assert second.count("a") == 1
    third = h.healthy_candidates(rule)
    assert third == ["b"]  # capacity 2 exhausted
    h.record_business_result(rule, "a", ok=True)
    h.record_business_result(rule, "a", ok=True)
    # threshold for close is probe-success based (2); business success in
    # half-open also counts deterministically toward recovery:
    assert h.status("a").state is HealthState.HEALTHY


def test_probe_path_does_not_touch_business_counters(clock, upstreams):
    """Active probes bypass LB slots: only business traffic inflates counts."""
    from gateway.loadbalancer import LoadBalancer

    h = HealthRegistry(upstreams, clock=clock)
    lb = LoadBalancer()
    rule = make_rule()
    from gateway.models import Request

    uid, _ = lb.acquire(Request("GET", "/v1"), rule, ["a", "b"])
    assert lb.inflight("r", uid) == 1
    # probe activity happens while business request in flight
    clock.advance(0.0)
    jobs = h.on_probe_tick()  # healthy -> no jobs
    assert jobs == []
    assert lb.inflight("r", uid) == 1
    lb.release("r", uid)
    assert lb.inflight("r", uid) == 0


def test_all_unavailable_when_everything_ejected(clock, upstreams):
    h = HealthRegistry(upstreams, clock=clock)
    rule = make_rule()
    for uid in ("a", "b"):
        for _ in range(3):
            h.record_business_result(rule, uid, ok=False)
    assert h.healthy_candidates(rule) == []
    snap = h.snapshot()
    assert snap["a"]["state"] == "EJECTED" and snap["b"]["state"] == "EJECTED"
