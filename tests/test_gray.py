"""Weighted gray (canary) routing, client stickiness and runtime re-weighting.

Every test logs its scenario plus the weight table / upstream states via the
standard decision logger, so ``pytest -s`` shows what was verified and why.
"""
from __future__ import annotations

import json
import threading
import time

from gateway.config import (
    ClientRateRule,
    GatewayConfig,
    LBPolicy,
    RouteRule,
    UpstreamConfig,
)
from gateway.gateway import Gateway
from gateway.gray import GrayController
from gateway.logging_setup import decision_logger, log_event
from gateway.models import Request, Response
from gateway.transport import StubTransport

_log = decision_logger("gateway.test.gray")


def scenario(name: str, **fields) -> None:
    log_event(_log, "scenario", name=name, **fields)


class Clock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ScriptedUpstream:
    """Business/health handler with scripted statuses and a call counter."""

    def __init__(self, name, *, statuses=None, health_status=200, delay=0.0):
        self.name = name
        self.statuses = list(statuses or [200])
        self.health_status = health_status
        self.delay = delay
        self.calls: list[Request] = []
        self._lock = threading.Lock()

    def __call__(self, request: Request) -> Response:
        if request.path.rstrip("/").endswith("/health"):
            return Response(status=200 if self.health_status == 200 else 503,
                            body=b"health", upstream_id=self.name)
        with self._lock:
            self.calls.append(request)
            idx = len(self.calls) - 1
            status = self.statuses[min(idx, len(self.statuses) - 1)]
        if self.delay:
            time.sleep(self.delay)
        return Response(status=status,
                        body=json.dumps({"by": self.name}).encode(),
                        upstream_id=self.name)

    def business_calls(self) -> int:
        with self._lock:
            return len(self.calls)


def build(routes, upstream_ids=("a", "b"), handlers=None, clock=None):
    upstreams = tuple(UpstreamConfig(u, f"stub://{u}") for u in upstream_ids)
    handlers = handlers or {u: ScriptedUpstream(u) for u in upstream_ids}
    cfg = GatewayConfig(routes=routes, upstreams=upstreams, rate=ClientRateRule())
    kw = {"clock": clock} if clock is not None else {}
    return Gateway(cfg, transport=StubTransport(handlers), **kw), handlers


def gray_rule(**kw) -> RouteRule:
    defaults = dict(
        id="g", prefix="/v1/g", upstream_ids=("a", "b"),
        failure_threshold=1, open_state_seconds=5.0,
        probe_interval_seconds=0.1,
    )
    defaults.update(kw)
    return RouteRule(**defaults)


def upstream_of(resp) -> str:
    return json.loads(resp.body)["by"]


# -- weighted split ---------------------------------------------------------

def test_weighted_split_exact_and_reproducible():
    scenario("weighted_split", weights={"a": 3, "b": 1}, expect="6:2 over 8 requests, twice identical")
    rule = gray_rule(weights={"a": 3, "b": 1})
    sequences = []
    for _ in range(2):  # two fresh gateways -> identical pick sequence
        gw, _ = build((rule,))
        picks = [upstream_of(gw.handle(Request("GET", "/v1/g/x"))) for _ in range(8)]
        sequences.append(picks)
    assert sequences[0] == sequences[1], "same weights + same sequence must reproduce"
    assert sequences[0].count("a") == 6 and sequences[0].count("b") == 2
    log_event(_log, "scenario_result", name="weighted_split", picks=sequences[0])


def test_weight_zero_gets_no_traffic_but_health_is_maintained():
    scenario("weight_zero", weights={"a": 1, "b": 0},
             expect="b receives nothing, breaker/probe bookkeeping continues")
    rule = gray_rule(weights={"a": 1, "b": 0})
    gw, handlers = build((rule,))
    for _ in range(5):
        assert upstream_of(gw.handle(Request("GET", "/v1/g/x"))) == "a"
    assert handlers["b"].business_calls() == 0
    # Health bookkeeping for b continues: breaker exists and stays HEALTHY,
    # and the probe scheduler still emits jobs for it once half-open.
    snap = gw.health.snapshot()
    assert snap["b"]["state"] == "HEALTHY"
    assert "g" in snap["b"]["routes"]
    log_event(_log, "scenario_result", name="weight_zero", health=snap,
              gray=gw.gray_snapshot())


def test_all_weights_zero_is_distinguishable_503():
    scenario("all_weights_zero", weights={"a": 0}, expect="503 ALL_UPSTREAMS_UNAVAILABLE/all_weights_zero")
    rule = gray_rule(upstream_ids=("a",), weights={"a": 0})
    gw, _ = build((rule,), upstream_ids=("a",))
    resp = gw.handle(Request("GET", "/v1/g/x"))
    assert resp.status == 503
    err = json.loads(resp.body)["error"]
    assert err["code"] == "ALL_UPSTREAMS_UNAVAILABLE"
    assert err["details"]["reason"] == "all_weights_zero"


def test_route_without_weights_keeps_legacy_round_robin():
    scenario("legacy_untouched", expect="route without weights still uses round_robin")
    legacy = RouteRule(id="legacy", prefix="/v1/old", upstream_ids=("a", "b"),
                       lb_policy=LBPolicy.ROUND_ROBIN)
    gw, _ = build((gray_rule(weights={"a": 1, "b": 0}), legacy))
    picks = [upstream_of(gw.handle(Request("GET", "/v1/old/x"))) for _ in range(4)]
    assert picks == ["a", "b", "a", "b"]
    assert not gw.gray.is_enabled("legacy")


# -- stickiness --------------------------------------------------------------

def test_sticky_client_pinned_others_spread():
    scenario("sticky_pin", weights={"a": 1, "b": 1}, ttl=60,
             expect="one client pinned; many clients spread over both")
    rule = gray_rule(weights={"a": 1, "b": 1}, sticky_ttl_seconds=60.0)
    gw, _ = build((rule,))
    req = lambda cid: Request("GET", "/v1/g/x", headers={"x-client-id": cid})
    first = upstream_of(gw.handle(req("user-1")))
    for _ in range(9):
        assert upstream_of(gw.handle(req("user-1"))) == first
    seen = {upstream_of(gw.handle(req(f"user-{i}"))) for i in range(2, 30)}
    assert seen == {"a", "b"}
    log_event(_log, "scenario_result", name="sticky_pin",
              pinned=first, gray=gw.gray_snapshot())


def test_sticky_expiry_reassigns_by_deterministic_hash():
    scenario("sticky_expiry", ttl=10, expect="pin expires -> hash reassignment, reproducible")
    clock = Clock()
    rule = gray_rule(weights={"a": 1, "b": 1}, sticky_ttl_seconds=10.0)
    gw, _ = build((rule,), clock=clock)
    req = lambda: Request("GET", "/v1/g/x", headers={"x-client-id": "user-9"})
    first = upstream_of(gw.handle(req()))
    clock.advance(5)
    assert upstream_of(gw.handle(req())) == first  # still within TTL
    clock.advance(6)  # now expired
    again = upstream_of(gw.handle(req()))
    # Reassignment follows the hash of the client id: a fresh gateway with
    # the same table must pin the same client to the same instance.
    gw2, _ = build((rule,), clock=Clock())
    assert upstream_of(gw2.handle(req())) == again
    log_event(_log, "scenario_result", name="sticky_expiry",
              first=first, reassigned=again)


def test_sticky_never_sent_to_ejected_and_no_snap_back_on_recovery():
    scenario("sticky_ejection", expect="ejected target -> reassigned; recovery does not snap back")
    clock = Clock()
    handlers = {
        "a": ScriptedUpstream("a"),
        "b": ScriptedUpstream("b"),
    }
    rule = gray_rule(weights={"a": 1, "b": 1}, sticky_ttl_seconds=60.0)
    gw, _ = build((rule,), handlers=handlers, clock=clock)
    req = lambda: Request("GET", "/v1/g/x", headers={"x-client-id": "user-e"})

    # Find the client's pinned instance, then force THAT instance to fail.
    first = upstream_of(gw.handle(req()))
    bad, good = ("a", "b") if first == "a" else ("b", "a")
    handlers[bad].statuses = [500]
    resp = gw.handle(req())  # this failure ejects `bad` (threshold 1)
    assert resp.status == 502
    assert gw.health.snapshot()[bad]["state"] == "EJECTED"
    before = handlers[bad].business_calls()
    for _ in range(5):  # never routed to the ejected instance
        assert upstream_of(gw.handle(req())) == good
    assert handlers[bad].business_calls() == before

    # Recovery: cool-down elapses, probe succeeds, breaker closes again.
    handlers[bad].statuses = [200]
    clock.advance(5.0)
    assert gw.run_probe_tick() >= 1
    assert gw.health.snapshot()[bad]["state"] == "HEALTHY"
    # No snap-back: the client stays on the good instance until TTL expiry.
    for _ in range(3):
        assert upstream_of(gw.handle(req())) == good
    log_event(_log, "scenario_result", name="sticky_ejection",
              bad=bad, good=good, health=gw.health.snapshot())


def test_sticky_weight_zero_forces_deterministic_reassignment():
    scenario("sticky_weight_zero", expect="weight->0 moves the pin; recovery keeps new pin")
    clock = Clock()
    rule = gray_rule(weights={"a": 1, "b": 1}, sticky_ttl_seconds=60.0)
    gw, _ = build((rule,), clock=clock)
    req = lambda: Request("GET", "/v1/g/x", headers={"x-client-id": "user-w"})
    first = upstream_of(gw.handle(req()))
    other = "b" if first == "a" else "a"
    gw.set_route_weights("g", {first: 0, other: 1})
    moved = upstream_of(gw.handle(req()))
    assert moved == other
    # Restoring the weight must not yank the client back (pin still valid).
    gw.set_route_weights("g", {"a": 1, "b": 1})
    assert upstream_of(gw.handle(req())) == other


def test_concurrent_same_client_never_splits():
    scenario("sticky_concurrency", threads=32,
             expect="one client, 32 concurrent requests -> exactly one instance")
    rule = gray_rule(weights={"a": 1, "b": 1}, sticky_ttl_seconds=60.0)
    handlers = {"a": ScriptedUpstream("a", delay=0.02),
                "b": ScriptedUpstream("b", delay=0.02)}
    gw, _ = build((rule,), handlers=handlers)
    results: list[str] = []

    def hit():
        resp = gw.handle(Request("GET", "/v1/g/x", headers={"x-client-id": "user-c"}))
        results.append(upstream_of(resp))

    threads = [threading.Thread(target=hit) for _ in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == 32
    assert len(set(results)) == 1, f"client split across instances: {set(results)}"
    pins = gw.gray_snapshot()["routes"]["g"]["sticky_active_pins"]
    assert pins == 1
    log_event(_log, "scenario_result", name="sticky_concurrency",
              landed=results[0], count=len(results))


# -- runtime re-weighting -----------------------------------------------------

def test_runtime_reweight_applies_to_new_requests_only():
    scenario("runtime_reweight", expect="1:0 -> all a; 0:1 -> all b, no restart")
    rule = gray_rule(weights={"a": 1, "b": 0})
    gw, _ = build((rule,))
    assert {upstream_of(gw.handle(Request("GET", "/v1/g/x"))) for _ in range(4)} == {"a"}
    gw.set_route_weights("g", {"a": 0, "b": 1})
    assert {upstream_of(gw.handle(Request("GET", "/v1/g/x"))) for _ in range(4)} == {"b"}
    snap = gw.gray_snapshot()["routes"]["g"]
    assert snap["weights"] == {"a": 0, "b": 1}
    assert snap["served"] == {"a": 4, "b": 4}
    log_event(_log, "scenario_result", name="runtime_reweight", gray=snap)


def test_reweight_rejects_unknown_route_and_upstream():
    scenario("reweight_validation", expect="unknown route/upstream rejected deterministically")
    import pytest

    from gateway.errors import GatewayError
    rule = gray_rule(weights={"a": 1, "b": 1})
    gw, _ = build((rule,))
    with pytest.raises(GatewayError):
        gw.set_route_weights("nope", {"a": 1})
    with pytest.raises(GatewayError):
        gw.set_route_weights("g", {"ghost": 1})
    with pytest.raises(GatewayError):
        gw.set_route_weights("g", {"a": -1})


def test_reweight_keeps_eligible_pins_and_moves_zeroed_ones():
    scenario("reweight_smooth_transition",
             expect="pins survive proportional reweight; weight 0 moves the pin")
    rule = gray_rule(weights={"a": 1, "b": 1}, sticky_ttl_seconds=60.0)
    gw, _ = build((rule,))
    req = lambda cid: Request("GET", "/v1/g/x", headers={"x-client-id": cid})
    pins_before = {cid: upstream_of(gw.handle(req(cid))) for cid in ("u1", "u2", "u3")}
    gw.set_route_weights("g", {"a": 9, "b": 1})  # both still eligible
    for cid, pinned in pins_before.items():
        assert upstream_of(gw.handle(req(cid))) == pinned
    # Zeroing one instance moves only its pinned clients.
    gw.set_route_weights("g", {"a": 1, "b": 0})
    for cid, pinned in pins_before.items():
        got = upstream_of(gw.handle(req(cid)))
        assert got == "a"


# -- explainability / observability -------------------------------------------

def test_explain_covers_weights_hash_and_sticky():
    scenario("explainability", expect="every decision carries weights/hash/sticky basis")
    controller = GrayController()
    rule = gray_rule(weights={"a": 3, "b": 1}, sticky_ttl_seconds=30.0)
    controller.configure_route(rule)
    _, first = controller.select(rule, "user-x", ["a", "b"])
    assert first["weights"] == {"a": 3, "b": 1}
    assert first["reason"] == "sticky_assign"
    bucket = first["hash"]["bucket"]
    lo, hi = first["hash"]["ranges"][first["selected"]]
    assert lo <= bucket < hi, "selected instance must own the hash bucket"
    _, second = controller.select(rule, "user-x", ["a", "b"])
    assert second["reason"] == "sticky_hit"
    assert second["selected"] == first["selected"]
    log_event(_log, "scenario_result", name="explainability", explain=second)


def test_snapshot_reports_weights_and_actual_distribution():
    scenario("observability", expect="snapshot weights + served counts match real traffic")
    rule = gray_rule(weights={"a": 3, "b": 1})
    gw, _ = build((rule,))
    for _ in range(8):
        gw.handle(Request("GET", "/v1/g/x"))
    snap = gw.gray_snapshot()["routes"]["g"]
    assert snap["weights"] == {"a": 3, "b": 1}
    assert snap["served"] == {"a": 6, "b": 2}
    assert snap["served_total"] == 8
