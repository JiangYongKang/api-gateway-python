"""Gray release through the full gateway pipeline (stub transport).

Scenarios: weighted split, weight-0 draining with health maintenance,
client stickiness (hit / expiry / ejection / drain / recovery),
runtime weight adjustment, and legacy-route compatibility.
"""
from __future__ import annotations

import json
import threading
from collections import Counter

import pytest

from gateway.config import (
    ClientRateRule,
    GatewayConfig,
    LBPolicy,
    RouteRule,
    UpstreamConfig,
)
from gateway.errors import GatewayError
from gateway.gateway import Gateway
from gateway.models import Request, Response
from gateway.transport import StubTransport


class FakeClock:
    def __init__(self, start=1000.0):
        self.t = start
        self._lock = threading.Lock()

    def __call__(self):
        with self._lock:
            return self.t

    def advance(self, dt):
        with self._lock:
            self.t += dt


def _echo(uid):
    def handler(request: Request) -> Response:
        return Response(
            200,
            headers={"content-type": "application/json"},
            body=json.dumps({"upstream": uid}).encode(),
        )

    return handler


def make_gateway(*, weights=None, sticky_seconds=0.0, clock=None, max_retries=0):
    """Two-upstream gray route + one legacy round-robin route."""
    clock = clock or FakeClock()
    routes = [
        RouteRule(
            id="gray",
            prefix="/v1/gray",
            upstream_ids=("old", "new"),
            lb_policy=LBPolicy.ROUND_ROBIN,
            weights=weights,
            sticky_seconds=sticky_seconds,
            timeout_seconds=1.0,
            max_retries=max_retries,
            failure_threshold=2,
            open_state_seconds=5.0,
        ),
        RouteRule(
            id="legacy",
            prefix="/v1/legacy",
            upstream_ids=("old", "new"),
            lb_policy=LBPolicy.ROUND_ROBIN,
            timeout_seconds=1.0,
        ),
    ]
    cfg = GatewayConfig(
        routes=tuple(routes),
        upstreams=(
            UpstreamConfig("old", "stub://old"),
            UpstreamConfig("new", "stub://new"),
        ),
        rate=ClientRateRule(),  # unlimited
    )
    transport = StubTransport({"old": _echo("old"), "new": _echo("new")})
    return Gateway(cfg, transport=transport, clock=clock), clock


def upstream_of(response) -> str:
    return json.loads(response.body)["upstream"]


def hit(gw, client, path="/v1/gray/x"):
    return gw.handle(Request("GET", path, headers={"x-client-id": client}))


# -- weighted split --------------------------------------------------------

def test_weighted_split_follows_weights():
    gw, _ = make_gateway(weights={"old": 1, "new": 3})
    counts = Counter(upstream_of(hit(gw, f"c{i}")) for i in range(2000))
    assert counts["old"] > 0 and counts["new"] > 0
    ratio = counts["new"] / (counts["old"] + counts["new"])
    assert 0.70 < ratio < 0.80, f"split off target: {counts}"


def test_same_client_deterministic_without_stickiness():
    gw, _ = make_gateway(weights={"old": 1, "new": 1})
    first = upstream_of(hit(gw, "client-7"))
    for _ in range(10):
        assert upstream_of(hit(gw, "client-7")) == first


def test_weight_zero_drains_traffic_but_health_keeps_tracking():
    gw, _ = make_gateway(weights={"old": 1, "new": 0})
    for i in range(50):
        assert upstream_of(hit(gw, f"c{i}")) == "old"
    # health registry still tracks the drained instance (HEALTHY, probed)
    snap = gw.health.snapshot()
    assert "new" in snap and snap["new"]["state"] == "HEALTHY"


def test_all_weights_zero_yields_no_upstream_error():
    gw, _ = make_gateway(weights={"old": 0, "new": 0})
    resp = hit(gw, "c1")
    assert resp.status == 503
    assert json.loads(resp.body)["error"]["code"] == "ALL_UPSTREAMS_UNAVAILABLE"


def test_legacy_route_unchanged_when_sibling_route_has_weights():
    gw, _ = make_gateway(weights={"old": 1, "new": 0})
    # legacy route has no weights -> plain round robin across both
    picks = {upstream_of(hit(gw, "c1", "/v1/legacy/x")) for _ in range(4)}
    assert picks == {"old", "new"}


# -- stickiness ------------------------------------------------------------

def test_sticky_client_pinned_for_ttl():
    gw, clock = make_gateway(weights={"old": 1, "new": 1}, sticky_seconds=30.0)
    first = upstream_of(hit(gw, "user-1"))
    clock.advance(10.0)
    assert upstream_of(hit(gw, "user-1")) == first
    clock.advance(29.0)  # sliding TTL refreshed at t=10 -> expires at 40
    assert upstream_of(hit(gw, "user-1")) == first


def test_sticky_expiry_reassigns_via_weighted_pick():
    gw, clock = make_gateway(weights={"old": 1, "new": 0}, sticky_seconds=10.0)
    assert upstream_of(hit(gw, "user-1")) == "old"
    gw.set_route_weights("gray", {"old": 0, "new": 1})
    clock.advance(11.0)  # assignment expired
    assert upstream_of(hit(gw, "user-1")) == "new"


def test_sticky_target_drained_to_zero_reassigns_without_sending_traffic():
    gw, _ = make_gateway(weights={"old": 1, "new": 1}, sticky_seconds=100.0)
    # pin user-1 to whichever instance, then drain that instance
    first = upstream_of(hit(gw, "user-1"))
    drained = {"old": 0, "new": 1} if first == "old" else {"old": 1, "new": 0}
    gw.set_route_weights("gray", drained)
    second = upstream_of(hit(gw, "user-1"))
    assert second != first  # reassigned to the only weighted instance
    assert second == ("new" if first == "old" else "old")


def test_sticky_target_ejected_never_receives_request():
    gw, _ = make_gateway(weights={"old": 1, "new": 1}, sticky_seconds=100.0)
    first = upstream_of(hit(gw, "user-1"))
    other = "new" if first == "old" else "old"
    # eject the pinned instance: force all traffic onto it, then fail twice
    gw._transport.register(first, lambda req: Response(500, body=b"boom"))
    gw.set_route_weights("gray", {first: 1, other: 0})
    for i in range(2):  # failure_threshold=2
        resp = hit(gw, f"breaker-{i}")
        assert resp.status == 502
    assert gw.health.status(first).state.value == "EJECTED"
    # restore balanced weights; sticky user must move deterministically and
    # never be sent to the ejected instance
    gw.set_route_weights("gray", {"old": 1, "new": 1})
    moved = upstream_of(hit(gw, "user-1"))
    assert moved == other
    assert gw.health.status(moved).state.value == "HEALTHY"


def test_recovered_instance_does_not_steal_sticky_client_back():
    gw, clock = make_gateway(weights={"old": 1, "new": 1}, sticky_seconds=100.0)
    first = upstream_of(hit(gw, "user-1"))
    other = "new" if first == "old" else "old"
    # drain the pinned instance -> client moves
    gw.set_route_weights("gray", {first: 0, other: 1})
    assert upstream_of(hit(gw, "user-1")) == other
    # restore weights before TTL expires: client must stay put (no flapping)
    gw.set_route_weights("gray", {"old": 1, "new": 1})
    clock.advance(50.0)  # still within the 100s sliding TTL
    assert upstream_of(hit(gw, "user-1")) == other


def test_concurrent_same_client_never_splits():
    gw, _ = make_gateway(weights={"old": 1, "new": 1}, sticky_seconds=60.0)
    results: list[str] = []
    lock = threading.Lock()

    def worker():
        uid = upstream_of(hit(gw, "hot-user"))
        with lock:
            results.append(uid)

    threads = [threading.Thread(target=worker) for _ in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(results)) == 1, f"hot-user split across {set(results)}"


# -- runtime adjustment & observability ------------------------------------

def test_runtime_weight_change_applies_to_new_requests():
    gw, _ = make_gateway(weights={"old": 1, "new": 0})
    assert upstream_of(hit(gw, "c1")) == "old"
    change = gw.set_route_weights("gray", {"old": 0, "new": 1})
    assert change["weights"] == {"old": 0, "new": 1}
    for i in range(20):
        assert upstream_of(hit(gw, f"n{i}")) == "new"


def test_set_route_weights_rejects_unknown_route_and_upstream():
    gw, _ = make_gateway(weights={"old": 1, "new": 1})
    for route_id, weights in (
        ("nope", {"old": 1}),
        ("gray", {"ghost": 1}),
        ("gray", {"old": -1}),
    ):
        with pytest.raises(GatewayError) as ei:
            gw.set_route_weights(route_id, weights)
        assert ei.value.code.value in {"ROUTE_NOT_FOUND", "INVALID_WEIGHTS"}


def test_gray_snapshot_reports_weights_and_traffic():
    gw, _ = make_gateway(weights={"old": 1, "new": 1}, sticky_seconds=30.0)
    for i in range(10):
        hit(gw, f"c{i}")
    snap = gw.gray_snapshot()
    assert snap["weights"]["gray"]["weights"] == {"old": 1, "new": 1}
    traffic = snap["traffic"]
    assert traffic["gray:old"] + traffic["gray:new"] == 10
    assert "gray:c0" in snap["sticky"]
