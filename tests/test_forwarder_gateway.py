"""Timeout-driven circuit breaking, retry semantics and error mapping."""
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
from gateway.models import Request, Response
from gateway.transport import StubTransport


def _resp(status=200, body=b"ok", upstream=None):
    return Response(status=status, body=body, upstream_id=upstream)


class ScriptedUpstream:
    """Callable fake with a call counter and scripted responses/delays."""

    def __init__(self, name, *, statuses=None, delay=0.0):
        self.name = name
        self.statuses = list(statuses or [200])
        self.delay = delay
        self.calls: list[Request] = []
        self._lock = threading.Lock()

    def __call__(self, request: Request) -> Response:
        with self._lock:
            self.calls.append(request)
            idx = len(self.calls) - 1
            status = self.statuses[min(idx, len(self.statuses) - 1)]
        if self.delay:
            time.sleep(self.delay)
        return _resp(status, json.dumps({"by": self.name}).encode(), self.name)


def build_gateway(routes, upstreams, handlers, rate=None):
    cfg = GatewayConfig(
        routes=routes,
        upstreams=upstreams,
        rate=rate or ClientRateRule(),
    )
    return Gateway(cfg, transport=StubTransport(handlers))


def body_of(resp):
    return json.loads(resp.body)


# -- ordinary upstream error vs timeout vs all-unavailable ----------------

def test_upstream_error_502_distinguishable():
    up = (UpstreamConfig("u", "stub://u"),)
    rule = RouteRule(id="r", prefix="/v1", upstream_ids=("u",), timeout_seconds=1.0)
    gw = build_gateway((rule,), up, {"u": ScriptedUpstream("u", statuses=[500])})
    r = gw.handle(Request("GET", "/v1/x"))
    assert r.status == 502
    assert body_of(r)["error"]["code"] == "UPSTREAM_ERROR"
    assert body_of(r)["error"]["upstream_status"] == 500


def test_timeout_504_then_ejection_503_are_distinct():
    up = (UpstreamConfig("slow", "stub://slow"),)
    rule = RouteRule(
        id="r", prefix="/v1", upstream_ids=("slow",),
        timeout_seconds=0.05, failure_threshold=2,
        open_state_seconds=10.0, max_retries=0,
    )
    gw = build_gateway(
        (rule,), up, {"slow": ScriptedUpstream("slow", delay=0.3)}
    )
    r1 = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "c1"}))
    assert r1.status == 504 and body_of(r1)["error"]["code"] == "UPSTREAM_TIMEOUT"
    r2 = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "c2"}))
    assert r2.status == 504  # second consecutive timeout, now ejected
    r3 = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "c3"}))
    assert r3.status == 503
    assert body_of(r3)["error"]["code"] == "UPSTREAM_CIRCUIT_OPEN"


def test_two_upstreams_ejected_after_sustained_failures():
    ups = tuple(UpstreamConfig(u, f"stub://{u}") for u in ("a", "b"))
    rule = RouteRule(
        id="r", prefix="/v1", upstream_ids=("a", "b"),
        lb_policy=LBPolicy.ROUND_ROBIN, timeout_seconds=1.0,
        failure_threshold=1, max_retries=0,
    )
    handlers = {u: ScriptedUpstream(u, statuses=[500]) for u in ("a", "b")}
    gw = build_gateway((rule,), ups, handlers)
    codes = []
    for i in range(4):
        r = gw.handle(Request("GET", f"/v1/{i}", headers={"x-client-id": f"c{i}"}))
        codes.append(body_of(r)["error"]["code"])
    # first two 502s eject a and b; later requests see circuits open
    assert codes.count("UPSTREAM_ERROR") == 2
    assert codes.count("UPSTREAM_CIRCUIT_OPEN") == 2
    # traffic never returned from an ejected instance (still only 1 call each)
    assert len(handlers["a"].calls) == 1 and len(handlers["b"].calls) == 1


# -- retry semantics / no extra side effects ------------------------------

def test_retry_moves_to_next_upstream_and_succeeds():
    ups = tuple(UpstreamConfig(u, f"stub://{u}") for u in ("a", "b"))
    rule = RouteRule(
        id="r", prefix="/v1", upstream_ids=("a", "b"),
        timeout_seconds=1.0, max_retries=1,
        retry_on_statuses=frozenset({503}),
    )
    handlers = {
        "a": ScriptedUpstream("a", statuses=[503]),
        "b": ScriptedUpstream("b", statuses=[200]),
    }
    gw = build_gateway((rule,), ups, handlers)
    r = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "c"}))
    assert r.status == 200 and body_of(r)["by"] == "b"
    assert len(handlers["a"].calls) == 1 and len(handlers["b"].calls) == 1


def test_non_retryable_4xx_stops_immediately():
    ups = tuple(UpstreamConfig(u, f"stub://{u}") for u in ("a", "b"))
    rule = RouteRule(
        id="r", prefix="/v1", upstream_ids=("a", "b"),
        timeout_seconds=1.0, max_retries=3,
        retry_on_statuses=frozenset({503}),
    )
    handlers = {
        "a": ScriptedUpstream("a", statuses=[404]),
        "b": ScriptedUpstream("b", statuses=[200]),
    }
    gw = build_gateway((rule,), ups, handlers)
    r = gw.handle(Request("GET", "/v1/x"))
    assert r.status == 502 and body_of(r)["error"]["upstream_status"] == 404
    assert len(handlers["a"].calls) == 1 and len(handlers["b"].calls) == 0


def test_post_is_not_retried_without_extra_side_effects():
    ups = tuple(UpstreamConfig(u, f"stub://{u}") for u in ("a", "b"))
    rule = RouteRule(
        id="r", prefix="/v1", upstream_ids=("a", "b"),
        timeout_seconds=1.0, max_retries=2,
        retry_on_statuses=frozenset({500, 503}),
    )
    handlers = {
        "a": ScriptedUpstream("a", statuses=[500]),
        "b": ScriptedUpstream("b", statuses=[200]),
    }
    gw = build_gateway((rule,), ups, handlers)
    r = gw.handle(Request("POST", "/v1/orders", body=b'{"q":1}'))
    assert r.status == 502
    # POST is non-idempotent: exactly one upstream invocation, one body
    assert len(handlers["a"].calls) == 1
    assert handlers["a"].calls[0].body == b'{"q":1}'
    assert handlers["b"].calls == []


# -- rate / concurrency / route error mapping ----------------------------

def test_rate_limit_and_route_error_are_distinct():
    up = (UpstreamConfig("u", "stub://u"),)
    rule = RouteRule(id="r", prefix="/v1", upstream_ids=("u",))
    rate = ClientRateRule(requests_per_window=1, window_seconds=60.0)
    gw = build_gateway((rule,), up, {"u": ScriptedUpstream("u")}, rate=rate)
    ok = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "z"}))
    limited = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "z"}))
    assert ok.status == 200
    assert limited.status == 429
    assert body_of(limited)["error"]["code"] == "RATE_LIMITED"
    assert limited.headers["retry-after"] == "60"

    missing = gw.handle(Request("GET", "/v9/none", headers={"x-client-id": "z2"}))
    assert missing.status == 404
    assert body_of(missing)["error"]["code"] == "ROUTE_NOT_FOUND"


def test_concurrency_limit_is_distinct_from_rate_limit():
    entered = threading.Event()
    release = threading.Event()

    def blocking_handler(request):
        entered.set()
        release.wait(timeout=2.0)
        return _resp(200)

    up = (UpstreamConfig("u", "stub://u"),)
    rule = RouteRule(id="r", prefix="/v1", upstream_ids=("u",), timeout_seconds=2.0)
    rate = ClientRateRule(requests_per_window=100, max_concurrency=1)
    gw = build_gateway((rule,), up, {"u": blocking_handler}, rate=rate)

    result_box = {}

    def worker():
        result_box["r"] = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "k"}))

    t = threading.Thread(target=worker)
    t.start()
    entered.wait(timeout=2.0)
    rejected = gw.handle(Request("GET", "/v1/y", headers={"x-client-id": "k"}))
    assert rejected.status == 503
    assert body_of(rejected)["error"]["code"] == "CONCURRENCY_LIMITED"
    release.set()
    t.join()
    assert result_box["r"].status == 200
    # slot released after completion -> next request admitted
    after = gw.handle(Request("GET", "/v1/z", headers={"x-client-id": "k"}))
    assert after.status == 200


def test_half_open_capacity_exhausted_is_all_unavailable_not_circuit_open():
    """Single upstream, half_open capacity 1: eject it, cool down, occupy the
    one half-open trial slot with a slow business request; the next request
    must be ALL_UPSTREAMS_UNAVAILABLE (capacity), distinct from circuit open.
    """
    import threading

    started = threading.Event()
    release = threading.Event()

    def handler(req):
        started.set()
        release.wait(timeout=2.0)
        return Response(200, body=b"ok")

    ups = (UpstreamConfig("u", "stub://u"),)
    rule = RouteRule(
        id="r", prefix="/v1", upstream_ids=("u",),
        timeout_seconds=2.0, failure_threshold=1,
        open_state_seconds=0.05,
        half_open_max_probe_requests=1, half_open_success_threshold=1,
        max_retries=0,
    )
    gw = build_gateway((rule,), ups, {"u": lambda req: Response(500)})
    r = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "init"}))
    assert r.status == 502

    # after cool-down the breaker is half-open; the slow trial request
    # occupies its single slot.
    gw._transport.register("u", handler)
    time.sleep(0.1)
    box = {}

    def worker():
        box["r"] = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "trial"}))

    t = threading.Thread(target=worker)
    t.start()
    assert started.wait(timeout=2.0)
    r2 = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "other"}))
    assert r2.status == 503
    assert body_of(r2)["error"]["code"] == "ALL_UPSTREAMS_UNAVAILABLE"
    assert body_of(r2)["error"]["details"]["reason"] == "half_open_capacity_exhausted"
    release.set()
    t.join()
    assert box["r"].status == 200
