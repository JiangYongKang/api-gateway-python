"""End-to-end ejection -> active probe recovery -> traffic resumes.

All timing uses an injectable fake clock, so the recovery sequence is
fully deterministic and fast.
"""
from __future__ import annotations

import json
import threading

from gateway.config import (
    ClientRateRule,
    GatewayConfig,
    RouteRule,
    UpstreamConfig,
)
from gateway.gateway import Gateway
from gateway.models import Request, Response
from gateway.transport import StubTransport


class FakeClock:
    def __init__(self, start=100.0):
        self.t = start
        self._lock = threading.Lock()

    def __call__(self):
        with self._lock:
            return self.t

    def advance(self, dt):
        with self._lock:
            self.t += dt


class ToggleUpstream:
    """Business handler fails while unhealthy; /health mirrors the toggle."""

    def __init__(self):
        self.healthy = False
        self.business_calls = 0

    def handler(self, request: Request) -> Response:
        if request.path.endswith("/health"):
            status = 200 if self.healthy else 503
            return Response(status, body=json.dumps({"ok": self.healthy}).encode())
        self.business_calls += 1
        if self.healthy:
            return Response(200, body=b"recovered")
        return Response(500, body=b"failure")


def make_gateway(clock, *, open_state=0.3, interval=0.1, threshold=1):
    upstream = ToggleUpstream()
    cfg = GatewayConfig(
        routes=(
            RouteRule(
                id="r", prefix="/v1", upstream_ids=("u",),
                timeout_seconds=1.0, failure_threshold=threshold,
                open_state_seconds=open_state,
                probe_interval_seconds=interval,
                probe_path="/health",
                half_open_success_threshold=1,
                half_open_max_probe_requests=1,
            ),
        ),
        upstreams=(UpstreamConfig("u", "stub://u"),),
        rate=ClientRateRule(),
    )
    gw = Gateway(cfg, transport=StubTransport({"u": upstream.handler}), clock=clock)
    return gw, upstream


def _code(resp):
    return json.loads(resp.body)["error"]["code"]


def test_eject_probe_recover_cycle_deterministic():
    clock = FakeClock()
    gw, up = make_gateway(clock)

    r = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "c1"}))
    assert r.status == 502  # the request that trips the breaker returns 502

    r = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "c2"}))
    assert r.status == 503 and _code(r) == "UPSTREAM_CIRCUIT_OPEN"
    calls_when_ejected = up.business_calls

    # still cooling -> no probe
    clock.advance(0.29)
    assert gw.run_probe_tick() == 0

    # cool-down over, probe runs but upstream still unhealthy -> re-ejected
    clock.advance(0.02)
    assert gw.run_probe_tick() == 1
    r = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "c3"}))
    assert r.status == 503 and _code(r) == "UPSTREAM_CIRCUIT_OPEN"
    assert up.business_calls == calls_when_ejected  # no traffic while ejected

    # recovery probe succeeds -> breaker closes -> traffic resumes
    up.healthy = True
    clock.advance(0.31)
    assert gw.run_probe_tick() == 1
    r = gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "c4"}))
    assert r.status == 200 and r.body == b"recovered"


def test_probes_do_not_consume_client_limiter_slots():
    clock = FakeClock()
    cfg_rate = ClientRateRule(requests_per_window=100, max_concurrency=100)
    gw, up = make_gateway(clock)
    gw.limiter._config = cfg_rate  # type: ignore[attr-defined]
    gw.handle(Request("GET", "/v1/x", headers={"x-client-id": "c1"}))
    clock.advance(0.32)
    before = dict(gw.limiter.snapshot())
    gw.run_probe_tick()  # probe failure -> stays ejected
    clock.advance(0.32)
    up.healthy = True
    gw.run_probe_tick()  # recovery probe
    after = gw.limiter.snapshot()
    # only the single business request exists in the limiter; probes absent
    assert set(after) == set(before) == {"c1"}
    assert after["c1"]["window_count"] == 1
    assert after["c1"]["inflight"] == 0
