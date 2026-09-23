import json

from gateway.demo import build_demo_gateway
from gateway.models import Request


def test_end_to_end_round_robin_and_errors():
    gw = build_demo_gateway(start_probes=False)
    # two requests -> different upstream due to round robin
    r1 = gw.handle(Request("GET", "/v1/echo/x"))
    r2 = gw.handle(Request("GET", "/v1/echo/x"))
    assert r1.status == 200 and r2.status == 200
    b1, b2 = json.loads(r1.body), json.loads(r2.body)
    assert {b1["upstream"], b2["upstream"]} == {"echo-a", "echo-b"}

    # no route -> distinguishable 404
    r = gw.handle(Request("GET", "/nope"))
    assert r.status == 404
    assert json.loads(r.body)["error"]["code"] == "ROUTE_NOT_FOUND"

    # ordinary upstream error (flaky 500, threshold 2 so first call = 502)
    r = gw.handle(Request("GET", "/v1/flaky/x", headers={"x-client-id": "c-flaky"}))
    assert r.status == 502
    assert json.loads(r.body)["error"]["code"] == "UPSTREAM_ERROR"
