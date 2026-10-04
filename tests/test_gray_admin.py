"""Gray-release admin endpoints over HTTP (TestClient/ASGI)."""
from __future__ import annotations

from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.config import (
    ClientRateRule,
    GatewayConfig,
    RouteRule,
    UpstreamConfig,
)
from gateway.gateway import Gateway
from gateway.models import Response
from gateway.transport import StubTransport


def make_client():
    cfg = GatewayConfig(
        routes=(
            RouteRule(
                id="gray", prefix="/v1/gray",
                upstream_ids=("a", "b"),
                weights={"a": 1, "b": 0},
                sticky_seconds=30.0,
                timeout_seconds=1.0, failure_threshold=5,
            ),
            RouteRule(
                id="plain", prefix="/v1/plain",
                upstream_ids=("a", "b"),
                timeout_seconds=1.0, failure_threshold=5,
            ),
        ),
        upstreams=(
            UpstreamConfig("a", "stub://a"),
            UpstreamConfig("b", "stub://b"),
        ),
        rate=ClientRateRule(),
    )
    transport = StubTransport({
        "a": lambda req: Response(200, body=b'{"upstream":"a"}'),
        "b": lambda req: Response(200, body=b'{"upstream":"b"}'),
    })
    gw = Gateway(cfg, transport=transport)
    return TestClient(create_app(gw)), gw


def test_get_weights_reports_configured_and_unconfigured_routes():
    client, _ = make_client()
    r = client.get("/_gateway/weights")
    assert r.status_code == 200
    body = r.json()
    assert body["weights"]["gray"]["weights"] == {"a": 1, "b": 0}
    assert body["weights"]["gray"]["total_weight"] == 1
    assert "plain" not in body["weights"]  # legacy route: no weight table
    assert body["sticky"] == {} and body["traffic"] == {}


def test_put_weights_takes_effect_without_restart():
    client, _ = make_client()
    for i in range(10):
        r = client.get("/v1/gray/x", headers={"x-client-id": f"c{i}"})
        assert r.json()["upstream"] == "a"
    r = client.put("/_gateway/weights/gray", json={"a": 0, "b": 1})
    assert r.status_code == 200
    assert r.json()["previous"] == {"a": 1, "b": 0}
    for i in range(10):
        resp = client.get("/v1/gray/x", headers={"x-client-id": f"n{i}"})
        assert resp.json()["upstream"] == "b"
    # observed distribution is queryable
    traffic = client.get("/_gateway/weights").json()["traffic"]
    assert traffic["gray:a"] == 10 and traffic["gray:b"] == 10


def test_put_weights_rejects_bad_payloads():
    client, _ = make_client()
    r = client.put("/_gateway/weights/gray", json={"ghost": 1})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "INVALID_WEIGHTS"
    assert r.json()["error"]["details"]["reason"] == "unknown_upstream"

    r = client.put("/_gateway/weights/gray", json={"a": -1})
    assert r.status_code == 400
    assert r.json()["error"]["details"]["reason"] == "invalid_weights"

    r = client.put("/_gateway/weights/gray", content=b"not-json")
    assert r.status_code == 400
    assert r.json()["error"]["details"]["reason"] == "invalid_json"

    r = client.put("/_gateway/weights/gray", json={})
    assert r.status_code == 400

    r = client.put("/_gateway/weights/no-such-route", json={"a": 1})
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "ROUTE_NOT_FOUND"


def test_sticky_assignments_visible_in_admin_snapshot():
    client, _ = make_client()
    client.get("/v1/gray/x", headers={"x-client-id": "user-1"})
    snap = client.get("/_gateway/weights").json()
    assert snap["sticky"]["gray:user-1"]["upstream"] == "a"
    assert snap["sticky"]["gray:user-1"]["expires_in"] > 0
