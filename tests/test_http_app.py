"""HTTP-level checks through the FastAPI adapter (TestClient/ASGI)."""
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


def make_client(rate=None, routes=None, handlers=None):
    cfg = GatewayConfig(
        routes=routes or (
            RouteRule(id="r", prefix="/v1", upstream_ids=("u",),
                      timeout_seconds=1.0, failure_threshold=5),
        ),
        upstreams=(UpstreamConfig("u", "stub://u"),),
        rate=rate or ClientRateRule(),
    )
    transport = StubTransport(handlers or {"u": lambda req: Response(200, body=b"hi")})
    gw = Gateway(cfg, transport=transport)
    return TestClient(create_app(gw)), gw


def test_happy_path_passes_body_and_upstream_header():
    client, _ = make_client()
    r = client.get("/v1/hello", headers={"x-client-id": "c"})
    assert r.status_code == 200
    assert r.content == b"hi"


def test_404_body_is_structured():
    client, _ = make_client()
    r = client.get("/no/such/route")
    assert r.status_code == 404
    err = r.json()["error"]
    assert err["code"] == "ROUTE_NOT_FOUND"
    assert "tried" in err["details"]


def test_429_retry_after_header():
    client, _ = make_client(rate=ClientRateRule(requests_per_window=1, window_seconds=3.0))
    assert client.get("/v1/a", headers={"x-client-id": "z"}).status_code == 200
    r = client.get("/v1/b", headers={"x-client-id": "z"})
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "RATE_LIMITED"
    assert r.headers["retry-after"] == "3"


def test_gateway_health_endpoint_reports_state():
    client, _ = make_client()
    r = client.get("/_gateway/health")
    assert r.status_code == 200
    data = r.json()
    assert data["upstreams"]["u"]["state"] == "HEALTHY"


def test_post_body_is_forwarded():
    def echo(req):
        return Response(200, body=req.body)
    client, _ = make_client(handlers={"u": echo})
    r = client.post("/v1/items", content=b'{"x":1}',
                    headers={"content-type": "application/json"})
    assert r.status_code == 200 and r.content == b'{"x":1}'
