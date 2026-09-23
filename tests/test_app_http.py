"""FastAPI 入口的错误码映射与响应可区分性。"""

from __future__ import annotations


import httpx
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.config_loader import load_config
from gateway.proxy import GatewayProxy
from gateway.upstream import UpstreamClient
from tests.conftest import FakeClock, ScriptedUpstream


def _client(behaviors: dict, *, single_upstream: bool = False, **cfg_overrides):
    from tests.conftest import build_config

    clock = FakeClock()
    data = build_config(**cfg_overrides)
    if single_upstream:
        data["routes"][0]["upstreams"] = [
            {"id": "u1", "base_url": "http://up1"}
        ]
    cfg = load_config(data)
    upstream = ScriptedUpstream(behaviors)
    proxy = GatewayProxy(
        cfg, UpstreamClient(upstream.sender),
        probe_sender=upstream.sender, clock=clock,
    )
    app = create_app(cfg, proxy=proxy)
    transport = httpx.ASGITransport(app=app)
    return TestClient(app), httpx.AsyncClient(transport=transport, base_url="http://gw"), clock


def test_route_not_found_maps_404():
    client, _, _ = _client({"u1": "ok", "u2": "ok"})
    r = client.get("/no/such/path")
    assert r.status_code == 404
    body = r.json()["error"]
    assert body["code"] == "route_not_found"
    assert "reason" in body["details"]


def test_no_available_upstream_maps_503_distinct_from_404():
    client, _, _ = _client(
        {"u1": "connect", "u2": "connect"}, failure_threshold=1
    )
    client.get("/svc/x")
    client.get("/svc/x")
    r = client.get("/svc/x")
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "no_available_upstream"


def test_rate_limited_maps_429():
    client, _, _ = _client(
        {"u1": "ok", "u2": "ok"},
        rate_limit={"requests_per_second": 1, "max_concurrent": 0},
    )
    assert client.get("/svc/x").status_code == 200
    r = client.get("/svc/x")
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "rate_limited"


async def test_circuit_open_maps_503_with_distinct_code():
    _, aclient, clock = _client(
        {"u1": "timeout"},
        single_upstream=True,
        failure_threshold=99,
        circuit_threshold=1,
        circuit_open_seconds=0.5,
    )
    r1 = await aclient.get("/svc/x")
    assert r1.status_code == 504  # 第一次：真实超时
    # 熔断已打开（阈值 1）；时间未推进，直接拒绝
    r2 = await aclient.get("/svc/x")
    assert r2.status_code == 503
    assert r2.json()["error"]["code"] == "circuit_open"


def test_timeout_maps_504():
    client, _, _ = _client({"u1": "timeout", "u2": "timeout"})
    r = client.get("/svc/x")
    assert r.status_code == 504
    assert r.json()["error"]["code"] == "upstream_timeout"


def test_bad_status_maps_502_and_upstream_id_header():
    client, _, _ = _client({"u1": 500, "u2": "ok"})
    r = client.get("/svc/x")
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "upstream_bad_status"


def test_success_response_tags_upstream_and_request_id():
    client, _, _ = _client({"u1": "ok", "u2": "ok"})
    r = client.get("/svc/x")
    assert r.status_code == 200
    assert r.headers["x-upstream-id"] == "u1"
    assert r.headers["x-request-id"]


def test_all_error_codes_are_distinct():
    """同一配置下可触发的各类失败必须互相可区分。"""
    client, _, _ = _client(
        {"u1": "timeout", "u2": "connect"},
        failure_threshold=99,
        circuit_threshold=1,
        rate_limit={"requests_per_second": 1, "max_concurrent": 0},
    )
    codes = set()
    codes.add(client.get("/nope").json()["error"]["code"])  # route_not_found
    codes.add(client.get("/svc/x").json()["error"]["code"])  # upstream_timeout
    # 第二次请求：令牌已耗尽（时间未推进）-> rate_limited
    codes.add(client.get("/svc/x").json()["error"]["code"])
    assert codes == {"route_not_found", "upstream_timeout", "rate_limited"}
