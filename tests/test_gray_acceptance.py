"""Acceptance: run the gateway as a real HTTP service and observe gray
splitting + stickiness with concurrent client traffic.

The service is started on an ephemeral port with uvicorn (in a thread) and
exercised through real HTTP requests from a thread pool. Assertions use the
public endpoints only: response bodies and ``GET /_gateway/gray``.
"""
from __future__ import annotations

import hashlib
import json
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
import uvicorn

from gateway.app import create_app
from gateway.config import (
    ClientRateRule,
    GatewayConfig,
    RouteRule,
    UpstreamConfig,
)
from gateway.gateway import Gateway
from gateway.logging_setup import decision_logger, log_event
from gateway.models import Response
from gateway.transport import StubTransport

_log = decision_logger("gateway.test.gray_acceptance")

SPLIT_ROUTE = "acc-split"    # weighted only, 8:2
STICKY_ROUTE = "acc-sticky"  # weighted 5:5 + sticky pins


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _echo(name: str):
    def handler(request) -> Response:
        if request.path.rstrip("/").endswith("/health"):
            return Response(status=200, body=b"health", upstream_id=name)
        return Response(
            status=200,
            body=json.dumps({"by": name}).encode(),
            upstream_id=name,
        )
    return handler


@pytest.fixture(scope="module")
def service():
    cfg = GatewayConfig(
        routes=(
            RouteRule(
                id=SPLIT_ROUTE, prefix="/v1/split", upstream_ids=("a", "b"),
                weights={"a": 8, "b": 2},
            ),
            RouteRule(
                id=STICKY_ROUTE, prefix="/v1/sticky", upstream_ids=("a", "b"),
                weights={"a": 5, "b": 5}, sticky_ttl_seconds=60.0,
            ),
        ),
        upstreams=(
            UpstreamConfig("a", "stub://a"),
            UpstreamConfig("b", "stub://b"),
        ),
        rate=ClientRateRule(),  # limits disabled for the acceptance run
    )
    gateway = Gateway(cfg, transport=StubTransport({"a": _echo("a"), "b": _echo("b")}))
    port = _free_port()
    config = uvicorn.Config(
        create_app(gateway), host="127.0.0.1", port=port, log_level="warning"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("gateway service did not start")
        time.sleep(0.02)
    log_event(_log, "acceptance_service_up", port=port,
              weights={SPLIT_ROUTE: {"a": 8, "b": 2}, STICKY_ROUTE: {"a": 5, "b": 5}})
    yield f"http://127.0.0.1:{port}", gateway
    server.should_exit = True
    thread.join(timeout=5)


def _get_upstream(client: httpx.Client, path: str, client_id: str) -> str:
    resp = client.get(path, headers={"x-client-id": client_id})
    assert resp.status_code == 200, resp.text
    return resp.json()["by"]


def _hash_target(route_id: str, client_id: str) -> str:
    """Mirror of the gateway's deterministic sticky hash (5:5 ranges)."""
    bucket = int(hashlib.sha256(f"{route_id}|{client_id}".encode()).hexdigest(), 16) % 10
    return "a" if bucket < 5 else "b"


def test_weighted_split_ratio_observed_over_http(service):
    base, gateway = service
    log_event(_log, "acceptance_scenario", name="split_ratio",
              expect="200 concurrent requests -> ~80/20 split, served totals match")
    with httpx.Client(base_url=base, timeout=5) as client:
        with ThreadPoolExecutor(max_workers=32) as pool:
            upstreams = list(pool.map(
                lambda i: _get_upstream(client, "/v1/split/x", f"anon-{i}"),
                range(200),
            ))
    a_count = upstreams.count("a")
    b_count = upstreams.count("b")
    ratio = b_count / len(upstreams)
    log_event(_log, "acceptance_result", name="split_ratio",
              a=a_count, b=b_count, ratio=round(ratio, 3),
              gray=gateway.gray_snapshot()["routes"][SPLIT_ROUTE])
    assert 0.10 <= ratio <= 0.30, f"split {a_count}:{b_count} outside 8:2 tolerance"
    served = gateway.gray_snapshot()["routes"][SPLIT_ROUTE]["served"]
    assert served == {"a": a_count, "b": b_count}
    assert sum(served.values()) == 200


def test_sticky_clients_stable_under_concurrency(service):
    base, gateway = service
    log_event(_log, "acceptance_scenario", name="sticky_concurrent",
              expect="8 clients x 8 concurrent requests; each client stays on one instance")
    # Deterministically pick 4 client ids hashing to a and 4 to b.
    ids = {"a": [], "b": []}
    i = 0
    while min(len(ids["a"]), len(ids["b"])) < 4:
        cid = f"acc-user-{i}"
        target = _hash_target(STICKY_ROUTE, cid)
        if len(ids[target]) < 4:
            ids[target].append(cid)
        i += 1
    clients = ids["a"] + ids["b"]

    with httpx.Client(base_url=base, timeout=5) as client:
        with ThreadPoolExecutor(max_workers=64) as pool:
            results = list(pool.map(
                lambda cid: (cid, [_get_upstream(client, "/v1/sticky/y", cid)
                                   for _ in range(8)]),
                clients,
            ))
    per_client = {cid: set(hits) for cid, hits in results}
    log_event(_log, "acceptance_result", name="sticky_concurrent",
              per_client={c: sorted(s) for c, s in per_client.items()},
              gray=gateway.gray_snapshot()["routes"][STICKY_ROUTE])
    for cid, hits in per_client.items():
        assert len(hits) == 1, f"client {cid} split across {hits}"
        assert hits.pop() == _hash_target(STICKY_ROUTE, cid), "pin must follow the hash rule"
    pins = gateway.gray_snapshot()["routes"][STICKY_ROUTE]["sticky_active_pins"]
    assert pins == len(clients)


def test_runtime_reweight_over_http_takes_effect_immediately(service):
    base, gateway = service
    log_event(_log, "acceptance_scenario", name="runtime_reweight",
              expect="POST /_gateway/gray/weights 8:2 -> 0:10; new traffic all to b")
    with httpx.Client(base_url=base, timeout=5) as client:
        before = client.get("/_gateway/gray").json()["routes"][SPLIT_ROUTE]
        assert before["weights"] == {"a": 8, "b": 2}
        resp = client.post("/_gateway/gray/weights", json={
            "route_id": SPLIT_ROUTE, "weights": {"a": 0, "b": 10},
        })
        assert resp.status_code == 200, resp.text
        assert resp.json()["weights"] == {"a": 0, "b": 10}
        # New requests follow the new table immediately.
        with ThreadPoolExecutor(max_workers=16) as pool:
            upstreams = list(pool.map(
                lambda i: _get_upstream(client, "/v1/split/x", f"post-{i}"),
                range(40),
            ))
        after = client.get("/_gateway/gray").json()["routes"][SPLIT_ROUTE]
    log_event(_log, "acceptance_result", name="runtime_reweight",
              landed=set(upstreams), weights=after["weights"], served=after["served"])
    assert set(upstreams) == {"b"}, "weight 0 instance must not receive new traffic"
    assert after["weights"] == {"a": 0, "b": 10}
    assert after["served_total"] == before["served_total"] + 40
