"""Live acceptance: real uvicorn server, concurrent clients, observed split.

Unlike the unit/integration tests this module starts the actual HTTP
service and drives it with concurrent ``httpx`` requests, then checks the
*observed* traffic split and stickiness — not just internal logic. Every
scenario logs its judgement basis (weights, health, observed counts) so
``pytest -s`` output doubles as an audit trail.
"""
from __future__ import annotations

import socket
import threading
import time
from collections import Counter
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

log = decision_logger("acceptance.gray")

WEIGHTS = {"old": 1, "new": 3}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _build_gateway() -> Gateway:
    cfg = GatewayConfig(
        routes=(
            RouteRule(
                id="gray", prefix="/v1/gray",
                upstream_ids=("old", "new"),
                weights=dict(WEIGHTS),
                sticky_seconds=30.0,
                timeout_seconds=2.0, failure_threshold=5,
            ),
        ),
        upstreams=(
            UpstreamConfig("old", "stub://old"),
            UpstreamConfig("new", "stub://new"),
        ),
        rate=ClientRateRule(),  # unlimited: we measure split, not limiting
    )
    transport = StubTransport({
        "old": lambda req: Response(200, body=b'{"upstream":"old"}'),
        "new": lambda req: Response(200, body=b'{"upstream":"new"}'),
    })
    return Gateway(cfg, transport=transport)


@pytest.fixture(scope="module")
def server():
    """Run the real service in a background uvicorn thread."""
    gateway = _build_gateway()
    app = create_app(gateway)
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    srv = uvicorn.Server(config)
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10.0
    while not srv.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn did not start in time")
        time.sleep(0.02)
    base = f"http://127.0.0.1:{port}"
    log_event(log, "server_up", base=base)
    yield base, gateway
    srv.should_exit = True
    thread.join(timeout=5.0)


def _fetch(base: str, client_id: str) -> str:
    r = httpx.get(
        f"{base}/v1/gray/x", headers={"x-client-id": client_id}, timeout=5.0
    )
    r.raise_for_status()
    return r.json()["upstream"]


def test_concurrent_split_matches_weights(server):
    """400 distinct clients, 8 threads: observed split ~= configured 1:3."""
    base, gateway = server
    clients = [f"split-{i}" for i in range(400)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        picked = list(pool.map(lambda c: _fetch(base, c), clients))
    counts = Counter(picked)
    ratio_new = counts["new"] / len(clients)
    log_event(
        log, "scenario", name="concurrent_split", weights=WEIGHTS,
        observed=dict(counts), expect="new~0.75±0.07",
        verdict="PASS" if 0.68 < ratio_new < 0.82 else "FAIL",
        upstream_states={k: v["state"] for k, v in gateway.health.snapshot().items()},
    )
    assert counts["old"] > 0 and counts["new"] > 0
    assert 0.68 < ratio_new < 0.82, f"split off target: {counts}"


def test_concurrent_same_client_never_splits(server):
    """One hot client hammered from 16 threads stays on one instance."""
    base, gateway = server
    with ThreadPoolExecutor(max_workers=16) as pool:
        picked = list(pool.map(lambda _: _fetch(base, "hot-user"), range(64)))
    distinct = set(picked)
    sticky = gateway.sticky.snapshot()
    log_event(
        log, "scenario", name="concurrent_stickiness", client="hot-user",
        distinct=sorted(distinct), sticky_entry=sticky.get("gray:hot-user"),
        verdict="PASS" if len(distinct) == 1 else "FAIL",
    )
    assert len(distinct) == 1, f"hot-user split across {distinct}"
    assert sticky["gray:hot-user"]["upstream"] == distinct.pop()


def test_many_concurrent_clients_each_stay_pinned(server):
    """Each of 60 clients sends 5 concurrent requests: no per-client split."""
    base, _ = server
    clients = [f"pin-{i}" for i in range(60)]

    def check(client_id: str) -> set[str]:
        with ThreadPoolExecutor(max_workers=5) as pool:
            return set(pool.map(lambda _: _fetch(base, client_id), range(5)))

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(check, clients))
    split_clients = [c for c, r in zip(clients, results) if len(r) != 1]
    log_event(
        log, "scenario", name="per_client_pinning", clients=len(clients),
        split_clients=split_clients,
        verdict="PASS" if not split_clients else "FAIL",
    )
    assert not split_clients


def test_runtime_reweight_over_http_moves_new_traffic(server):
    """PUT new weights without restart; new clients follow, no 5xx storm."""
    base, gateway = server
    r = httpx.put(
        f"{base}/_gateway/weights/gray", json={"old": 0, "new": 1}, timeout=5.0
    )
    assert r.status_code == 200, r.text
    clients = [f"after-reweight-{i}" for i in range(120)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        picked = list(pool.map(lambda c: _fetch(base, c), clients))
    counts = Counter(picked)
    log_event(
        log, "scenario", name="runtime_reweight",
        new_weights={"old": 0, "new": 1}, observed=dict(counts),
        expect="all-new", verdict="PASS" if counts["old"] == 0 else "FAIL",
    )
    assert counts["old"] == 0 and counts["new"] == len(clients)

    # weight-0 instance keeps its health tracking (still HEALTHY, still
    # listed) — draining never disables health maintenance
    health = gateway.health.snapshot()
    assert health["old"]["state"] == "HEALTHY"

    # restore the original 1:3 split for other scenarios / repeat runs
    r = httpx.put(f"{base}/_gateway/weights/gray", json=WEIGHTS, timeout=5.0)
    assert r.status_code == 200
    clients = [f"restored-{i}" for i in range(200)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        picked = list(pool.map(lambda c: _fetch(base, c), clients))
    counts = Counter(picked)
    log_event(
        log, "scenario", name="restore_weights", weights=WEIGHTS,
        observed=dict(counts),
        verdict="PASS" if counts["old"] > 0 and counts["new"] > 0 else "FAIL",
    )
    assert counts["old"] > 0 and counts["new"] > 0


def test_admin_snapshot_reflects_live_traffic(server):
    """The observability endpoint reports weights, pins and real counts."""
    base, _ = server
    for i in range(20):
        _fetch(base, f"obs-{i}")
    snap = httpx.get(f"{base}/_gateway/weights", timeout=5.0).json()
    log_event(
        log, "scenario", name="observability",
        weights=snap["weights"]["gray"]["weights"],
        traffic_keys=len([k for k in snap["traffic"] if k.startswith("gray:")]),
        sticky_entries=len([k for k in snap["sticky"] if k.startswith("gray:")]),
    )
    assert snap["weights"]["gray"]["weights"] == WEIGHTS
    traffic_total = sum(
        v for k, v in snap["traffic"].items() if k.startswith("gray:")
    )
    assert traffic_total >= 20
    assert any(k.startswith("gray:obs-") for k in snap["sticky"])
