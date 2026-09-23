"""Static gateway configuration models."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class LBPolicy(str, Enum):
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_INFLIGHT = "least_inflight"


@dataclass(frozen=True)
class UpstreamConfig:
    id: str
    base_url: str  # e.g. "http://127.0.0.1:9001" or "stub://echo-a"


@dataclass(frozen=True)
class RouteRule:
    id: str
    prefix: str  # request path prefix; first matching rule wins
    upstream_ids: tuple[str, ...]
    lb_policy: LBPolicy = LBPolicy.ROUND_ROBIN
    methods: tuple[str, ...] | None = None  # None -> any method
    # Request handling:
    timeout_seconds: float = 2.0
    max_retries: int = 0  # extra attempts after the first failure
    retry_on_statuses: frozenset[int] = frozenset({502, 503, 504})
    # Circuit breaker (failure/timeout driven ejection):
    failure_threshold: int = 3
    open_state_seconds: float = 5.0
    half_open_max_probe_requests: int = 1
    half_open_success_threshold: int = 1
    # Active probing:
    probe_path: str = "/health"
    probe_interval_seconds: float = 1.0
    probe_timeout_seconds: float = 1.0


@dataclass(frozen=True)
class ClientRateRule:
    # Per-client rate limit, keyed by client id (header X-Client-Id / host).
    requests_per_window: int = 0  # 0 disables
    window_seconds: float = 1.0
    max_concurrency: int = 0  # 0 disables


@dataclass(frozen=True)
class GatewayConfig:
    routes: tuple[RouteRule, ...] = ()
    upstreams: tuple[UpstreamConfig, ...] = ()
    rate: ClientRateRule = field(default_factory=ClientRateRule)
    max_body_bytes: int = 1_048_576


def demo_config() -> GatewayConfig:
    """Deterministic local demo topology.

    Upstreams:
      * echo-a / echo-b – healthy echo services (round-robin route);
      * echo-c         – participates in the least-inflight route;
      * flaky          – always fails (drives passive ejection);
      * slow           – always sleeps (drives timeout + circuit open).

    Rate rule: 10 requests / 2 s window, max 4 concurrent, per client.
    """
    upstreams = (
        UpstreamConfig(id="echo-a", base_url="stub://echo-a"),
        UpstreamConfig(id="echo-b", base_url="stub://echo-b"),
        UpstreamConfig(id="echo-c", base_url="stub://echo-c"),
        UpstreamConfig(id="flaky", base_url="stub://flaky"),
        UpstreamConfig(id="slow", base_url="stub://slow"),
    )
    routes = (
        RouteRule(
            id="echo-rr",
            prefix="/v1/echo",
            upstream_ids=("echo-a", "echo-b"),
            lb_policy=LBPolicy.ROUND_ROBIN,
            timeout_seconds=1.0,
            max_retries=1,
            failure_threshold=3,
            open_state_seconds=3.0,
            probe_interval_seconds=0.2,
            probe_path="/health",
        ),
        RouteRule(
            id="echo-li",
            prefix="/v1/load",
            upstream_ids=("echo-a", "echo-b", "echo-c"),
            lb_policy=LBPolicy.LEAST_INFLIGHT,
            timeout_seconds=1.0,
            max_retries=0,
            failure_threshold=3,
            open_state_seconds=3.0,
            probe_interval_seconds=0.2,
            probe_path="/health",
        ),
        RouteRule(
            id="flaky-route",
            prefix="/v1/flaky",
            upstream_ids=("flaky",),
            lb_policy=LBPolicy.ROUND_ROBIN,
            timeout_seconds=1.0,
            max_retries=0,
            failure_threshold=2,
            open_state_seconds=2.0,
            half_open_success_threshold=1,
            probe_interval_seconds=0.2,
            probe_path="/health",
        ),
        RouteRule(
            id="slow-route",
            prefix="/v1/slow",
            upstream_ids=("slow",),
            lb_policy=LBPolicy.ROUND_ROBIN,
            timeout_seconds=0.3,
            max_retries=0,
            failure_threshold=2,
            open_state_seconds=2.0,
            half_open_success_threshold=1,
            probe_interval_seconds=0.2,
            probe_path="/health",
        ),
    )
    return GatewayConfig(
        routes=routes,
        upstreams=upstreams,
        rate=ClientRateRule(
            requests_per_window=10,
            window_seconds=2.0,
            max_concurrency=4,
        ),
    )
