"""从 dict（通常来自 JSON/YAML）构建 GatewayConfig，便于本地配置与测试。"""

from __future__ import annotations

from .config import (
    GatewayConfig,
    LBStrategy,
    RateLimitConfig,
    RouteConfig,
    UpstreamConfig,
)
from .errors import GatewayError, GatewayErrorType


def _require(mapping: dict, key: str, ctx: str) -> object:
    if key not in mapping:
        raise GatewayError(
            GatewayErrorType.INTERNAL_ERROR,
            f"配置缺少字段 {ctx}.{key}",
        )
    return mapping[key]


def load_config(data: dict) -> GatewayConfig:
    """从普通 dict 加载网关配置。未知策略会抛错而不是静默回退。"""
    routes_raw = data.get("routes", [])
    routes: list[RouteConfig] = []
    for r in routes_raw:
        ups = tuple(
            UpstreamConfig(
                id=str(_require(u, "id", f"routes[{r.get('id')}].upstreams")),
                base_url=str(
                    _require(u, "base_url", f"routes[{r.get('id')}].upstreams")
                ),
                weight=int(u.get("weight", 1)),
            )
            for u in _require(r, "upstreams", f"routes[{r.get('id')}]")
        )
        strategy_name = r.get("lb_strategy", LBStrategy.ROUND_ROBIN.value)
        try:
            strategy = LBStrategy(strategy_name)
        except ValueError as exc:
            raise GatewayError(
                GatewayErrorType.INTERNAL_ERROR,
                f"未知负载均衡策略: {strategy_name!r}",
                details={"route": r.get("id")},
            ) from exc
        routes.append(
            RouteConfig(
                id=str(_require(r, "id", "routes")),
                path_prefix=str(_require(r, "path_prefix", f"routes[{r}]")),
                upstreams=ups,
                lb_strategy=strategy,
                failure_threshold=int(r.get("failure_threshold", 3)),
                probe_interval=float(r.get("probe_interval", 1.0)),
                probe_path=str(r.get("probe_path", "/")),
                circuit_threshold=int(r.get("circuit_threshold", 5)),
                circuit_open_seconds=float(r.get("circuit_open_seconds", 2.0)),
                timeout_seconds=float(r.get("timeout_seconds", 1.0)),
                max_retries=int(r.get("max_retries", 0)),
                retry_on_status=tuple(r.get("retry_on_status", (502, 503, 504))),
            )
        )
    rl_raw = data.get("rate_limit", {})
    rl = RateLimitConfig(
        requests_per_second=float(rl_raw.get("requests_per_second", 0.0)),
        max_concurrent=int(rl_raw.get("max_concurrent", 0)),
    )
    return GatewayConfig(
        routes=tuple(routes),
        rate_limit=rl,
        client_id_header=str(data.get("client_id_header", "X-Client-Id")),
        default_client_id=str(data.get("default_client_id", "anonymous")),
    )


def default_demo_config() -> GatewayConfig:
    """本地演示用配置：两条路由、多个上游、开启限流/熔断。"""
    return load_config(
        {
            "rate_limit": {"requests_per_second": 20, "max_concurrent": 10},
            "routes": [
                {
                    "id": "users",
                    "path_prefix": "/users",
                    "lb_strategy": "round_robin",
                    "failure_threshold": 3,
                    "probe_interval": 0.5,
                    "circuit_threshold": 4,
                    "circuit_open_seconds": 1.0,
                    "timeout_seconds": 0.5,
                    "max_retries": 1,
                    "upstreams": [
                        {"id": "users-1", "base_url": "http://127.0.0.1:18081"},
                        {"id": "users-2", "base_url": "http://127.0.0.1:18082", "weight": 1},
                    ],
                },
                {
                    "id": "orders",
                    "path_prefix": "/orders",
                    "lb_strategy": "least_connections",
                    "upstreams": [
                        {"id": "orders-1", "base_url": "http://127.0.0.1:18091"},
                    ],
                },
            ],
        }
    )
