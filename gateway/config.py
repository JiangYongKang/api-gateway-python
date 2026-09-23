"""网关配置的数据模型：路由 / 上游 / 策略参数。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class LBStrategy(str, Enum):
    """负载均衡策略。"""

    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"


@dataclass(frozen=True)
class UpstreamConfig:
    """单个上游实例配置。"""

    id: str
    base_url: str
    weight: int = 1


@dataclass(frozen=True)
class RouteConfig:
    """一条路由规则：path 前缀匹配，转发到一组上游。"""

    id: str
    path_prefix: str
    upstreams: tuple[UpstreamConfig, ...]
    lb_strategy: LBStrategy = LBStrategy.ROUND_ROBIN
    # 健康检查
    failure_threshold: int = 3
    probe_interval: float = 1.0
    probe_path: str = "/"
    # 熔断
    circuit_threshold: int = 5
    circuit_open_seconds: float = 2.0
    # 上游调用
    timeout_seconds: float = 1.0
    max_retries: int = 0
    retry_on_status: tuple[int, ...] = (502, 503, 504)


@dataclass(frozen=True)
class RateLimitConfig:
    """按客户端维度的限流与并发控制。"""

    requests_per_second: float = 0.0  # 0 表示不限
    max_concurrent: int = 0  # 0 表示不限


@dataclass(frozen=True)
class GatewayConfig:
    """网关总配置。"""

    routes: tuple[RouteConfig, ...] = ()
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    client_id_header: str = "X-Client-Id"
    default_client_id: str = "anonymous"


def validate_config(config: GatewayConfig) -> None:
    """校验配置，非法时抛 ValueError。"""
    seen_route_ids: set[str] = set()
    for route in config.routes:
        if not route.id:
            raise ValueError("route id must be non-empty")
        if route.id in seen_route_ids:
            raise ValueError(f"duplicate route id: {route.id}")
        seen_route_ids.add(route.id)
        if not route.path_prefix.startswith("/"):
            raise ValueError(f"route {route.id}: path_prefix must start with '/'")
        if not route.upstreams:
            raise ValueError(f"route {route.id}: at least one upstream required")
        seen_up_ids: set[str] = set()
        for up in route.upstreams:
            if not up.id:
                raise ValueError(f"route {route.id}: upstream id must be non-empty")
            if up.id in seen_up_ids:
                raise ValueError(
                    f"route {route.id}: duplicate upstream id: {up.id}"
                )
            seen_up_ids.add(up.id)
            if not up.base_url.startswith(("http://", "https://")):
                raise ValueError(
                    f"upstream {up.id}: base_url must be http(s) URL"
                )
            if up.weight < 1:
                raise ValueError(f"upstream {up.id}: weight must be >= 1")
        if route.failure_threshold < 1:
            raise ValueError(f"route {route.id}: failure_threshold must be >= 1")
        if route.probe_interval <= 0:
            raise ValueError(f"route {route.id}: probe_interval must be > 0")
        if route.circuit_threshold < 1:
            raise ValueError(f"route {route.id}: circuit_threshold must be >= 1")
        if route.circuit_open_seconds <= 0:
            raise ValueError(f"route {route.id}: circuit_open_seconds must be > 0")
        if route.timeout_seconds <= 0:
            raise ValueError(f"route {route.id}: timeout_seconds must be > 0")
        if route.max_retries < 0:
            raise ValueError(f"route {route.id}: max_retries must be >= 0")
    rl = config.rate_limit
    if rl.requests_per_second < 0:
        raise ValueError("rate_limit.requests_per_second must be >= 0")
    if rl.max_concurrent < 0:
        raise ValueError("rate_limit.max_concurrent must be >= 0")
