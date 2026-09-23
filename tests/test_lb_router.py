"""路由匹配与负载均衡策略的可解释、确定性选择。"""

from __future__ import annotations

import pytest

from gateway.config_loader import load_config
from gateway.loadbalancer import LoadBalancer
from gateway.router import Router
from gateway.errors import GatewayErrorType


def _route(**kw):
    data = {
        "id": kw.get("id", "r"),
        "path_prefix": kw.get("prefix", "/"),
        "lb_strategy": kw.get("strategy", "round_robin"),
        "upstreams": kw.get(
            "ups",
            [
                {"id": "u1", "base_url": "http://1", "weight": kw.get("w1", 1)},
                {"id": "u2", "base_url": "http://2", "weight": kw.get("w2", 1)},
                {"id": "u3", "base_url": "http://3", "weight": kw.get("w3", 1)},
            ],
        ),
    }
    return load_config({"routes": [data]}).routes[0]


def test_longest_prefix_match():
    cfg = load_config(
        {
            "routes": [
                {"id": "a", "path_prefix": "/svc", "upstreams": [
                    {"id": "x", "base_url": "http://x"}]},
                {"id": "b", "path_prefix": "/svc/v2", "upstreams": [
                    {"id": "y", "base_url": "http://y"}]},
            ]
        }
    )
    router = Router(cfg)
    route, reason = router.match_or_raise("GET", "/svc/v2/x")
    assert route.id == "b"
    assert "longest-prefix" in reason

    route, _ = router.match_or_raise("GET", "/svc/v1/x")
    assert route.id == "a"


def test_route_not_found_error():
    router = Router(load_config({"routes": []}))
    with pytest.raises(Exception) as exc:
        router.match_or_raise("GET", "/nope")
    assert exc.value.error_type is GatewayErrorType.ROUTE_NOT_FOUND


def test_round_robin_is_explainable_and_stable():
    lb = LoadBalancer()
    route = _route()
    picks = [lb.select(route, ["u1", "u2", "u3"], 0.0)[0] for _ in range(6)]
    assert picks == ["u1", "u2", "u3", "u1", "u2", "u3"]
    # 依据字符串包含游标与健康集合
    _, reason = lb.select(route, ["u1", "u2"], 0.0)
    assert "round_robin" in reason and "cursor" in reason


def test_round_robin_after_ejection_set_shrinks_deterministically():
    lb = LoadBalancer()
    route = _route()
    assert lb.select(route, ["u1", "u2", "u3"], 0.0)[0] == "u1"
    assert lb.select(route, ["u1", "u2", "u3"], 0.0)[0] == "u2"
    # u2 摘除：健康集合收缩，游标继续单调推进，选择确定不报错
    assert lb.select(route, ["u1", "u3"], 0.0)[0] == "u1"
    assert lb.select(route, ["u1", "u3"], 0.0)[0] == "u3"


def test_weighted_round_robin_distribution():
    lb = LoadBalancer(seed=1)
    route = _route(strategy="weighted_round_robin", w1=5, w2=1, w3=1)
    picks = [lb.select(route, ["u1", "u2"], 0.0)[0] for _ in range(6)]
    # 权重 5:1 —— u1 出现 5 次，u2 1 次，且均匀穿插
    assert sorted(picks) == ["u1"] * 5 + ["u2"]
    assert picks[0] == "u1"


def test_random_strategy_seeded_reproducible():
    route = _route(strategy="random")
    a = LoadBalancer(seed=42)
    b = LoadBalancer(seed=42)
    picks_a = [a.select(route, ["u1", "u2", "u3"], 0.0)[0] for _ in range(5)]
    picks_b = [b.select(route, ["u1", "u2", "u3"], 0.0)[0] for _ in range(5)]
    assert picks_a == picks_b


def test_least_connections_picks_minimum_and_tiebreaks_by_id():
    lb = LoadBalancer()
    route = _route(strategy="least_connections")
    assert lb.select(route, ["u1", "u2", "u3"], 0.0)[0] == "u1"
    lb.note_start("r", "u1")
    assert lb.select(route, ["u1", "u2", "u3"], 0.0)[0] == "u2"
    lb.note_completion("r", "u1")
    # u1/u2/u3 都为 0，并列按 id 字典序 -> u1
    assert lb.select(route, ["u1", "u2", "u3"], 0.0)[0] == "u1"


def test_empty_healthy_set_returns_none():
    lb = LoadBalancer()
    chosen, reason = lb.select(_route(), [], 0.0)
    assert chosen is None and reason == "no healthy upstream"
