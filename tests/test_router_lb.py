"""Routing rules and load-balancer policy behaviour/explanation."""
from __future__ import annotations

import random

import pytest

from gateway.config import LBPolicy, RouteRule
from gateway.loadbalancer import LoadBalancer
from gateway.models import Request
from gateway.router import Router


def rule(policy=LBPolicy.ROUND_ROBIN, ids=("a", "b", "c"), rid="r"):
    return RouteRule(id=rid, prefix="/v1", upstream_ids=ids, lb_policy=policy)


# -- router --------------------------------------------------------------

def test_first_matching_rule_wins():
    rules = (
        RouteRule(id="specific", prefix="/v1/users", upstream_ids=("a",)),
        RouteRule(id="general", prefix="/v1", upstream_ids=("b",)),
    )
    router = Router(rules)
    assert router.match(Request("GET", "/v1/users/1")).id == "specific"
    assert router.match(Request("GET", "/v1/orders")).id == "general"


def test_segment_boundary_prefix():
    router = Router((RouteRule(id="r", prefix="/v1", upstream_ids=("a",)),))
    assert router.match(Request("GET", "/v1")).id == "r"
    assert router.match(Request("GET", "/v1/x")).id == "r"
    with pytest.raises(Exception) as ei:
        router.match(Request("GET", "/v10/x"))
    assert ei.value.code.value == "ROUTE_NOT_FOUND"


def test_method_filtering_and_distinguishable_404():
    router = Router((
        RouteRule(id="post-only", prefix="/w", upstream_ids=("a",), methods=("POST",)),
    ))
    assert router.match(Request("POST", "/w")).id == "post-only"
    with pytest.raises(Exception) as ei:
        router.match(Request("GET", "/w"))
    err = ei.value
    assert err.code.value == "ROUTE_NOT_FOUND"
    # explanation records why each rule was rejected
    tried = err.details["tried"]
    assert tried[0]["prefix_match"] is True and tried[0]["method_match"] is False

    explain = router.explain(Request("GET", "/zzz"))
    assert explain["matched"] is False


# -- load balancer -------------------------------------------------------

def test_round_robin_is_deterministic():
    lb = LoadBalancer()
    r = rule()
    picks = []
    for _ in range(7):
        uid, info = lb.acquire(Request("GET", "/v1"), r, ["a", "b", "c"])
        picks.append(uid)
        lb.release("r", uid)
        assert info["policy"] == "round_robin"
        assert info["position"] == info["counter"] % 3
    assert picks == ["a", "b", "c", "a", "b", "c", "a"]


def test_random_policy_explains_roll():
    lb = LoadBalancer(rng=random.Random(7))
    r = rule(LBPolicy.RANDOM)
    uid, info = lb.acquire(Request("GET", "/v1"), r, ["a", "b", "c"])
    assert uid == ["a", "b", "c"][info["roll"]]
    assert info["candidate_count"] == 3


def test_least_inflight_prefers_idle_then_order_tie():
    lb = LoadBalancer()
    r = rule(LBPolicy.LEAST_INFLIGHT)
    # acquire a twice, b once -> next must select c
    u1, _ = lb.acquire(Request("GET", "/v1"), r, ["a", "b", "c"])
    u2, _ = lb.acquire(Request("GET", "/v1"), r, ["a", "b", "c"])
    assert {u1, u2} == {"a", "b"}
    u3, info = lb.acquire(Request("GET", "/v1"), r, ["a", "b", "c"])
    assert u3 == "c"
    assert info["inflight"] == {"a": 1, "b": 1, "c": 0}
    lb.release("r", "a"); lb.release("r", "b"); lb.release("r", "c")
    # all zero again -> tie broken by config order
    uid, _ = lb.acquire(Request("GET", "/v1"), r, ["a", "b", "c"])
    assert uid == "a"


def test_excluded_candidates_not_selected():
    lb = LoadBalancer()
    r = rule()
    uid, info = lb.acquire(Request("GET", "/v1"), r, ["a", "b", "c"],
                           exclude=frozenset({"a", "b"}))
    assert uid == "c"
    assert info["excluded"] == ["a", "b"]
    none, info = lb.acquire(Request("GET", "/v1"), r, ["a"],
                            exclude=frozenset({"a"}))
    assert none is None and info["reason"] == "no_candidate_available"
