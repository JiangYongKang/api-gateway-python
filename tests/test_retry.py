"""重试规则：幂等、换实例、令牌/并发不重复计数；非幂等不重试。"""

from __future__ import annotations

import pytest

from gateway.errors import GatewayErrorType
from tests.conftest import ScriptedUpstream, build_config, make_proxy


async def test_idempotent_request_retries_on_another_upstream_and_succeeds(clock):
    cfg = build_config(max_retries=1, lb_strategy="round_robin")
    upstream = ScriptedUpstream({"u1": "connect", "u2": "ok"})
    proxy, _, _ = make_proxy(cfg, upstream, clock)

    out = await proxy.handle("GET", "/svc/x", {}, b"")
    assert out.status_code == 200
    assert out.upstream_id == "u2"
    assert out.attempts == 2
    # 两次尝试分别落在不同实例
    assert [c["upstream_id"] for c in upstream.sender.calls] == ["u1", "u2"]


async def test_non_idempotent_post_is_not_retried(clock):
    cfg = build_config(max_retries=2, lb_strategy="round_robin")
    upstream = ScriptedUpstream({"u1": "connect", "u2": "ok"})
    proxy, _, _ = make_proxy(cfg, upstream, clock)

    with pytest.raises(Exception) as exc:
        await proxy.handle("POST", "/svc/x", {}, b'{"id":1}')
    assert exc.value.error_type is GatewayErrorType.UPSTREAM_ERROR
    # POST 只调用一次，无额外副作用
    assert len(upstream.sender.calls) == 1
    assert upstream.sender.calls[0]["method"] == "POST"


async def test_retry_does_not_reconsume_rate_limit_tokens(clock):
    cfg = build_config(
        max_retries=1,
        rate_limit={"requests_per_second": 1.0, "max_concurrent": 0},
    )
    upstream = ScriptedUpstream({"u1": "connect", "u2": "ok"})
    proxy, _, _ = make_proxy(cfg, upstream, clock)

    # 第一次请求（含一次重试）成功；重试不重复扣令牌
    out = await proxy.handle("GET", "/svc/x", {}, b"")
    assert out.attempts == 2
    # 第二次请求应被限流（令牌已被第一次请求用完），证明只扣了一次
    with pytest.raises(Exception) as exc:
        await proxy.handle("GET", "/svc/x", {}, b"")
    assert exc.value.error_type is GatewayErrorType.RATE_LIMITED


async def test_retry_does_not_reuse_failed_upstream(clock):
    # 单上游 + 1 次重试：第二次选择无健康实例 -> 503，不重复打同一实例
    cfg = build_config(max_retries=1)
    cfg["routes"][0]["upstreams"] = [{"id": "u1", "base_url": "http://up1"}]
    upstream = ScriptedUpstream({"u1": "connect"})
    proxy, _, _ = make_proxy(cfg, upstream, clock)

    with pytest.raises(Exception) as exc:
        await proxy.handle("GET", "/svc/x", {}, b"")
    # 第一次尝试真实失败；第二次因无其它健康实例不重复调用
    assert exc.value.error_type is GatewayErrorType.NO_AVAILABLE_UPSTREAM
    assert len(upstream.sender.calls) == 1
    assert exc.value.details["tried"] == ["u1"]


async def test_retry_exhaustion_reports_all_tried(clock):
    cfg = build_config(max_retries=1)
    upstream = ScriptedUpstream({"u1": 503, "u2": 503})
    proxy, _, _ = make_proxy(cfg, upstream, clock)

    with pytest.raises(Exception) as exc:
        await proxy.handle("GET", "/svc/x", {}, b"")
    # 最终错误仍是可区分的上游状态码错误，且记录两个尝试实例
    assert exc.value.error_type is GatewayErrorType.UPSTREAM_BAD_STATUS
    assert set(exc.value.details["tried"]) == {"u1", "u2"}
    assert exc.value.details["attempts"] == 2
