"""超时/连续失败触发熔断；熔断错误、超时错误与普通上游错误必须可区分。"""

from __future__ import annotations

import pytest

from gateway.circuit import CircuitBreaker, CircuitState
from gateway.errors import GatewayErrorType
from tests.conftest import ScriptedUpstream, build_config, make_proxy


def test_circuit_breaker_state_machine():
    cb = CircuitBreaker(threshold=2, open_seconds=0.5)
    assert cb.allow_request("u1", 0.0) is True
    cb.record_failure("u1", 0.1)
    assert cb.record_failure("u1", 0.2) is CircuitState.OPEN
    # OPEN 期间拒绝
    assert cb.allow_request("u1", 0.3) is False
    # 冷却到期：放一个试探请求进入 HALF_OPEN
    assert cb.allow_request("u1", 0.8) is True
    # 试探期间拒绝其余请求
    assert cb.allow_request("u1", 0.81) is False
    # 试探失败：重新 OPEN
    assert cb.record_failure("u1", 0.82) is CircuitState.OPEN
    # 再次冷却后试探成功：CLOSED
    assert cb.allow_request("u1", 1.4) is True
    assert cb.record_success("u1", 1.41) is CircuitState.CLOSED
    assert cb.allow_request("u1", 1.42) is True


async def test_timeout_returns_distinct_504(clock):
    cfg = build_config(max_retries=0, timeout_seconds=0.5)
    cfg["routes"][0]["upstreams"] = [{"id": "u1", "base_url": "http://up1"}]
    upstream = ScriptedUpstream({"u1": "timeout"})
    proxy, _, _ = make_proxy(cfg, upstream, clock)

    with pytest.raises(Exception) as exc:
        await proxy.handle("GET", "/svc/x", {}, b"")
    assert exc.value.error_type is GatewayErrorType.UPSTREAM_TIMEOUT
    assert exc.value.status_code == 504
    assert exc.value.details["timeout_seconds"] == 0.5


async def test_bad_status_returns_distinct_502(clock):
    cfg = build_config(max_retries=0)
    cfg["routes"][0]["upstreams"] = [{"id": "u1", "base_url": "http://up1"}]
    upstream = ScriptedUpstream({"u1": 500})
    proxy, _, _ = make_proxy(cfg, upstream, clock)

    with pytest.raises(Exception) as exc:
        await proxy.handle("GET", "/svc/x", {}, b"")
    assert exc.value.error_type is GatewayErrorType.UPSTREAM_BAD_STATUS
    assert exc.value.status_code == 502
    assert exc.value.details["upstream_status"] == 500


async def test_client_4xx_is_passthrough_not_failure(clock):
    cfg = build_config(max_retries=0)
    cfg["routes"][0]["upstreams"] = [{"id": "u1", "base_url": "http://up1"}]
    upstream = ScriptedUpstream({"u1": 404})
    proxy, _, _ = make_proxy(cfg, upstream, clock)
    rt = proxy.runtimes["svc"]

    out = await proxy.handle("GET", "/svc/missing", {}, b"")
    assert out.status_code == 404
    # 4xx 不计入失败/熔断
    assert rt.circuit.snapshot() == {"u1": "closed"}
    assert rt.health.get("u1").consecutive_failures == 0


async def test_consecutive_timeouts_open_circuit_with_distinct_error(clock):
    cfg = build_config(
        failure_threshold=99,  # 此用例只验证熔断，避免健康摘除先触发
        circuit_threshold=2,
        circuit_open_seconds=0.5,
        max_retries=0,
    )
    cfg["routes"][0]["upstreams"] = [{"id": "u1", "base_url": "http://up1"}]
    upstream = ScriptedUpstream({"u1": "timeout"})
    proxy, _, _ = make_proxy(cfg, upstream, clock)
    rt = proxy.runtimes["svc"]

    # 前两次：504 超时
    for _ in range(2):
        with pytest.raises(Exception) as exc:
            await proxy.handle("GET", "/svc/x", {}, b"")
        assert exc.value.error_type is GatewayErrorType.UPSTREAM_TIMEOUT
    assert rt.circuit.snapshot() == {"u1": "open"}

    # 熔断打开后：直接拒绝，请求不会发到上游（sender 调用次数停在 2）
    calls_before = len(upstream.sender.calls)
    with pytest.raises(Exception) as exc:
        await proxy.handle("GET", "/svc/x", {}, b"")
    assert exc.value.error_type is GatewayErrorType.CIRCUIT_OPEN
    assert exc.value.status_code == 503
    assert len(upstream.sender.calls) == calls_before


async def test_circuit_half_open_trial_success_closes(clock):
    cfg = build_config(
        failure_threshold=99,
        circuit_threshold=1,
        circuit_open_seconds=0.5,
        max_retries=0,
    )
    cfg["routes"][0]["upstreams"] = [{"id": "u1", "base_url": "http://up1"}]
    # 第一次业务失败 -> OPEN；冷却后的试探请求成功
    upstream = ScriptedUpstream({"u1": ["timeout", "ok"]})
    proxy, _, _ = make_proxy(cfg, upstream, clock)
    rt = proxy.runtimes["svc"]

    with pytest.raises(Exception):
        await proxy.handle("GET", "/svc/x", {}, b"")
    assert rt.circuit.snapshot() == {"u1": "open"}

    clock.now += 0.6
    out = await proxy.handle("GET", "/svc/x", {}, b"")
    assert out.status_code == 200
    assert rt.circuit.snapshot() == {"u1": "closed"}
