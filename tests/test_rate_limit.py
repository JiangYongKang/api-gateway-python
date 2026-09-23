"""按客户端的限流与并发控制；并发下计数必须一致，拒绝原因可区分。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.errors import GatewayErrorType
from gateway.ratelimit import RateLimiter


async def test_token_bucket_rate_limit_rejects_with_distinct_code(clock):
    rl = RateLimiter(requests_per_second=2, max_concurrent=0)
    results = []
    for _ in range(3):
        ok, reason, _ = await rl.try_acquire("c1", clock())
        results.append((ok, reason))
    assert results[0][0] and results[1][0]
    assert results[2] == (False, "rate_limited")

    # 补充令牌后恢复
    clock.now += 1.0
    ok, reason, _ = await rl.try_acquire("c1", clock())
    assert ok and reason == "allowed"


async def test_concurrency_limit_release_allows_reentry(clock):
    rl = RateLimiter(requests_per_second=0, max_concurrent=2)
    for _ in range(2):
        ok, reason, _ = await rl.try_acquire("c1", clock())
        assert (ok, reason) == (True, "allowed")
    ok, reason, _ = await rl.try_acquire("c1", clock())
    assert (ok, reason) == (False, "concurrency_limited")
    await rl.release("c1")
    ok, _, _ = await rl.try_acquire("c1", clock())
    assert ok


async def test_limits_are_per_client(clock):
    rl = RateLimiter(requests_per_second=1, max_concurrent=1)
    ok_a, _, _ = await rl.try_acquire("a", clock())
    ok_b, _, _ = await rl.try_acquire("b", clock())
    assert ok_a and ok_b
    _, reason_a, _ = await rl.try_acquire("a", clock())
    _, reason_b, _ = await rl.try_acquire("b", clock())
    assert reason_a == reason_b == "concurrency_limited"


async def test_concurrent_acquires_count_consistently():
    """50 个并发请求争抢 10 个并发名额：恰好 10 个通过、40 个被拒。"""
    rl = RateLimiter(requests_per_second=0, max_concurrent=10)
    allowed_count = 0
    denied_count = 0
    lock = asyncio.Lock()

    async def one():
        nonlocal allowed_count, denied_count
        ok, reason, _ = await rl.try_acquire("hot-client", 1.0)
        async with lock:
            if ok:
                allowed_count += 1
            else:
                assert reason == "concurrency_limited"
                denied_count += 1
        if ok:
            await asyncio.sleep(0)  # 不释放，保持在途
        return ok

    results = await asyncio.gather(*(one() for _ in range(50)))
    assert sum(results) == 10
    assert allowed_count == 10 and denied_count == 40
    snap = await rl.snapshot()
    assert snap["hot-client"]["in_flight"] == 10


async def test_gateway_rejects_rate_limited_request(clock, caplog):
    from tests.conftest import ScriptedUpstream, build_config, make_proxy
    from tests.conftest import decision_stages

    cfg = build_config(rate_limit={"requests_per_second": 1, "max_concurrent": 0})
    upstream = ScriptedUpstream({"u1": "ok", "u2": "ok"})
    proxy, _, _ = make_proxy(cfg, upstream, clock)

    ok_out = await proxy.handle("GET", "/svc/x", {}, b"")
    assert ok_out.status_code == 200
    with pytest.raises(Exception) as exc:
        await proxy.handle("GET", "/svc/x", {}, b"")
    assert exc.value.error_type is GatewayErrorType.RATE_LIMITED
    assert exc.value.status_code == 429
    # 判定日志必须包含请求、判定依据
    assert "rate_limit" in decision_stages(caplog)
