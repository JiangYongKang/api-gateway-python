"""上游连续失败摘除、探测恢复；摘除期间不得发送业务流量。"""

from __future__ import annotations


from gateway.errors import GatewayErrorType
from tests.conftest import ScriptedUpstream, build_config, make_proxy


async def _request(proxy, expect_error: bool = False):
    try:
        return await proxy.handle("GET", "/svc/x", {}, b"")
    except Exception as exc:
        if expect_error:
            return exc
        raise


async def test_consecutive_failures_eject_upstream(clock):
    # 单上游：failure_threshold=3，前两次不摘除，第三次摘除
    cfg = build_config(failure_threshold=3, max_retries=0)
    cfg["routes"][0]["upstreams"] = [{"id": "u1", "base_url": "http://up1"}]
    upstream = ScriptedUpstream({"u1": "connect"})
    proxy, _, _ = make_proxy(cfg, upstream, clock)
    rt = proxy.runtimes["svc"]

    for _ in range(2):
        await _request(proxy, expect_error=True)
        assert rt.health.get("u1").state.value == "healthy"
        clock.now += 0.01

    await _request(proxy, expect_error=True)
    assert rt.health.get("u1").state.value == "unhealthy"


async def test_traffic_never_goes_to_ejected_upstream(clock):
    # 两上游：u1 一次失败即摘除，之后所有业务流量只去 u2
    cfg = build_config(failure_threshold=1, max_retries=0)
    upstream = ScriptedUpstream({"u1": "connect", "u2": "ok"})
    proxy, _, _ = make_proxy(cfg, upstream, clock)
    rt = proxy.runtimes["svc"]

    await _request(proxy, expect_error=True)  # 轮询首命中 u1 -> 摘除
    assert rt.health.snapshot() == {"u1": "unhealthy", "u2": "healthy"}

    for _ in range(4):
        out = await proxy.handle("GET", "/svc/x", {}, b"")
        assert out.upstream_id == "u2"


async def test_success_resets_failure_count(clock):
    # 只有一个上游，失败/成功都落在 u1
    cfg = build_config(failure_threshold=2, max_retries=0)
    cfg["routes"][0]["upstreams"] = [{"id": "u1", "base_url": "http://up1"}]
    upstream = ScriptedUpstream({"u1": ["connect", "ok", "connect", "ok"]})
    proxy, _, _ = make_proxy(cfg, upstream, clock)
    rt = proxy.runtimes["svc"]

    await _request(proxy, expect_error=True)
    assert rt.health.get("u1").consecutive_failures == 1
    await proxy.handle("GET", "/svc/x", {}, b"")  # 成功：计数清零
    assert rt.health.get("u1").consecutive_failures == 0
    assert rt.health.get("u1").state.value == "healthy"


async def test_all_unhealthy_returns_distinct_error(clock):
    cfg = build_config(failure_threshold=1, max_retries=0)
    upstream = ScriptedUpstream({"u1": "connect", "u2": "timeout"})
    proxy, _, _ = make_proxy(cfg, upstream, clock)
    rt = proxy.runtimes["svc"]

    # 两次请求分别打挂两个实例
    await _request(proxy, expect_error=True)
    await _request(proxy, expect_error=True)
    assert set(rt.health.snapshot().values()) == {"unhealthy"}

    exc = await _request(proxy, expect_error=True)
    assert exc.error_type is GatewayErrorType.NO_AVAILABLE_UPSTREAM
    assert exc.status_code == 503
    assert exc.details["health"] == {"u1": "unhealthy", "u2": "unhealthy"}


async def test_probe_recovers_ejected_upstream_on_deterministic_rule(clock):
    # u1 是唯一上游，摘除后无业务流量可发；探测成功后恢复接入
    cfg = build_config(failure_threshold=1, probe_interval=0.2, max_retries=0)
    cfg["routes"][0]["upstreams"] = [{"id": "u1", "base_url": "http://up1"}]
    upstream = ScriptedUpstream({"u1": ["connect", "ok", "ok"]})
    proxy, _, _ = make_proxy(cfg, upstream, clock)
    rt = proxy.runtimes["svc"]

    await _request(proxy, expect_error=True)
    assert rt.health.get("u1").state.value == "unhealthy"

    # 冷却未到：不探测
    clock.now += 0.1
    assert await proxy.probe_sweep(clock.now) == {}

    # 冷却到期：探测成功 -> 恢复 HEALTHY，业务流量重新接入
    clock.now += 0.11
    report = await proxy.probe_sweep(clock.now)
    assert report["svc"] == {"u1": "healthy"}
    out = await proxy.handle("GET", "/svc/x", {}, b"")
    assert out.upstream_id == "u1"
    assert rt.health.get("u1").state.value == "healthy"


async def test_failed_probe_re_ejects_and_waits_another_cooldown(clock):
    cfg = build_config(failure_threshold=1, probe_interval=0.2, max_retries=0)
    cfg["routes"][0]["upstreams"] = [{"id": "u1", "base_url": "http://up1"}]
    upstream = ScriptedUpstream({"u1": "connect"})
    # 探测流量独立且始终失败，与业务脚本互不干扰
    probe = ScriptedUpstream({"u1": "connect", "probe:http://up1": "connect"})
    proxy, _, _ = make_proxy(cfg, upstream, clock, probe_upstream=probe)
    rt = proxy.runtimes["svc"]

    await _request(proxy, expect_error=True)
    assert rt.health.get("u1").state.value == "unhealthy"

    clock.now += 0.21
    report = await proxy.probe_sweep(clock.now)  # 探测仍失败
    assert report["svc"] == {"u1": "unhealthy"}

    # 立刻再扫一次：必须重新等冷却，不重复探测
    assert await proxy.probe_sweep(clock.now) == {}


async def test_probe_uses_dedicated_path(clock):
    """探测走独立的探测路径，与业务路径隔离。"""
    cfg = build_config(failure_threshold=1, probe_interval=0.2, max_retries=0)
    cfg["routes"][0]["upstreams"] = [{"id": "u1", "base_url": "http://up1"}]
    upstream = ScriptedUpstream({"u1": ["connect", "ok"]})
    proxy, _, _ = make_proxy(cfg, upstream, clock)

    await _request(proxy, expect_error=True)
    business_paths = [c["path"] for c in upstream.sender.calls]
    assert business_paths == ["/svc/x"]

    clock.now += 0.21
    await proxy.probe_sweep(clock.now)
    probe_call = upstream.sender.calls[-1]
    assert probe_call["path"] == "/healthz"
    assert probe_call["method"] == "GET"
