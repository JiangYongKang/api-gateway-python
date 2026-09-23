"""判定日志必须包含：请求信息、判定依据、上游健康/熔断状态。"""

from __future__ import annotations

from tests.conftest import (
    ScriptedUpstream,
    build_config,
    decision_stages,
    make_proxy,
)
from tests.conftest import FakeClock


async def test_decision_logs_cover_request_reason_and_upstream_state(caplog):
    clock = FakeClock()
    upstream = ScriptedUpstream({"u1": "connect", "u2": "ok"})
    proxy, _, _ = make_proxy(
        build_config(failure_threshold=1, max_retries=0), upstream, clock
    )

    try:
        await proxy.handle("GET", "/svc/orders", {"X-Client-Id": "c-7"}, b"",
                           request_id="req-123")
    except Exception:
        pass

    stages = decision_stages(caplog)
    # 完整判定链各阶段都有日志
    for expected in ["route_match", "rate_limit", "lb_select",
                     "upstream_call", "health_update"]:
        assert expected in stages, stages

    fields_by_stage = {}
    for record in caplog.records:
        f = getattr(record, "gateway_fields", None)
        if f:
            fields_by_stage.setdefault(f["stage"], f)

    # 路由阶段：请求方法/路径 + 依据 + 上游状态
    rm = fields_by_stage["route_match"]
    assert rm["method"] == "GET" and rm["path"] == "/svc/orders"
    assert "reason" in rm and "health" in rm and "circuit" in rm
    assert rm["request_id"] == "req-123"

    # 限流阶段：客户端维度与判定
    rl = fields_by_stage["rate_limit"]
    assert rl["client_id"] == "c-7" and rl["decision"] == "allowed"

    # 选实例阶段：明确依据
    lb = fields_by_stage["lb_select"]
    assert lb["upstream"] == "u1" and "round_robin" in lb["reason"]

    # 失败后健康状态变化被记录
    hu = fields_by_stage["health_update"]
    assert hu["event"] == "failure"
    assert hu["health"]["u1"] == "unhealthy"
