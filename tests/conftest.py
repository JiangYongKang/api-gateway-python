"""测试共享工具：可编排的假上游与时间。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.config_loader import load_config
from gateway.proxy import GatewayProxy
from gateway.upstream import FakeSender, UpstreamClient, UpstreamResult


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class ScriptedUpstream:
    """按 upstream_id 编排行为的假上游。

    behaviors[uid] 可以是：
    - "ok" / "timeout" / "connect"
    - int（作为 5xx/4xx 状态码）
    - 上述值组成的列表（按调用次数依次返回，最后一项持续生效）
    """

    def __init__(self, behaviors: dict) -> None:
        self.behaviors = behaviors
        self.sender = FakeSender(self._handle)
        self.lock = asyncio.Lock()

    async def _handle(self, uid, base_url, method, path, headers, body):
        async with self.lock:
            spec = self.behaviors.get(uid, "ok")
            if isinstance(spec, list):
                step = spec[min(self._count(uid), len(spec) - 1)]
            else:
                step = spec
            self._bump(uid)
        return _make_result(uid, step)

    def _key(self, uid: str) -> str:
        return f"__n_{uid}"

    def _count(self, uid: str) -> int:
        return getattr(self, self._key(uid), 0)

    def _bump(self, uid: str) -> None:
        setattr(self, self._key(uid), self._count(uid) + 1)


def _make_result(uid: str, step: object) -> UpstreamResult:
    if step == "ok":
        return UpstreamResult(ok=True, status_code=200, body=b"ok", upstream_id=uid)
    if step == "timeout":
        from gateway.upstream import UpstreamFailureKind

        return UpstreamResult(
            ok=False, failure=UpstreamFailureKind.TIMEOUT, upstream_id=uid
        )
    if step == "connect":
        from gateway.upstream import UpstreamFailureKind

        return UpstreamResult(
            ok=False, failure=UpstreamFailureKind.CONNECT, upstream_id=uid
        )
    code = int(step)
    from gateway.upstream import UpstreamFailureKind

    return UpstreamResult(
        ok=code < 500,
        status_code=code,
        body=f"status-{code}".encode(),
        failure=UpstreamFailureKind.BAD_STATUS if code >= 500 else None,
        upstream_id=uid,
    )


def build_config(**overrides) -> dict:
    cfg = {
        "routes": [
            {
                "id": "svc",
                "path_prefix": "/svc",
                "lb_strategy": "round_robin",
                "failure_threshold": 3,
                "probe_interval": 0.2,
                "probe_path": "/healthz",
                "circuit_threshold": 4,
                "circuit_open_seconds": 0.3,
                "timeout_seconds": 0.5,
                "max_retries": 0,
                "upstreams": [
                    {"id": "u1", "base_url": "http://up1"},
                    {"id": "u2", "base_url": "http://up2"},
                ],
            }
        ]
    }
    if "rate_limit" in overrides:
        cfg["rate_limit"] = overrides.pop("rate_limit")
    if overrides:
        cfg["routes"][0].update(overrides)
    return cfg


def make_proxy(
    config_data: dict,
    upstream: ScriptedUpstream,
    clock: FakeClock | None = None,
    probe_upstream: ScriptedUpstream | None = None,
):
    clock = clock or FakeClock()
    cfg = load_config(config_data)
    client = UpstreamClient(upstream.sender)
    probe_sender = (
        probe_upstream.sender if probe_upstream is not None else upstream.sender
    )
    proxy = GatewayProxy(cfg, client, probe_sender=probe_sender, clock=clock)
    return proxy, cfg, clock


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture(autouse=True)
def _gateway_logs_to_caplog(caplog):
    """让网关结构化日志在测试中向上传播，便于断言判定依据。"""
    from gateway.logging_utils import get_gateway_logger

    logger = get_gateway_logger()
    logger.propagate = True
    caplog.set_level("INFO", logger="gateway")
    yield
    logger.propagate = False


def decision_stages(caplog) -> list[str]:
    stages = []
    for record in caplog.records:
        fields = getattr(record, "gateway_fields", None)
        if fields:
            stages.append(fields.get("stage", ""))
    return stages
