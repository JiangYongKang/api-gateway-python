"""上游健康状态管理：连续失败摘除，健康探测后按确定性规则恢复。

状态机（每个上游独立）：

    HEALTHY --连续失败>=threshold--> UNHEALTHY
    UNHEALTHY --冷却 probe_interval--> SUSPECT（仅允许探测流量）
    SUSPECT --探测成功--> HEALTHY
    SUSPECT --探测失败--> UNHEALTHY（重新计时）

业务流量只允许发往 HEALTHY；SUSPECT 实例只接受健康探测，
且探测使用独立的 sender，因此探测与业务流量互不干扰。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from .config import UpstreamConfig


class HealthState(str, Enum):
    HEALTHY = "healthy"
    SUSPECT = "suspect"  # 半开：仅允许探测流量
    UNHEALTHY = "unhealthy"


@dataclass
class UpstreamHealth:
    config: UpstreamConfig
    state: HealthState = HealthState.HEALTHY
    consecutive_failures: int = 0
    last_state_change: float = 0.0
    probes_sent: int = 0


class ProbeSender(Protocol):
    """健康探测使用的发送器，与正常业务流量隔离。"""

    async def __call__(self, base_url: str, path: str) -> bool: ...



class HealthManager:
    """管理一条路由下全部上游的健康状态。"""

    def __init__(self, failure_threshold: int, probe_interval: float) -> None:
        self._threshold = failure_threshold
        self._interval = probe_interval
        self._items: dict[str, UpstreamHealth] = {}

    @property
    def probe_interval(self) -> float:
        return self._interval

    def register(self, upstream: UpstreamConfig) -> UpstreamHealth:
        health = UpstreamHealth(config=upstream)
        self._items[upstream.id] = health
        return health

    def get(self, upstream_id: str) -> UpstreamHealth:
        return self._items[upstream_id]

    def is_available(self, upstream_id: str) -> bool:
        """业务流量判定：只有 HEALTHY 才可用。"""
        return self._items[upstream_id].state is HealthState.HEALTHY

    def healthy_ids(self) -> list[str]:
        return [
            uid
            for uid, h in self._items.items()
            if h.state is HealthState.HEALTHY
        ]

    def record_success(self, upstream_id: str, now: float) -> HealthState:
        h = self._items[upstream_id]
        h.consecutive_failures = 0
        if h.state is not HealthState.HEALTHY:
            h.state = HealthState.HEALTHY
            h.last_state_change = now
        return h.state

    def record_failure(self, upstream_id: str, now: float) -> HealthState:
        h = self._items[upstream_id]
        h.consecutive_failures += 1
        if (
            h.consecutive_failures >= self._threshold
            and h.state is not HealthState.UNHEALTHY
        ):
            h.state = HealthState.UNHEALTHY
            h.last_state_change = now
        return h.state

    def due_for_probe(self, upstream_id: str, now: float) -> bool:
        """UNHEALTHY 冷却到期则转 SUSPECT 并返回 True（每次冷却期至多一次）。"""
        h = self._items[upstream_id]
        if h.state is HealthState.UNHEALTHY and (
            now - h.last_state_change >= self._interval
        ):
            h.state = HealthState.SUSPECT
            h.last_state_change = now
            return True
        return False

    async def probe_one(
        self,
        upstream_id: str,
        send: ProbeSender,
        now: float,
        path: str = "/",
    ) -> HealthState:
        """对 SUSPECT 实例发起一次探测，按确定规则决定恢复或重新摘除。"""
        h = self._items[upstream_id]
        if h.state is not HealthState.SUSPECT:
            return h.state
        h.probes_sent += 1
        ok = await send(h.config.base_url, path)
        if ok:
            self.record_success(upstream_id, now)
        else:
            # 探测失败：回到 UNHEALTHY 并重新计时
            h.state = HealthState.UNHEALTHY
            h.last_state_change = now
        return h.state

    def snapshot(self) -> dict[str, str]:
        return {uid: h.state.value for uid, h in self._items.items()}
