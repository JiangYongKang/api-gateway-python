"""熔断器：上游超时或连续失败时打开，冷却后半开试探。

    CLOSED --连续失败>=threshold--> OPEN
    OPEN --冷却 open_seconds--> HALF_OPEN（只放一个试探请求）
    HALF_OPEN --试探成功--> CLOSED
    HALF_OPEN --试探失败--> OPEN（重新计时）

与健康管理的分工：熔断是更快速的实例级保护（阈值可不同），
健康摘除是持续故障下的隔离；两者任一拒绝都不会把请求发往该实例。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitStatus:
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    opened_at: float = 0.0
    trial_in_flight: bool = False


class CircuitBreaker:
    def __init__(self, threshold: int, open_seconds: float) -> None:
        self._threshold = threshold
        self._open_seconds = open_seconds
        self._items: dict[str, CircuitStatus] = {}

    def register(self, upstream_id: str) -> CircuitStatus:
        st = CircuitStatus()
        self._items[upstream_id] = st
        return st

    def _get(self, upstream_id: str) -> CircuitStatus:
        st = self._items.get(upstream_id)
        if st is None:
            st = CircuitStatus()
            self._items[upstream_id] = st
        return st

    def allow_request(self, upstream_id: str, now: float) -> bool:
        """是否允许把业务请求发往该上游；OPEN 冷却到期时放一个试探请求。"""
        st = self._get(upstream_id)
        if st.state is CircuitState.CLOSED:
            return True
        if st.state is CircuitState.OPEN:
            if now - st.opened_at >= self._open_seconds:
                st.state = CircuitState.HALF_OPEN
                st.trial_in_flight = True
                return True
            return False
        # HALF_OPEN：试探请求未结束前拒绝其余请求，避免探测流量放大
        return not st.trial_in_flight

    def record_success(self, upstream_id: str, now: float) -> CircuitState:
        st = self._get(upstream_id)
        st.failure_count = 0
        st.trial_in_flight = False
        if st.state is not CircuitState.CLOSED:
            st.state = CircuitState.CLOSED
        return st.state

    def record_failure(self, upstream_id: str, now: float) -> CircuitState:
        st = self._get(upstream_id)
        st.failure_count += 1
        st.trial_in_flight = False
        if st.failure_count >= self._threshold or st.state is CircuitState.HALF_OPEN:
            st.state = CircuitState.OPEN
            st.opened_at = now
        return st.state

    def snapshot(self) -> dict[str, str]:
        return {uid: st.state.value for uid, st in self._items.items()}
