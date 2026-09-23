"""负载均衡：从一条路由的健康上游中按可配置策略挑选实例。

支持策略：
- round_robin：在“当前健康实例集合”上轮询（游标在全局位置上单调递增，
  摘除/恢复后选择依然确定、可解释，不会因集合变化重复命中或跳过异常）；
- weighted_round_robin：平滑加权轮询（nginx 风格），权重越大概率越高，
  且分布均匀、确定；
- random：带种子的伪随机（测试可复现）；
- least_connections：选在途连接最少者，并列时按 id 字典序，结果确定。
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from .config import LBStrategy, RouteConfig


@dataclass
class _RRState:
    cursor: int = 0


@dataclass
class _WrrState:
    current: dict[str, int] = field(default_factory=dict)


class LoadBalancer:
    """按路由配置的策略选择上游；策略行为确定且可解释。"""

    def __init__(self, seed: int | None = None) -> None:
        self._rr: dict[str, _RRState] = {}
        self._wrr: dict[str, _WrrState] = {}
        self._in_flight: dict[tuple[str, str], int] = {}
        self._rng = random.Random(seed)

    def select(
        self,
        route: RouteConfig,
        healthy_ids: list[str],
        now: float,
    ) -> tuple[str | None, str]:
        """返回 (选中的上游 id, 判定依据说明)；无健康实例时 id 为 None。"""
        if not healthy_ids:
            return None, "no healthy upstream"
        ordered = sorted(healthy_ids)
        if route.lb_strategy is LBStrategy.ROUND_ROBIN:
            return self._round_robin(route, ordered)
        if route.lb_strategy is LBStrategy.WEIGHTED_ROUND_ROBIN:
            return self._weighted_round_robin(route, ordered)
        if route.lb_strategy is LBStrategy.RANDOM:
            return self._random(route, ordered)
        return self._least_connections(route, ordered)

    def note_start(self, route_id: str, upstream_id: str) -> None:
        key = (route_id, upstream_id)
        self._in_flight[key] = self._in_flight.get(key, 0) + 1

    def note_completion(self, route_id: str, upstream_id: str) -> None:
        key = (route_id, upstream_id)
        if self._in_flight.get(key, 0) > 0:
            self._in_flight[key] -= 1

    def _weights(self, route: RouteConfig, ids: list[str]) -> dict[str, int]:
        return {u.id: u.weight for u in route.upstreams if u.id in ids}

    def _round_robin(self, route: RouteConfig, ids: list[str]) -> tuple[str, str]:
        st = self._rr.setdefault(route.id, _RRState())
        idx = st.cursor % len(ids)
        st.cursor += 1
        chosen = ids[idx]
        return chosen, (
            f"round_robin: cursor={st.cursor - 1} -> index={idx} "
            f"of healthy={ids}"
        )

    def _weighted_round_robin(
        self, route: RouteConfig, ids: list[str]
    ) -> tuple[str, str]:
        st = self._wrr.setdefault(route.id, _WrrState())
        weights = self._weights(route, ids)
        total = sum(weights.values())
        for uid in ids:
            st.current[uid] = st.current.get(uid, 0) + weights[uid]
        chosen = max(ids, key=lambda uid: (st.current[uid], uid))
        st.current[chosen] -= total
        detail = ", ".join(f"{uid}={weights[uid]}" for uid in ids)
        return chosen, (
            f"weighted_round_robin: weights({detail}) "
            f"effective={ {u: st.current[u] for u in ids} } -> {chosen}"
        )

    def _random(self, route: RouteConfig, ids: list[str]) -> tuple[str, str]:
        idx = self._rng.randrange(len(ids))
        return ids[idx], f"random: index={idx} of healthy={ids}"

    def _least_connections(
        self, route: RouteConfig, ids: list[str]
    ) -> tuple[str, str]:
        loads = {
            uid: self._in_flight.get((route.id, uid), 0) for uid in ids
        }
        # 最少在途；并列时 id 字典序，保证确定
        chosen = min(ids, key=lambda uid: (loads[uid], uid))
        return chosen, f"least_connections: in_flight={loads} -> {chosen}"
