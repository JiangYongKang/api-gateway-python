"""按客户端维度的令牌桶限流与并发计数控制。

- requests_per_second：令牌桶容量=每秒配额，按经过时间补充令牌；
  一个请求只取一个令牌，重试不会重复计数（令牌在入口处一次性获取）。
- max_concurrent：同一客户端同时在途请求数上限，请求结束必须 release。
- 两种拒绝原因可区分：rate_limited / concurrency_limited。

FastAPI 运行在单事件循环内，用 asyncio.Lock 保证“检查+扣减”原子，
并发请求下计数严格一致。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass
class ClientCounters:
    tokens: float
    last_refill: float
    in_flight: int


class RateLimiter:
    def __init__(self, requests_per_second: float, max_concurrent: int) -> None:
        self._rps = requests_per_second
        self._max_concurrent = max_concurrent
        self._counters: dict[str, ClientCounters] = {}
        self._lock = asyncio.Lock()

    @property
    def rps(self) -> float:
        return self._rps

    @property
    def max_concurrent(self) -> int:
        return self._max_concurrent

    async def try_acquire(
        self, client_id: str, now: float
    ) -> tuple[bool, str, ClientCounters]:
        """尝试通过限流与并发检查，返回(是否放行, 拒绝原因, 计数器快照)。"""
        async with self._lock:
            c = self._counters.get(client_id)
            if c is None:
                c = ClientCounters(
                    tokens=self._rps, last_refill=now, in_flight=0
                )
                self._counters[client_id] = c
            # 令牌补充
            if self._rps > 0:
                elapsed = max(0.0, now - c.last_refill)
                c.tokens = min(self._rps, c.tokens + elapsed * self._rps)
                c.last_refill = now
            # 并发优先判定，给出可区分原因
            if self._max_concurrent > 0 and c.in_flight >= self._max_concurrent:
                return False, "concurrency_limited", ClientCounters(
                    c.tokens, c.last_refill, c.in_flight
                )
            # 每个请求入口只取一次令牌（重试不重复扣）
            if self._rps > 0 and c.tokens < 1.0:
                return False, "rate_limited", ClientCounters(
                    c.tokens, c.last_refill, c.in_flight
                )
            if self._rps > 0:
                c.tokens -= 1.0
            c.in_flight += 1
            return True, "allowed", ClientCounters(c.tokens, c.last_refill, c.in_flight)

    async def release(self, client_id: str) -> None:
        async with self._lock:
            c = self._counters.get(client_id)
            if c is not None and c.in_flight > 0:
                c.in_flight -= 1

    async def snapshot(self) -> dict[str, dict]:
        async with self._lock:
            return {
                cid: {
                    "tokens": round(c.tokens, 3),
                    "in_flight": c.in_flight,
                }
                for cid, c in self._counters.items()
            }
