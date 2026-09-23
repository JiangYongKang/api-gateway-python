"""真实并发端到端：并发上限严格执行，计数一致，被拒请求不触达上游。"""

from __future__ import annotations

import asyncio

from gateway.config_loader import load_config
from gateway.proxy import GatewayProxy
from gateway.upstream import UpstreamClient, UpstreamResult
from tests.conftest import FakeClock


class GatedSender:
    """所有请求阻塞在闸门上，直到测试放行；记录峰值在途。"""

    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.in_flight = 0
        self.peak = 0
        self.lock = asyncio.Lock()

    async def __call__(self, uid, base_url, method, path, headers, body, timeout):
        async with self.lock:
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)
        try:
            await asyncio.wait_for(self.gate.wait(), timeout=5)
            return UpstreamResult(ok=True, status_code=200, body=b"ok", upstream_id=uid)
        finally:
            async with self.lock:
                self.in_flight -= 1


def _proxy(sender, max_concurrent):
    cfg = load_config(
        {
            "rate_limit": {"requests_per_second": 0, "max_concurrent": max_concurrent},
            "routes": [
                {
                    "id": "svc",
                    "path_prefix": "/svc",
                    "upstreams": [
                        {"id": "u1", "base_url": "http://up1"},
                        {"id": "u2", "base_url": "http://up2"},
                    ],
                }
            ],
        }
    )
    return GatewayProxy(cfg, UpstreamClient(sender), probe_sender=sender, clock=FakeClock())


async def test_concurrency_limit_under_parallel_load():
    sender = GatedSender()
    proxy = _proxy(sender, max_concurrent=3)

    async def one():
        try:
            out = await proxy.handle("GET", "/svc/x", {"X-Client-Id": "hot"}, b"")
            return ("ok", out.upstream_id)
        except Exception as exc:
            return (exc.error_type.value, "")

    task = asyncio.gather(*(one() for _ in range(12)))
    await asyncio.sleep(0.05)  # 等所有请求完成限流判定并卡在闸门

    # 恰好 3 个在上游在途；其余 9 个被并发限制，从未触达上游
    assert sender.in_flight == 3
    assert sender.peak == 3

    sender.gate.set()
    results = await task

    ok = [r for r in results if r[0] == "ok"]
    limited = [r for r in results if r[0] == "concurrency_limited"]
    assert len(ok) == 3 and len(limited) == 9
    # 放行后并发名额全部释放
    snap = await proxy.limiter.snapshot()
    assert snap["hot"]["in_flight"] == 0


async def test_per_client_concurrency_is_isolated():
    sender = GatedSender()
    proxy = _proxy(sender, max_concurrent=1)

    async def call(client):
        try:
            await proxy.handle("GET", "/svc/x", {"X-Client-Id": client}, b"")
            return "ok"
        except Exception as exc:
            return exc.error_type.value

    task = asyncio.gather(call("a"), call("b"), call("a"), call("b"))
    await asyncio.sleep(0.05)
    sender.gate.set()
    results = await task
    assert sorted(results) == sorted(
        ["ok", "ok", "concurrency_limited", "concurrency_limited"]
    )
