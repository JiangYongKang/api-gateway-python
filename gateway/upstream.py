"""上游 HTTP 调用抽象与结果类型。

- 业务流量与健康探测共用同一发送协议，但由调用方分别发起；
  健康探测只访问探测路径、不占用业务并发名额、不扣限流令牌，互不干扰。
- 故障被分为 connect / timeout / bad_status 三类，供健康与熔断决策。
- 真实发送器基于 httpx；测试可注入内存假发送器，无需真实网络。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Awaitable, Callable, Protocol

import httpx


class UpstreamFailureKind(str, Enum):
    CONNECT = "connect"
    TIMEOUT = "timeout"
    BAD_STATUS = "bad_status"


@dataclass
class UpstreamResult:
    ok: bool
    status_code: int = 0
    body: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)
    failure: UpstreamFailureKind | None = None
    upstream_id: str = ""

    @property
    def is_timeout(self) -> bool:
        return self.failure is UpstreamFailureKind.TIMEOUT


# 该状态码及以上视为上游故障（4xx 视为客户端问题，透传不计故障）
BAD_STATUS_MIN = 500


class AsyncSender(Protocol):
    async def __call__(
        self,
        upstream_id: str,
        base_url: str,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        timeout: float,
    ) -> UpstreamResult: ...


class HttpxSender:
    """真实 httpx 发送器：每个请求独立超时，连接错误与超时分开归类。"""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __call__(
        self,
        upstream_id: str,
        base_url: str,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        timeout: float,
    ) -> UpstreamResult:
        url = base_url.rstrip("/") + path
        try:
            resp = await self._client.request(
                method,
                url,
                headers=headers,
                content=body,
                timeout=timeout,
            )
        except httpx.TimeoutException:
            return UpstreamResult(
                ok=False,
                failure=UpstreamFailureKind.TIMEOUT,
                upstream_id=upstream_id,
            )
        except httpx.TransportError:
            return UpstreamResult(
                ok=False,
                failure=UpstreamFailureKind.CONNECT,
                upstream_id=upstream_id,
            )
        ok = resp.status_code < BAD_STATUS_MIN
        return UpstreamResult(
            ok=ok,
            status_code=resp.status_code,
            body=resp.content,
            headers=dict(resp.headers),
            failure=None if ok else UpstreamFailureKind.BAD_STATUS,
            upstream_id=upstream_id,
        )


FakeHandler = Callable[[str, str, str, str, dict[str, str], bytes], Awaitable[UpstreamResult]]


class FakeSender:
    """内存假发送器：handler 自行决定每个上游的响应，供单测使用。"""

    def __init__(self, handler: FakeHandler | None = None) -> None:
        self.calls: list[dict] = []
        self._handler = handler

    async def __call__(
        self,
        upstream_id: str,
        base_url: str,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        timeout: float,
    ) -> UpstreamResult:
        self.calls.append(
            {
                "upstream_id": upstream_id,
                "method": method,
                "path": path,
                "body": body,
                "timeout": timeout,
            }
        )
        if self._handler is not None:
            return await self._handler(
                upstream_id, base_url, method, path, headers, body
            )
        return UpstreamResult(ok=True, status_code=200, upstream_id=upstream_id)


class UpstreamClient:
    """薄封装：统一在结果上标记 upstream_id。"""

    def __init__(self, sender: AsyncSender | None = None) -> None:
        self._sender: AsyncSender = sender or HttpxSender()

    async def send(
        self,
        upstream_id: str,
        base_url: str,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        timeout: float,
    ) -> UpstreamResult:
        result = await self._sender(
            upstream_id, base_url, method, path, headers, body, timeout
        )
        result.upstream_id = upstream_id
        return result
