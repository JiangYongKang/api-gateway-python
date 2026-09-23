"""网关编排：路由 -> 限流 -> 选实例(健康/熔断) -> 转发 -> 重试与状态更新。

关键一致性约定：
- 限流令牌与并发名额在请求入口一次性获取，重试不重复计数；finally 中释放并发。
- 业务流量只发给 HEALTHY 且熔断器允许的实例；探测流量走独立 sender 与路径。
- 每类失败都产生不同的 GatewayErrorType，错误响应可区分。
- 重试只允许幂等方法，且不重复使用已试过的实例（同一请求最多 N+1 次上游调用）。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from .circuit import CircuitBreaker
from .config import GatewayConfig, RouteConfig, validate_config
from .errors import GatewayError, GatewayErrorType
from .health import HealthManager, HealthState, ProbeSender
from .loadbalancer import LoadBalancer
from .logging_utils import explain, get_gateway_logger
from .ratelimit import RateLimiter
from .router import Router
from .upstream import (
    AsyncSender,
    FakeSender,
    UpstreamClient,
    UpstreamFailureKind,
    UpstreamResult,
)

_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})


@dataclass
class ForwardOutcome:
    status_code: int
    body: bytes
    headers: dict[str, str] = field(default_factory=dict)
    upstream_id: str = ""
    attempts: int = 0
    error: GatewayError | None = None


class RouteRuntime:
    """一条路由的运行时状态：健康管理 + 熔断 + 负载均衡。"""

    def __init__(self, route: RouteConfig) -> None:
        self.route = route
        self.health = HealthManager(route.failure_threshold, route.probe_interval)
        self.circuit = CircuitBreaker(
            route.circuit_threshold, route.circuit_open_seconds
        )
        for up in route.upstreams:
            self.health.register(up)
            self.circuit.register(up.id)


class GatewayProxy:
    def __init__(
        self,
        config: GatewayConfig,
        upstream_client: UpstreamClient | None = None,
        probe_sender: AsyncSender | None = None,
        clock: Any = time.monotonic,
    ) -> None:
        validate_config(config)
        self.config = config
        self.router = Router(config)
        self.limiter = RateLimiter(
            config.rate_limit.requests_per_second,
            config.rate_limit.max_concurrent,
        )
        self.lb = LoadBalancer(seed=20260923)
        self.client = upstream_client or UpstreamClient()
        # 探测默认走业务 sender 的独立调用路径；测试可注入独立 sender。
        # 注入的是 AsyncSender（返回 UpstreamResult），此处包装为 bool 判定。
        raw_probe_sender: AsyncSender = probe_sender or FakeSender()

        async def _probe_adapter(base_url: str, path: str) -> bool:
            result = await raw_probe_sender(
                upstream_id=f"probe:{base_url}",
                base_url=base_url,
                method="GET",
                path=path,
                headers={},
                body=b"",
                timeout=self._probe_timeout(),
            )
            return result.ok

        self._probe_sender: ProbeSender = _probe_adapter
        self._clock = clock
        self.runtimes: dict[str, RouteRuntime] = {
            r.id: RouteRuntime(r) for r in config.routes
        }
        self.logger = get_gateway_logger()
        self._probe_task: asyncio.Task | None = None

    # ---------- 健康探测（与业务流量隔离） ----------

    async def probe_sweep(self, now: float | None = None) -> dict[str, dict[str, str]]:
        """执行一轮探测：只处理到期的摘除实例，成功才恢复，失败重新计时。"""
        now = self._clock() if now is None else now
        report: dict[str, dict[str, str]] = {}
        for rt in self.runtimes.values():
            route_report: dict[str, str] = {}
            for up in rt.route.upstreams:
                uid = up.id
                if rt.health.due_for_probe(uid, now):
                    explain(
                        self.logger,
                        request_id="-",
                        stage="probe_start",
                        route=rt.route.id,
                        upstream=uid,
                        path=rt.route.probe_path,
                    )
                    state = await rt.health.probe_one(
                        uid, self._send_probe, now, rt.route.probe_path
                    )
                    if state is HealthState.HEALTHY:
                        # 恢复接入同时重置熔断计数
                        rt.circuit.record_success(uid, now)
                    route_report[uid] = state.value
            if route_report:
                report[rt.route.id] = route_report
                explain(
                    self.logger,
                    request_id="-",
                    stage="probe_result",
                    route=rt.route.id,
                    result=route_report,
                    health=rt.health.snapshot(),
                )
        return report

    def _probe_timeout(self) -> float:
        return min((r.timeout_seconds for r in self.config.routes), default=1.0)

    async def _send_probe(self, base_url: str, path: str) -> bool:
        return await self._probe_sender(base_url, path)

    async def _probe_loop(self) -> None:
        """周期性探测循环，只与摘除实例通信，不触碰业务流量。"""
        try:
            while True:
                intervals = [r.probe_interval for r in self.config.routes]
                await asyncio.sleep(min(intervals) if intervals else 1.0)
                await self.probe_sweep()
        except asyncio.CancelledError:
            raise

    def start_background_probe(self) -> None:
        if self._probe_task is None:
            self._probe_task = asyncio.create_task(self._probe_loop())

    async def stop_background_probe(self) -> None:
        if self._probe_task is not None:
            self._probe_task.cancel()
            try:
                await self._probe_task
            except asyncio.CancelledError:
                pass
            self._probe_task = None

    # ---------- 主处理流程 ----------

    def _client_id(self, headers: dict[str, str]) -> str:
        return (
            headers.get(self.config.client_id_header)
            or headers.get(self.config.client_id_header.lower())
            or self.config.default_client_id
        )

    def _select_upstream(
        self, rt: RouteRuntime, excluded: set[str], now: float
    ) -> tuple[str | None, str, bool]:
        """返回 (上游id, 依据, 熔断是否全部打开)。"""
        healthy = [
            uid for uid in rt.health.healthy_ids() if uid not in excluded
        ]
        if not healthy:
            return None, "no healthy upstream (excluded=%s)" % sorted(excluded), False
        chosen, reason = self.lb.select(rt.route, healthy, now)
        if chosen is not None and rt.circuit.allow_request(chosen, now):
            return chosen, reason, False
        # 首选被熔断：在其余健康实例中找熔断器允许者（保持确定性）
        for uid in sorted(u for u in healthy if u != chosen):
            if rt.circuit.allow_request(uid, now):
                return (
                    uid,
                    f"{reason}; first choice circuit-blocked, fallback={uid}",
                    False,
                )
        return None, f"all circuits open among healthy={healthy}", True

    async def handle(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        now: float | None = None,
        request_id: str = "-",
    ) -> ForwardOutcome:
        now = self._clock() if now is None else now
        # 1) 路由
        route, route_reason = self.router.match_or_raise(method, path)
        rt = self.runtimes[route.id]
        explain(
            self.logger, request_id, "route_match",
            method=method, path=path, route=route.id, reason=route_reason,
            health=rt.health.snapshot(), circuit=rt.circuit.snapshot(),
        )

        # 2) 限流与并发（入口一次性获取，重试不重复）
        client_id = self._client_id(headers)
        allowed, deny_reason, counters = await self.limiter.try_acquire(
            client_id, now
        )
        explain(
            self.logger, request_id, "rate_limit",
            client_id=client_id, decision=deny_reason,
            tokens=round(counters.tokens, 3), in_flight=counters.in_flight,
        )
        if not allowed:
            if deny_reason == "rate_limited":
                raise GatewayError(
                    GatewayErrorType.RATE_LIMITED,
                    f"客户端 {client_id} 请求频率超限",
                    details={"client_id": client_id, "limit_rps": self.limiter.rps},
                )
            raise GatewayError(
                GatewayErrorType.CONCURRENCY_LIMITED,
                f"客户端 {client_id} 并发请求超限",
                details={
                    "client_id": client_id,
                    "max_concurrent": self.limiter.max_concurrent,
                },
            )

        try:
            return await self._forward(
                rt, method, path, headers, body, now, request_id
            )
        finally:
            await self.limiter.release(client_id)

    async def _forward(
        self,
        rt: RouteRuntime,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        now: float,
        request_id: str,
    ) -> ForwardOutcome:
        route = rt.route
        attempts_left = route.max_retries + 1
        can_retry = method.upper() in _IDEMPOTENT_METHODS
        tried: set[str] = set()
        last_error: GatewayError | None = None

        while attempts_left > 0:
            uid, reason, all_open = self._select_upstream(rt, tried, now)
            if uid is None:
                etype = (
                    GatewayErrorType.CIRCUIT_OPEN
                    if all_open
                    else GatewayErrorType.NO_AVAILABLE_UPSTREAM
                )
                raise GatewayError(
                    etype,
                    (
                        f"路由 {route.id} 上游熔断中"
                        if all_open
                        else f"路由 {route.id} 无可用上游"
                    ),
                    details={
                        "route": route.id,
                        "health": rt.health.snapshot(),
                        "circuit": rt.circuit.snapshot(),
                        "tried": sorted(tried),
                        "reason": reason,
                    },
                )
            tried.add(uid)
            attempts_left -= 1
            up_conf = next(u for u in route.upstreams if u.id == uid)
            self.lb.note_start(route.id, uid)
            explain(
                self.logger, request_id, "lb_select",
                route=route.id, upstream=uid, reason=reason,
                attempt=route.max_retries + 1 - attempts_left,
            )
            result = await self.client.send(
                upstream_id=uid,
                base_url=up_conf.base_url,
                method=method,
                path=path,
                headers=headers,
                body=body,
                timeout=route.timeout_seconds,
            )
            self.lb.note_completion(route.id, uid)
            explain(
                self.logger, request_id, "upstream_call",
                route=route.id, upstream=uid, ok=result.ok,
                status_code=result.status_code, failure=result.failure,
            )

            if result.ok:
                rt.health.record_success(uid, now)
                rt.circuit.record_success(uid, now)
                explain(
                    self.logger, request_id, "health_update",
                    upstream=uid, event="success",
                    health=rt.health.snapshot(), circuit=rt.circuit.snapshot(),
                )
                # 4xx/2xx/3xx 原样透传
                return ForwardOutcome(
                    status_code=result.status_code or 200,
                    body=result.body,
                    headers=result.headers,
                    upstream_id=uid,
                    attempts=len(tried),
                )

            # 失败：更新健康与熔断
            new_health = rt.health.record_failure(uid, now)
            new_circuit = rt.circuit.record_failure(uid, now)
            explain(
                self.logger, request_id, "health_update",
                upstream=uid, event="failure", failure=result.failure,
                health_state=new_health.value, circuit_state=new_circuit.value,
                health=rt.health.snapshot(), circuit=rt.circuit.snapshot(),
            )
            last_error = self._failure_error(route, uid, result, tried)

            retriable = (
                can_retry
                and attempts_left > 0
                and self._is_retriable(route, result)
            )
            explain(
                self.logger, request_id, "retry",
                upstream=uid, retriable=retriable,
                attempts_left=attempts_left, idempotent=can_retry,
            )
            if not retriable:
                break

        assert last_error is not None
        last_error.details["attempts"] = len(tried)
        raise last_error

    @staticmethod
    def _is_retriable(route: RouteConfig, result: UpstreamResult) -> bool:
        if result.failure in (
            UpstreamFailureKind.TIMEOUT,
            UpstreamFailureKind.CONNECT,
        ):
            return True
        if (
            result.failure is UpstreamFailureKind.BAD_STATUS
            and result.status_code in route.retry_on_status
        ):
            return True
        return False

    def _failure_error(
        self,
        route: RouteConfig,
        uid: str,
        result: UpstreamResult,
        tried: set[str],
    ) -> GatewayError:
        common = {"route": route.id, "upstream": uid, "tried": sorted(tried)}
        if result.failure is UpstreamFailureKind.TIMEOUT:
            return GatewayError(
                GatewayErrorType.UPSTREAM_TIMEOUT,
                f"上游 {uid} 请求超时",
                details={**common, "timeout_seconds": route.timeout_seconds},
            )
        if result.failure is UpstreamFailureKind.CONNECT:
            return GatewayError(
                GatewayErrorType.UPSTREAM_ERROR,
                f"上游 {uid} 连接失败",
                details=common,
            )
        return GatewayError(
            GatewayErrorType.UPSTREAM_BAD_STATUS,
            f"上游 {uid} 返回错误状态码 {result.status_code}",
            details={**common, "upstream_status": result.status_code},
        )

    def explain_snapshot(self) -> dict[str, Any]:
        return {
            route_id: {
                "health": rt.health.snapshot(),
                "circuit": rt.circuit.snapshot(),
                "strategy": rt.route.lb_strategy.value,
            }
            for route_id, rt in self.runtimes.items()
        }
