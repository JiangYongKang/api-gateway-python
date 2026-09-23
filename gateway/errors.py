"""网关统一错误定义。

所有失败原因都映射为不同的 ``GatewayErrorType``，保证：
- 无匹配路由 vs 上游全不可用 可区分；
- 限流 / 并发超限 / 熔断 / 普通上游错误 可区分。
"""

from __future__ import annotations

from enum import Enum


class GatewayErrorType(str, Enum):
    """网关可返回的全部失败原因。"""

    ROUTE_NOT_FOUND = "route_not_found"
    NO_AVAILABLE_UPSTREAM = "no_available_upstream"
    RATE_LIMITED = "rate_limited"
    CONCURRENCY_LIMITED = "concurrency_limited"
    CIRCUIT_OPEN = "circuit_open"
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_ERROR = "upstream_error"
    UPSTREAM_BAD_STATUS = "upstream_bad_status"
    RETRY_EXHAUSTED = "retry_exhausted"
    INTERNAL_ERROR = "internal_error"


# 每种错误对应的 HTTP 状态码
ERROR_STATUS: dict[GatewayErrorType, int] = {
    GatewayErrorType.ROUTE_NOT_FOUND: 404,
    GatewayErrorType.NO_AVAILABLE_UPSTREAM: 503,
    GatewayErrorType.RATE_LIMITED: 429,
    GatewayErrorType.CONCURRENCY_LIMITED: 429,
    GatewayErrorType.CIRCUIT_OPEN: 503,
    GatewayErrorType.UPSTREAM_TIMEOUT: 504,
    GatewayErrorType.UPSTREAM_ERROR: 502,
    GatewayErrorType.UPSTREAM_BAD_STATUS: 502,
    GatewayErrorType.RETRY_EXHAUSTED: 502,
    GatewayErrorType.INTERNAL_ERROR: 500,
}


class GatewayError(Exception):
    """网关内部抛出的统一异常，携带可解释的错误类型与详情。"""

    def __init__(
        self,
        error_type: GatewayErrorType,
        message: str,
        *,
        details: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.message = message
        self.details = details or {}

    @property
    def status_code(self) -> int:
        return ERROR_STATUS[self.error_type]

    def to_body(self) -> dict:
        return {
            "error": {
                "code": self.error_type.value,
                "message": self.message,
                "details": self.details,
            }
        }
