"""Error codes and the gateway's single exception type.

Every gateway failure maps to a distinct ``ErrorCode`` so callers (and logs)
can tell apart: no route, all upstreams ejected, circuit open, rate limit,
concurrency limit, timeout, and ordinary upstream errors.
"""
from __future__ import annotations

from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    ROUTE_NOT_FOUND = "ROUTE_NOT_FOUND"
    ALL_UPSTREAMS_UNAVAILABLE = "ALL_UPSTREAMS_UNAVAILABLE"
    UPSTREAM_CIRCUIT_OPEN = "UPSTREAM_CIRCUIT_OPEN"
    RATE_LIMITED = "RATE_LIMITED"
    CONCURRENCY_LIMITED = "CONCURRENCY_LIMITED"
    UPSTREAM_TIMEOUT = "UPSTREAM_TIMEOUT"
    UPSTREAM_ERROR = "UPSTREAM_ERROR"


# Deterministic HTTP status mapping (documented contract, see README).
HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.ROUTE_NOT_FOUND: 404,
    ErrorCode.ALL_UPSTREAMS_UNAVAILABLE: 503,
    ErrorCode.UPSTREAM_CIRCUIT_OPEN: 503,
    ErrorCode.RATE_LIMITED: 429,
    ErrorCode.CONCURRENCY_LIMITED: 503,
    ErrorCode.UPSTREAM_TIMEOUT: 504,
    ErrorCode.UPSTREAM_ERROR: 502,
}


class GatewayError(Exception):
    """The only exception the gateway core raises for handled failures."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        upstream_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.upstream_status = upstream_status

    @property
    def http_status(self) -> int:
        return HTTP_STATUS[self.code]

    def to_body(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "error": {
                "code": self.code.value,
                "message": self.message,
            }
        }
        if self.details:
            body["error"]["details"] = self.details
        if self.upstream_status is not None:
            body["error"]["upstream_status"] = self.upstream_status
        return body
