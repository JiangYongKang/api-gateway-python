"""Internal request/response models used between gateway components."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Request:
    method: str
    path: str
    headers: dict[str, str] = field(default_factory=dict)
    query: dict[str, str] = field(default_factory=dict)
    body: bytes = b""

    @property
    def client_id(self) -> str:
        """Client dimension for rate limiting.

        Preference: explicit ``X-Client-Id`` header, then
        ``X-Forwarded-For`` first hop, then ``anonymous``.
        Header lookup is case-insensitive.
        """
        lowered = {k.lower(): v for k, v in self.headers.items()}
        explicit = lowered.get("x-client-id")
        if explicit:
            return explicit.strip()
        forwarded = lowered.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return "anonymous"


@dataclass
class Response:
    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    # Which upstream produced this response (for logs / retries).
    upstream_id: str | None = None
