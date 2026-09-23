"""Decision logging: request, decision basis and upstream state are logged.

Every test run prints, per request:
  * the request (method/path/client),
  * the decision basis (matched rule, chosen policy/candidate, limits),
  * upstream state (health snapshot, attempts, result).
Logs are single-line ``key=value`` pairs so they stay readable under
concurrent test execution.
"""
from __future__ import annotations

import logging
import sys
from typing import Any

_CONFIGURED = False


class _KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = f"{self.formatTime(record)} [{record.levelname}] {record.name} #{getattr(record, 'event', '-')}"
        fields = getattr(record, "fields", None)
        if fields:
            rendered = " ".join(f"{k}={_render(v)}" for k, v in fields.items())
            base = f"{base} {rendered}"
        if record.exc_info:
            base = f"{base}\n{self.formatException(record.exc_info)}"
        return base


def _render(value: Any) -> str:
    text = str(value)
    if any(ch in text for ch in (" ", "\n")):
        return '"' + text.replace('"', "'") + '"'
    return text


def configure_logging(level: int = logging.INFO) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_KeyValueFormatter())
    root = logging.getLogger("gateway")
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
    root.propagate = False
    _CONFIGURED = True


def decision_logger(name: str = "gateway") -> logging.Logger:
    configure_logging()
    return logging.getLogger(name if name.startswith("gateway") else f"gateway.{name}")


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    """Emit one structured decision-log line."""
    logger.info("", extra={"event": event, "fields": fields})
