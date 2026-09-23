"""请求判定日志：打印请求、判定依据与上游状态，保证行为可解释。"""

from __future__ import annotations

import json
import logging
from typing import Any

_LOGGER_NAME = "gateway"


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "gateway_fields", None)
        if extra:
            payload.update(extra)
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_gateway_logger() -> logging.Logger:
    """返回配置好的网关 logger（幂等，重复调用不会叠加 handler）。"""
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def explain(logger: logging.Logger, request_id: str, stage: str, **fields: Any) -> None:
    """打印一条结构化判定日志。

    stage 示例：route_match / rate_limit / lb_select / upstream_call /
                health_update / circuit_update / retry / error
    """
    logger.info(
        "gateway decision",
        extra={"gateway_fields": {"request_id": request_id, "stage": stage, **fields}},
    )
