"""
logging_utils.py — 容器结构化日志工具

提供 configure_container_logging / log_event 两个接口。
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    """输出 JSON 格式日志行。"""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        payload: dict = {
            "ts": ts,
            "level": record.levelname,
            "service": getattr(record, "service", "dataflow_analyse"),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("stage", "event"):
            val = getattr(record, key, None)
            if val is not None:
                payload[key] = val
        for key in (
            "task_id",
            "project_id",
            "owner_id",
            "epoch",
            "control_version",
            "dispatch_status",
            "status",
            "claimed_count",
            "current_running",
        ):
            val = getattr(record, key, None)
            if val is not None:
                payload[key] = val
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_container_logging(service: str = "dataflow_analyse") -> None:
    """配置全局 JSON 日志（输出到 stderr）。"""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_JsonFormatter())

    old_factory = logging.getLogRecordFactory()

    def _factory(*args, **kwargs):  # type: ignore[no-untyped-def]
        record = old_factory(*args, **kwargs)
        record.service = service  # type: ignore[attr-defined]
        return record

    logging.setLogRecordFactory(_factory)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG if os.environ.get("LOG_LEVEL", "").upper() == "DEBUG" else logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)
    return root


def log_event(
    logger: logging.Logger,
    level: int,
    message: str,
    **fields: object,
) -> None:
    """记录带有额外字段的结构化日志。"""
    logger.log(level, message, extra=fields, stacklevel=2)
