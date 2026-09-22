"""Structured JSON logging for the worker agent.

Mirrors internal/logging on the Go control plane so log lines from either
side of the system share the same shape (service, environment, level,
msg), making them easy to correlate when read together (e.g. in a shared
log aggregator from Phase 10 onward).
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any

_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class _JSONFormatter(logging.Formatter):
    def __init__(self, service: str, environment: str) -> None:
        super().__init__()
        self._service = service
        self._environment = environment

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "time": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(record.created)),
            "level": record.levelname.lower(),
            "msg": record.getMessage(),
            "service": self._service,
            "environment": self._environment,
        }
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)

        extra_fields = getattr(record, "fields", None)
        if isinstance(extra_fields, dict):
            payload.update(extra_fields)

        return json.dumps(payload)


def configure(service: str, environment: str, level: str) -> logging.Logger:
    """Configure and return the "orionqueue" logger to emit one JSON
    object per line on stdout.

    Clears any handlers from a previous call first, so calling this
    repeatedly (as tests do, once per test) never accumulates duplicate
    handlers and duplicate output.
    """
    logger = logging.getLogger("orionqueue")
    logger.handlers.clear()

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(_JSONFormatter(service, environment))
    logger.addHandler(handler)
    logger.setLevel(_LEVELS.get(level, logging.INFO))
    logger.propagate = False
    return logger
