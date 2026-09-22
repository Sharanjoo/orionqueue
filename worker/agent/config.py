"""Runtime configuration for the OrionQueue worker agent.

Loaded from ORIONQUEUE_-prefixed environment variables, layered over
defaults that are always valid, mirroring internal/config on the Go
control-plane side so both halves of the system follow the same
configuration convention.
"""

from __future__ import annotations

import dataclasses
import os

VALID_LOG_LEVELS = {"debug", "info", "warning", "error"}

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


@dataclasses.dataclass(frozen=True)
class Config:
    worker_id: str
    environment: str
    log_level: str
    fake_gpu: bool


def defaults() -> Config:
    """Baseline configuration before environment overrides are applied.

    fake_gpu defaults to True because most environments this agent runs
    in (this dev machine, CI) have no NVIDIA GPU; real hardware detection
    overrides this automatically starting Phase 4/9.
    """
    return Config(
        worker_id=f"worker-local-{os.getpid()}",
        environment="local",
        log_level="info",
        fake_gpu=True,
    )


def load() -> Config:
    """Build a Config from environment variables layered over defaults.

    Raises ValueError if a supplied value is malformed, so a misconfigured
    agent fails fast at startup instead of behaving unpredictably later.
    """
    base = defaults()

    worker_id = os.environ.get("ORIONQUEUE_WORKER_ID", base.worker_id)
    environment = os.environ.get("ORIONQUEUE_ENVIRONMENT", base.environment)
    log_level = os.environ.get("ORIONQUEUE_LOG_LEVEL", base.log_level)

    fake_gpu_raw = os.environ.get("ORIONQUEUE_FAKE_GPU")
    fake_gpu = (
        base.fake_gpu if fake_gpu_raw is None else _parse_bool(fake_gpu_raw, "ORIONQUEUE_FAKE_GPU")
    )

    if not worker_id:
        raise ValueError("ORIONQUEUE_WORKER_ID must not be empty")
    if log_level not in VALID_LOG_LEVELS:
        raise ValueError(
            f"invalid ORIONQUEUE_LOG_LEVEL {log_level!r}: must be one of {sorted(VALID_LOG_LEVELS)}"
        )

    return Config(
        worker_id=worker_id,
        environment=environment,
        log_level=log_level,
        fake_gpu=fake_gpu,
    )


def _parse_bool(raw: str, var_name: str) -> bool:
    lowered = raw.strip().lower()
    if lowered in _TRUE_VALUES:
        return True
    if lowered in _FALSE_VALUES:
        return False
    raise ValueError(f"invalid boolean for {var_name}: {raw!r}")
