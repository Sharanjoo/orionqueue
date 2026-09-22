"""Runtime configuration for the OrionQueue worker agent.

Loaded from ORIONQUEUE_-prefixed environment variables, layered over
defaults that are always valid, mirroring internal/config on the Go
control-plane side so both halves of the system follow the same
configuration convention.
"""

from __future__ import annotations

import dataclasses
import os
import socket

VALID_LOG_LEVELS = {"debug", "info", "warning", "error"}

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}

DEFAULT_API_GRPC_ADDR = "localhost:9080"
DEFAULT_CPU_CAPACITY = 8.0
DEFAULT_MEMORY_CAPACITY_BYTES = 64 * (1024**3)  # 64 GiB


@dataclasses.dataclass(frozen=True)
class Config:
    worker_id: str
    environment: str
    log_level: str
    fake_gpu: bool

    # Phase 4 additions: registration/heartbeat against the control plane.
    api_grpc_addr: str
    hostname: str
    cpu_capacity: float
    memory_capacity_bytes: int
    fake_gpu_count: int
    fake_gpu_memory_bytes: int


def defaults() -> Config:
    """Baseline configuration before environment overrides are applied.

    fake_gpu defaults to True because most environments this agent runs
    in (this dev machine, CI) have no NVIDIA GPU; real hardware detection
    overrides this automatically starting Phase 9.
    """
    # Imported lazily to avoid a hard dependency at module import time for
    # callers that only need config, not GPU discovery.
    from gpu.fake import DEFAULT_GPU_COUNT, DEFAULT_GPU_MEMORY_BYTES

    return Config(
        worker_id=f"worker-local-{os.getpid()}",
        environment="local",
        log_level="info",
        fake_gpu=True,
        api_grpc_addr=DEFAULT_API_GRPC_ADDR,
        hostname=socket.gethostname(),
        cpu_capacity=DEFAULT_CPU_CAPACITY,
        memory_capacity_bytes=DEFAULT_MEMORY_CAPACITY_BYTES,
        fake_gpu_count=DEFAULT_GPU_COUNT,
        fake_gpu_memory_bytes=DEFAULT_GPU_MEMORY_BYTES,
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
    api_grpc_addr = os.environ.get("ORIONQUEUE_API_GRPC_ADDR", base.api_grpc_addr)
    hostname = os.environ.get("ORIONQUEUE_HOSTNAME", base.hostname)

    fake_gpu_raw = os.environ.get("ORIONQUEUE_FAKE_GPU")
    fake_gpu = (
        base.fake_gpu if fake_gpu_raw is None else _parse_bool(fake_gpu_raw, "ORIONQUEUE_FAKE_GPU")
    )

    cpu_capacity = _parse_float(
        os.environ.get("ORIONQUEUE_CPU_CAPACITY"), base.cpu_capacity, "ORIONQUEUE_CPU_CAPACITY"
    )
    memory_capacity_bytes = _parse_int(
        os.environ.get("ORIONQUEUE_MEMORY_CAPACITY_BYTES"),
        base.memory_capacity_bytes,
        "ORIONQUEUE_MEMORY_CAPACITY_BYTES",
    )
    fake_gpu_count = _parse_int(
        os.environ.get("ORIONQUEUE_FAKE_GPU_COUNT"),
        base.fake_gpu_count,
        "ORIONQUEUE_FAKE_GPU_COUNT",
    )
    fake_gpu_memory_bytes = _parse_int(
        os.environ.get("ORIONQUEUE_FAKE_GPU_MEMORY_BYTES"),
        base.fake_gpu_memory_bytes,
        "ORIONQUEUE_FAKE_GPU_MEMORY_BYTES",
    )

    if not worker_id:
        raise ValueError("ORIONQUEUE_WORKER_ID must not be empty")
    if log_level not in VALID_LOG_LEVELS:
        raise ValueError(
            f"invalid ORIONQUEUE_LOG_LEVEL {log_level!r}: must be one of {sorted(VALID_LOG_LEVELS)}"
        )
    if not api_grpc_addr:
        raise ValueError("ORIONQUEUE_API_GRPC_ADDR must not be empty")
    if not hostname:
        raise ValueError("ORIONQUEUE_HOSTNAME must not be empty")
    if cpu_capacity < 0:
        raise ValueError("ORIONQUEUE_CPU_CAPACITY must not be negative")
    if memory_capacity_bytes < 0:
        raise ValueError("ORIONQUEUE_MEMORY_CAPACITY_BYTES must not be negative")
    if fake_gpu_count < 0:
        raise ValueError("ORIONQUEUE_FAKE_GPU_COUNT must not be negative")
    if fake_gpu_memory_bytes < 0:
        raise ValueError("ORIONQUEUE_FAKE_GPU_MEMORY_BYTES must not be negative")

    return Config(
        worker_id=worker_id,
        environment=environment,
        log_level=log_level,
        fake_gpu=fake_gpu,
        api_grpc_addr=api_grpc_addr,
        hostname=hostname,
        cpu_capacity=cpu_capacity,
        memory_capacity_bytes=memory_capacity_bytes,
        fake_gpu_count=fake_gpu_count,
        fake_gpu_memory_bytes=fake_gpu_memory_bytes,
    )


def _parse_bool(raw: str, var_name: str) -> bool:
    lowered = raw.strip().lower()
    if lowered in _TRUE_VALUES:
        return True
    if lowered in _FALSE_VALUES:
        return False
    raise ValueError(f"invalid boolean for {var_name}: {raw!r}")


def _parse_float(raw: str | None, default: float, var_name: str) -> float:
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"invalid float for {var_name}: {raw!r}") from exc


def _parse_int(raw: str | None, default: int, var_name: str) -> int:
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"invalid integer for {var_name}: {raw!r}") from exc
