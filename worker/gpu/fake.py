"""Deterministic fake GPU inventory generator.

Every value this module produces is simulated — see
docs/adr/0003-fake-vs-real-gpu.md. Nothing here talks to real hardware or
NVML; that's Phase 9. Generation is deterministic for a given worker ID so
re-running the same worker always reports the same UUIDs, which is what
makes demos and tests reproducible.
"""

from __future__ import annotations

import dataclasses
import hashlib

DEFAULT_GPU_COUNT = 2
DEFAULT_GPU_MEMORY_BYTES = 16 * (1024**3)  # 16 GiB, simulated


@dataclasses.dataclass(frozen=True)
class GPU:
    device_index: int
    uuid: str
    total_memory_bytes: int
    allocated_memory_bytes: int = 0
    utilization_percent: float = 0.0
    temperature_celsius: float | None = None
    mig_profile: str = ""
    health_state: str = "HEALTHY"


def discover(
    worker_id: str,
    count: int = DEFAULT_GPU_COUNT,
    memory_bytes: int = DEFAULT_GPU_MEMORY_BYTES,
) -> list[GPU]:
    """Returns `count` simulated GPUs for worker_id.

    Deterministic: the same (worker_id, count, memory_bytes) always
    produces the same UUIDs, computed from a hash of the worker ID and
    device index rather than random generation.
    """
    if count < 0:
        raise ValueError(f"count must not be negative, got {count}")
    if memory_bytes < 0:
        raise ValueError(f"memory_bytes must not be negative, got {memory_bytes}")

    return [
        GPU(
            device_index=i,
            uuid=_fake_uuid(worker_id, i),
            total_memory_bytes=memory_bytes,
        )
        for i in range(count)
    ]


def _fake_uuid(worker_id: str, device_index: int) -> str:
    """A deterministic, sha256-derived identifier shaped like NVML's
    "GPU-<uuid>" convention — the "fake-" segment makes it unmistakable
    that this was never read from real hardware.
    """
    digest = hashlib.sha256(f"{worker_id}:{device_index}".encode()).hexdigest()
    return f"GPU-fake-{digest[0:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"
