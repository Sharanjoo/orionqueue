# ADR-0003: Fake GPU vs. real GPU execution

Status: Accepted (Phase 0)

## Context

This machine has no NVIDIA GPU (`nvidia-smi` not found), and CI runners
generally don't either. The project must still demonstrate GPU-aware
scheduling, multi-GPU gang scheduling, checkpointing, and recovery in a way
that's runnable and testable by anyone, while also implementing a real
NVML/CUDA/NCCL path for when hardware is available — without ever presenting
simulated numbers as if they were measured on real hardware.

## Decision

Define one `GPUProvider` interface (`worker/gpu`) with two implementations:

- **Fake-GPU provider (default, used in local dev and CI):** deterministic,
  seeded simulation of GPU count, memory, utilization curves, execution
  time, and injectable failures/checkpoint steps. Every value this provider
  produces is tagged `simulated: true` in logs, metrics labels, and API
  responses, and the dashboard renders a visible "simulated" badge whenever
  the backend is running in this mode. Determinism means the same job spec +
  seed always produces the same simulated trace, which is what makes the
  failure-injection and checkpoint-recovery tests reproducible.
- **Real-GPU provider:** uses NVML for discovery (UUID, memory, utilization,
  device index) and a PyTorch/`torch.distributed`+NCCL launcher for actual
  multi-GPU execution. Before running a real job, the worker validates CUDA
  and NCCL availability and fails the job with a clear, specific error
  (not a silent fallback) if they aren't usable — falling back silently
  would make it easy to mistake a misconfigured node for a working one.

Mode selection: a worker auto-detects (NVML available and functional → real
mode; otherwise fake mode), overridable with `ORIONQUEUE_FAKE_GPU=1`/`0` for
explicit control in tests and demos.

## Consequences

- The entire system (scheduler, checkpointing, dashboard, failure recovery)
  is fully testable and demoable without any GPU hardware, which is required
  given this project's actual development environment.
- Every metric, log line, and UI element that comes from the fake provider is
  labeled as simulated at the source, not just in a README caveat — so a
  reader of raw metrics/logs can't mistake simulated numbers for measured
  ones even out of context.
- No benchmark or capability claim in this repo's docs will describe
  real-GPU/NCCL behavior unless it was actually run on real hardware and the
  output is captured under `docs/benchmarks/`. Until then, Phase 9's real-mode
  code is described as "implemented, not yet validated."
