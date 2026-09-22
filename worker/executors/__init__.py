"""Job executors for the OrionQueue worker agent.

Phase 6 provides only the deterministic fake-GPU executor
(:mod:`executors.fake`); real execution against actual GPU hardware
(CUDA/PyTorch/NCCL) is added in Phase 9. See
docs/adr/0003-fake-vs-real-gpu.md.
"""

__all__ = ["fake"]
