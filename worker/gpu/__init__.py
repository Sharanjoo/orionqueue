"""GPU discovery for the OrionQueue worker agent.

Phase 4 provides only the deterministic fake-GPU backend
(:mod:`gpu.fake`); NVML-based real-hardware discovery is added in
Phase 9. See docs/adr/0003-fake-vs-real-gpu.md.
"""

__all__ = ["fake"]
