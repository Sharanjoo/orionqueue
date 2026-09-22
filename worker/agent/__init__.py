"""OrionQueue worker agent.

Registers with the control plane, reports GPU resources, and executes
jobs. Phase 1 scope is process foundation only (config, structured
logging, startup/shutdown) — registration and execution land in Phase 4
onward. See ../../docs/architecture/system-overview.md.
"""

__version__ = "0.1.0"
