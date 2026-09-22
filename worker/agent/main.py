"""Entry point for the OrionQueue worker agent.

Phase 1 scope: start up, load config, log structured startup/shutdown
messages, and run until interrupted. Registration with the control plane,
heartbeats, and job execution are added starting Phase 4.
"""

from __future__ import annotations

import signal
import sys
import time
from types import FrameType

from agent.config import load
from agent.logging_setup import configure

# How often the run loop wakes up to check for a shutdown request. Phase 1
# has nothing else to do in this loop; later phases replace this sleep
# with real heartbeat/lease-renewal work.
_POLL_INTERVAL_SECONDS = 0.2


def main(argv: list[str] | None = None) -> int:
    try:
        cfg = load()
    except ValueError as exc:
        sys.stderr.write(json_error_line(str(exc)))
        return 1

    logger = configure("orionqueue-worker", cfg.environment, cfg.log_level)
    logger.info(
        "starting",
        extra={"fields": {"worker_id": cfg.worker_id, "fake_gpu": cfg.fake_gpu}},
    )
    logger.info("phase 1 foundation: no registration/heartbeat loop yet (see Phase 4)")

    shutdown_requested = _install_signal_handlers(logger)
    while not shutdown_requested["value"]:
        time.sleep(_POLL_INTERVAL_SECONDS)

    logger.info("stopped")
    return 0


def json_error_line(message: str) -> str:
    """A minimal hand-built JSON line for the one case where the real JSON
    logger can't be used yet: configuration failed before it could be
    configured with a log level.
    """
    escaped = message.replace("\\", "\\\\").replace('"', '\\"')
    return f'{{"level": "error", "msg": "invalid configuration", "error": "{escaped}"}}\n'


def _install_signal_handlers(logger) -> dict[str, bool]:
    state = {"value": False}

    def _handle(signum: int, frame: FrameType | None) -> None:
        logger.info("shutdown signal received", extra={"fields": {"signal": signum}})
        state["value"] = True

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    return state


if __name__ == "__main__":
    raise SystemExit(main())
