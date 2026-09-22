"""Entry point for the OrionQueue worker agent.

Phase 4 scope: discover (simulated, see gpu/fake.py) GPU inventory,
register with the control plane's WorkerService, then heartbeat on the
interval the server told it to (RegisterWorkerResponse.heartbeat_interval_seconds)
until interrupted. Registration retries with backoff if the control plane
is briefly unreachable at startup; a heartbeat failure is logged and
retried on the next normal interval rather than treated as fatal, since
the server-side lease TTL is already sized to tolerate a few missed
heartbeats (see internal/workers.Service and ADR-0001). Job execution is
added in Phase 6.
"""

from __future__ import annotations

import signal
import sys
import time
from types import FrameType

import grpc

from agent import __version__
from agent.config import Config, load
from agent.grpc_client import WorkerClient
from agent.logging_setup import configure
from gpu import fake

_SLEEP_CHUNK_SECONDS = 0.2
_REGISTER_INITIAL_BACKOFF_SECONDS = 1.0
_REGISTER_MAX_BACKOFF_SECONDS = 15.0


def main(argv: list[str] | None = None) -> int:
    try:
        cfg = load()
    except ValueError as exc:
        sys.stderr.write(json_error_line(str(exc)))
        return 1

    logger = configure("orionqueue-worker", cfg.environment, cfg.log_level)
    logger.info(
        "starting",
        extra={
            "fields": {
                "worker_id": cfg.worker_id,
                "hostname": cfg.hostname,
                "fake_gpu": cfg.fake_gpu,
            }
        },
    )

    if cfg.fake_gpu:
        gpus = fake.discover(cfg.worker_id, cfg.fake_gpu_count, cfg.fake_gpu_memory_bytes)
        logger.info(
            "discovered simulated GPU inventory",
            extra={"fields": {"gpu_count": len(gpus), "simulated": True}},
        )
    else:
        gpus = []
        logger.warning("real GPU discovery is not implemented yet (Phase 9); reporting no GPUs")

    client = WorkerClient(cfg.api_grpc_addr)
    try:
        return run(cfg, logger, client, gpus)
    finally:
        client.close()


def run(cfg: Config, logger, client: WorkerClient, gpus: list[fake.GPU]) -> int:
    shutdown_requested = _install_signal_handlers(logger)

    worker_id, heartbeat_interval = _register_with_retry(
        cfg, logger, client, gpus, shutdown_requested
    )
    if worker_id is None:
        logger.info("stopped (shutdown requested before registration completed)")
        return 0

    logger.info(
        "registered",
        extra={
            "fields": {
                "worker_id": worker_id,
                "heartbeat_interval_seconds": heartbeat_interval,
            }
        },
    )

    while not shutdown_requested["value"]:
        _sleep_in_chunks(heartbeat_interval, shutdown_requested)
        if shutdown_requested["value"]:
            break
        try:
            client.heartbeat(worker_id, gpus, running_job_ids=[])
            logger.info("heartbeat sent", extra={"fields": {"worker_id": worker_id}})
        except grpc.RpcError as exc:
            logger.error(
                "heartbeat failed, will retry on the next interval",
                extra={"fields": {"worker_id": worker_id, "error": str(exc)}},
            )

    logger.info("stopped")
    return 0


def _register_with_retry(
    cfg: Config,
    logger,
    client: WorkerClient,
    gpus: list[fake.GPU],
    shutdown_requested: dict,
):
    delay = _REGISTER_INITIAL_BACKOFF_SECONDS
    attempt = 0
    while not shutdown_requested["value"]:
        attempt += 1
        try:
            result = client.register(
                hostname=cfg.hostname,
                cpu_capacity=cfg.cpu_capacity,
                memory_capacity_bytes=cfg.memory_capacity_bytes,
                gpus=gpus,
                software_version=__version__,
                labels={"environment": cfg.environment},
            )
            return result.worker_id, max(result.heartbeat_interval_seconds, 1.0)
        except grpc.RpcError as exc:
            logger.warning(
                "registration attempt failed, retrying",
                extra={
                    "fields": {
                        "attempt": attempt,
                        "error": str(exc),
                        "retry_in_seconds": delay,
                    }
                },
            )
            _sleep_in_chunks(delay, shutdown_requested)
            delay = min(delay * 2, _REGISTER_MAX_BACKOFF_SECONDS)
    return None, None


def _sleep_in_chunks(seconds: float, shutdown_requested: dict) -> None:
    """Sleeps for `seconds`, but in small increments so a shutdown signal
    arriving mid-sleep (during a long heartbeat interval or a backoff
    delay) is noticed promptly instead of after the full sleep completes.
    """
    remaining = seconds
    while remaining > 0 and not shutdown_requested["value"]:
        chunk = min(_SLEEP_CHUNK_SECONDS, remaining)
        time.sleep(chunk)
        remaining -= chunk


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
