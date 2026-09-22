"""Entry point for the OrionQueue worker agent.

Discovers (simulated, see gpu/fake.py) GPU inventory, registers with the
control plane's WorkerService, then heartbeats on the interval the server
told it to (RegisterWorkerResponse.heartbeat_interval_seconds) until
interrupted. Registration retries with backoff if the control plane is
briefly unreachable at startup; a heartbeat failure is logged and retried
on the next normal interval rather than treated as fatal, since the
server-side lease TTL is already sized to tolerate a few missed
heartbeats (see internal/workers.Service and ADR-0001).

Phase 6 scope: each heartbeat response may carry newly assigned jobs
(WorkerHeartbeatResponse.assigned_jobs — see ADR-0004-ish note in
grpc_client.py's docstring). Each assigned job is executed in its own
daemon thread via executors.fake.run(), reporting Started/Completed/Failed
to the control plane around it, so a long-running (simulated) job never
blocks the heartbeat loop — a missed heartbeat during a slow job would
otherwise risk the worker being declared LOST (Phase 4) while it's still
healthy and simply busy. Real GPU execution replacing executors.fake is
Phase 9.

Phase 7 scope: a heartbeat response may also carry stop_job_ids — jobs
this worker should cooperatively stop (a user cancelled a RUNNING job, or
the scheduler preempted it for a higher-priority one — the worker doesn't
need to know or care which). Each running job's thread has its own
threading.Event, passed to executors.fake.run() as should_stop; setting
it makes that job's run() return early with stopped=True on its next
between-step check, and the thread reports that via
JobService.ReportJobStopped instead of Completed/Failed. Note that a
SIGTERM/SIGINT to the worker process itself is still NOT a graceful
drain: it abandons any still-running job threads outright (they're daemon
threads and die with the process), relying on the existing lease-expiry +
LoseWorker recovery path (Phase 4/6) to requeue or fail/cancel/preempt
them, exactly as if the worker had crashed. Only a server-initiated stop
signal is cooperative; process shutdown is not.
"""

from __future__ import annotations

import signal
import sys
import threading
import time
from types import FrameType

import grpc

from agent import __version__
from agent.config import Config, load
from agent.grpc_client import AssignedJob, WorkerClient
from agent.logging_setup import configure
from executors import fake as executor_fake
from gpu import fake as gpu_fake

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
        gpus = gpu_fake.discover(cfg.worker_id, cfg.fake_gpu_count, cfg.fake_gpu_memory_bytes)
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


def run(cfg: Config, logger, client: WorkerClient, gpus: list[gpu_fake.GPU]) -> int:
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

    running_jobs: dict[str, threading.Thread] = {}
    cancel_events: dict[str, threading.Event] = {}
    running_jobs_lock = threading.Lock()

    while not shutdown_requested["value"]:
        _sleep_in_chunks(heartbeat_interval, shutdown_requested)
        if shutdown_requested["value"]:
            break
        try:
            with running_jobs_lock:
                running_job_ids = list(running_jobs.keys())
            result = client.heartbeat(worker_id, gpus, running_job_ids=running_job_ids)
            logger.info(
                "heartbeat sent",
                extra={
                    "fields": {
                        "worker_id": worker_id,
                        "running_jobs": len(running_job_ids),
                        "newly_assigned": len(result.assigned_jobs),
                        "stop_requested": len(result.stop_job_ids),
                    }
                },
            )
        except grpc.RpcError as exc:
            logger.error(
                "heartbeat failed, will retry on the next interval",
                extra={"fields": {"worker_id": worker_id, "error": str(exc)}},
            )
            continue

        for job in result.assigned_jobs:
            with running_jobs_lock:
                if job.job_id in running_jobs:
                    continue
            _start_job(
                logger, client, worker_id, job, running_jobs, cancel_events, running_jobs_lock
            )

        for job_id in result.stop_job_ids:
            with running_jobs_lock:
                event = cancel_events.get(job_id)
            if event is not None:
                event.set()
                logger.info(
                    "stop signal delivered to job's execution thread",
                    extra={"fields": {"job_id": job_id, "worker_id": worker_id}},
                )
            # If event is None, the job isn't (or isn't yet) running here —
            # e.g. it's still on the assigned_jobs side of this very
            # heartbeat response and hasn't started a thread yet, or it
            # already finished and reported terminal on its own. Either
            # way there's nothing to signal; a job that starts moments
            # later will simply appear in stop_job_ids again on the next
            # heartbeat (the server keeps offering it until it sees
            # ReportJobStopped), so nothing is lost.

    logger.info("stopped")
    return 0


def _start_job(
    logger,
    client: WorkerClient,
    worker_id: str,
    job: AssignedJob,
    running_jobs: dict[str, threading.Thread],
    cancel_events: dict[str, threading.Event],
    running_jobs_lock: threading.Lock,
) -> None:
    """Starts one assigned job in its own daemon thread, so job execution
    (which, even in fake-GPU mode, is deliberately modeled as taking real
    wall-clock time — see executors/fake.py) never blocks the heartbeat
    loop above. The thread reports Started before executing and
    Completed/Failed/Stopped after, then removes itself from running_jobs
    (and cancel_events) so the next heartbeat's running_job_ids reflects
    reality and the job isn't started a second time.

    cancel_events holds one threading.Event per running job, checked by
    executors.fake.run()'s should_stop hook — see run()'s stop_job_ids
    handling above, which is what actually sets one.
    """

    cancel_event = threading.Event()

    def _execute() -> None:
        job_id = job.job_id
        try:
            client.report_started(job_id, worker_id)
            logger.info("job started", extra={"fields": {"job_id": job_id, "worker_id": worker_id}})
        except grpc.RpcError as exc:
            if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                # The job is no longer SCHEDULED on the server (e.g. it
                # was cancelled — SCHEDULED jobs go straight to CANCELLED,
                # with no stop signal involved, since nothing is executing
                # them yet — between the heartbeat that offered it and
                # this call). It must not be executed at all: clean up and
                # return without ever calling executor_fake.run().
                logger.info(
                    "job is no longer SCHEDULED on the control plane, not executing it",
                    extra={"fields": {"job_id": job_id, "error": str(exc)}},
                )
                with running_jobs_lock:
                    running_jobs.pop(job_id, None)
                    cancel_events.pop(job_id, None)
                return
            # Any other error (network/unavailable/etc.) is logged but
            # doesn't stop execution — the control plane already believes
            # this job is SCHEDULED to us, and the subsequent
            # Completed/Failed/Stopped report is what actually matters
            # for the job's terminal state.
            logger.error(
                "failed to report job started, executing anyway",
                extra={"fields": {"job_id": job_id, "error": str(exc)}},
            )

        outcome = executor_fake.run(
            job.command, timeout_seconds=job.timeout_seconds, should_stop=cancel_event.is_set
        )

        try:
            if outcome.success:
                client.report_completed(job_id, worker_id, exit_code=0)
                logger.info(
                    "job completed",
                    extra={
                        "fields": {"job_id": job_id, "steps_completed": outcome.steps_completed}
                    },
                )
            elif outcome.stopped:
                client.report_stopped(job_id, worker_id)
                logger.info(
                    "job stopped (cancelled or preempted)",
                    extra={
                        "fields": {"job_id": job_id, "steps_completed": outcome.steps_completed}
                    },
                )
            else:
                client.report_failed(job_id, worker_id, outcome.failure_reason)
                logger.warning(
                    "job failed",
                    extra={
                        "fields": {
                            "job_id": job_id,
                            "reason": outcome.failure_reason,
                            "steps_completed": outcome.steps_completed,
                        }
                    },
                )
        except grpc.RpcError as exc:
            logger.error(
                "failed to report job outcome to control plane",
                extra={"fields": {"job_id": job_id, "error": str(exc)}},
            )
        finally:
            with running_jobs_lock:
                running_jobs.pop(job_id, None)
                cancel_events.pop(job_id, None)

    thread = threading.Thread(target=_execute, name=f"orionqueue-job-{job.job_id}", daemon=True)
    with running_jobs_lock:
        running_jobs[job.job_id] = thread
        cancel_events[job.job_id] = cancel_event
    thread.start()


def _register_with_retry(
    cfg: Config,
    logger,
    client: WorkerClient,
    gpus: list[gpu_fake.GPU],
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
