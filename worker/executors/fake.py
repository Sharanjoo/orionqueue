"""Deterministic fake-GPU job executor — see docs/adr/0003-fake-vs-real-gpu.md.

Simulates running a job by stepping through a fixed number of steps, each
taking a configurable, deterministic duration, with an optional
configurable step to fail at for reproducible failure-injection demos and
tests. No real container/process execution or GPU compute happens — this
is what "fake GPU mode" means for job *execution* specifically, as
opposed to gpu/fake.py's simulated *inventory*.

Configuration comes from the job's `command` field. Phase 6 has no real
image/command to execute in fake-GPU mode, so `command` becomes the
simulated executor's own configuration instead of adding new Job/proto
fields just for this — a deliberate reuse, not a misuse, of an existing
field. Recognized flags (all optional, unrecognized entries are ignored):

    --steps=N          number of steps to run (default 5)
    --step-seconds=X   seconds to sleep per step (default 1.0)
    --fail-at-step=N   raise a simulated failure when starting step N
                       (1-indexed; default: never fail)

Example: a job submitted with
command=["--steps=3", "--fail-at-step=2", "--step-seconds=0.5"] runs step
1 successfully, then fails at the start of step 2.
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable

DEFAULT_STEPS = 5
DEFAULT_STEP_SECONDS = 1.0


@dataclasses.dataclass(frozen=True)
class Config:
    steps: int = DEFAULT_STEPS
    step_seconds: float = DEFAULT_STEP_SECONDS
    fail_at_step: int | None = None


@dataclasses.dataclass(frozen=True)
class Outcome:
    success: bool
    steps_completed: int
    failure_reason: str = ""
    # stopped distinguishes a cooperative stop (should_stop returned True —
    # Phase 7 cancellation/preemption) from a genuine failure
    # (--fail-at-step or a timeout). Both leave success=False, but the
    # caller (agent/main.py) needs to know which happened: a genuine
    # failure is reported via JobService.ReportJobFailed (subject to
    # retry), while a cooperative stop is reported via
    # JobService.ReportJobStopped (never retried — the server already
    # knows why it asked to stop). Always False unless should_stop is what
    # ended the run.
    stopped: bool = False


def parse_command(command: list[str]) -> Config:
    """Parses a job's `command` list into an executor Config."""
    steps = DEFAULT_STEPS
    step_seconds = DEFAULT_STEP_SECONDS
    fail_at_step: int | None = None

    for arg in command:
        if arg.startswith("--steps="):
            steps = _parse_positive_int(arg.split("=", 1)[1], "--steps")
        elif arg.startswith("--step-seconds="):
            step_seconds = _parse_nonnegative_float(arg.split("=", 1)[1], "--step-seconds")
        elif arg.startswith("--fail-at-step="):
            fail_at_step = _parse_positive_int(arg.split("=", 1)[1], "--fail-at-step")

    return Config(steps=steps, step_seconds=step_seconds, fail_at_step=fail_at_step)


def _parse_positive_int(raw: str, flag: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"invalid integer for {flag}: {raw!r}") from exc
    if value < 1:
        raise ValueError(f"{flag} must be at least 1, got {value}")
    return value


def _parse_nonnegative_float(raw: str, flag: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"invalid float for {flag}: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{flag} must not be negative, got {value}")
    return value


def run(
    command: list[str],
    timeout_seconds: float = 0.0,
    on_step: Callable[[int, int], None] | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> Outcome:
    """Runs a simulated job to completion, failure, or timeout.

    on_step(step_number, total_steps), if given, is called after each
    step completes — a hook for progress logging (real progress
    *reporting* to the control plane is Phase 10, once there's a metrics
    pipeline for it to feed). should_stop(), if given, is polled between
    steps (not during a step's sleep) and stops execution early with
    success=False, stopped=True if it ever returns True — this is what
    agent/main.py wires to a per-job threading.Event so a cooperative
    cancellation or preemption stop signal (Phase 7) actually interrupts a
    running fake job instead of letting it run to completion regardless.

    timeout_seconds <= 0 means no timeout. The timeout is checked between
    steps, not preemptively during a step's sleep — coarse-grained, but
    adequate given step_seconds is always small relative to any
    reasonable timeout in practice.
    """
    cfg = parse_command(command)
    start = time.monotonic()

    for step in range(1, cfg.steps + 1):
        if should_stop is not None and should_stop():
            return Outcome(
                success=False,
                steps_completed=step - 1,
                failure_reason="stopped before completion",
                stopped=True,
            )

        if cfg.fail_at_step is not None and step == cfg.fail_at_step:
            return Outcome(
                success=False,
                steps_completed=step - 1,
                failure_reason=f"simulated failure at step {step} of {cfg.steps} (--fail-at-step)",
            )

        if timeout_seconds > 0 and (time.monotonic() - start) > timeout_seconds:
            return Outcome(
                success=False,
                steps_completed=step - 1,
                failure_reason=f"timed out after {timeout_seconds}s (exceeded before step {step})",
            )

        time.sleep(cfg.step_seconds)

        if on_step is not None:
            on_step(step, cfg.steps)

    return Outcome(success=True, steps_completed=cfg.steps)
