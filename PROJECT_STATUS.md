# OrionQueue — Project Status

Last updated: 2026-09-22 (Phase 7)

## Current phase

**Phase 7 — Cancellation and preemption.** Complete.

A user can now cancel a RUNNING job, and the scheduler can preempt a
lower-priority RUNNING job to make room for a higher-priority one —
both graceful, both proven live against the real stack.
`CancelJob` on a RUNNING (or CHECKPOINTING) job moves it to
`CANCEL_REQUESTED` instead of erroring (Phase 6's behavior); the control
plane delivers a stop signal to the assigned worker piggybacked on the
existing heartbeat (`WorkerHeartbeatResponse.stop_job_ids`, reusing the
same channel Phase 6's `assigned_jobs` established); the worker's
per-job execution thread is interrupted cooperatively via a
`threading.Event` wired into `executors.fake.run()`'s existing
`should_stop` hook; and the worker confirms via a new
`JobService.ReportJobStopped` RPC, which the server resolves based on
the job's own current state — `CANCEL_REQUESTED` → terminal `CANCELLED`,
or `PREEMPTED` → `QUEUED` (unassigned, `current_attempt` unchanged, so
preemption never counts against `retry_limit`). Preemption itself
(`internal/scheduler.PlanPreemptions`) is a second, opt-in scheduling
pass — off by default per ADR-0005, enabled via
`ORIONQUEUE_PREEMPTION_ENABLED` — that only considers `Preemptible`
RUNNING jobs, only at a strictly lower effective priority than the
pending job, preempting the fewest jobs needed on a single worker
(gang-scheduling-consistent) before falling back to leaving the pending
job queued. Cancellation and preemption share the same worker-side
stop-and-confirm mechanism by design, and `Service.LoseWorker` (Phase 6)
was extended to finalize both `CANCEL_REQUESTED` and `PREEMPTED` jobs
immediately if their worker is lost, without waiting for a confirmation
that can now never arrive.

## Phase tracker

| Phase | Name | Status |
|---|---|---|
| 0 | Repository inspection & technical design | Done |
| 1 | Project foundation | Done |
| 2 | Protobuf and API layer | Done |
| 3 | Persistence (PostgreSQL) | Done |
| 4 | Worker registration & heartbeats | Done |
| 5 | Scheduler | Done |
| 6 | Job execution | Done |
| 7 | Cancellation and preemption | Done |
| 8 | Checkpointing and recovery | Not started |
| 9 | Real GPU and NCCL execution | Not started |
| 10 | Observability | Not started |
| 11 | Dashboard | Not started |
| 12 | Docker Compose and Kubernetes | Not started |
| 13 | AWS and Terraform | Not started |
| 14 | Benchmarks and final hardening | Not started |

## Implemented vs. simulated vs. not yet validated

- **Implemented:**
  - `proto/orionqueue/v1/worker_service.proto`: `WorkerHeartbeatResponse`
    gains `stop_job_ids` — reuses Phase 6's heartbeat-piggyback channel
    for the opposite direction (stop, not start).
  - `proto/orionqueue/v1/job_service.proto`: `ReportJobStopped` RPC
    (gRPC-only, worker→control plane, same access pattern as Phase 6's
    Report* RPCs).
  - `internal/jobs.Service`: `Cancel` now moves a RUNNING/CHECKPOINTING
    job to `CANCEL_REQUESTED` instead of erroring, and is idempotent for
    an already-`CANCEL_REQUESTED` job; `ListStoppableForWorker` (feeds
    `stop_job_ids`); `ReportStopped` (resolves `CANCEL_REQUESTED` →
    `CANCELLED` or `PREEMPTED` → `QUEUED` based on the job's own current
    state); `Preempt` (`RUNNING` + `Preemptible` → `PREEMPTED`, refuses
    otherwise). `LoseWorker` extended to also finalize
    `CANCEL_REQUESTED`/`PREEMPTED` jobs immediately (no confirmation
    needed — a LOST worker can't still be executing anything).
  - `internal/api`: `JobServer.ReportJobStopped`;
    `WorkerServer.WorkerHeartbeat` now also attaches `stop_job_ids`.
  - `internal/scheduler.PlanPreemptions`: a pure function (same testing
    philosophy as `Plan`) computing the minimum set of `RUNNING`,
    `Preemptible` jobs to preempt, per worker, to fit a QUEUED job `Plan`
    couldn't place — lowest effective-priority-first, never preempting a
    job at an equal-or-higher priority than the pending one, never more
    jobs than necessary. `Service.RunOnce` runs it as a second, opt-in
    pass (gated by `WithPreemptionEnabled`) after the normal `Plan` pass;
    a new `Result.Preempted` counter reports how many jobs it preempted.
  - `internal/config`: `PreemptionEnabled` (`ORIONQUEUE_PREEMPTION_ENABLED`,
    default `false` per ADR-0005), wired into `cmd/scheduler`.
  - `worker/executors/fake.py`: `Outcome` gains `stopped: bool`,
    distinguishing a cooperative stop (`should_stop` returned `True`)
    from a genuine failure — both leave `success=False`, but the caller
    needs to know which to report correctly.
  - `worker/agent/grpc_client.py`: `report_stopped`; `HeartbeatResult`
    gains `stop_job_ids`.
  - `worker/agent/main.py`: each running job's thread now has its own
    `threading.Event` (`cancel_events`), wired to
    `executors.fake.run()`'s `should_stop` hook; the heartbeat loop sets
    the matching event for every ID in `stop_job_ids`, and the thread
    reports `Stopped` (not `Failed`) when that's why it ended. Also
    fixed a related correctness gap surfaced by this phase:
    `report_started` failing with `FAILED_PRECONDITION` (the job is no
    longer `SCHEDULED` server-side — e.g. it was cancelled between being
    offered and being started) now skips execution entirely instead of
    running it anyway.
- **Simulated:** unchanged from Phase 6 — job execution is still fake-GPU
  only; only the stop/retry/priority *behavior* around it is real.
- **Not yet validated:** real GPU/NCCL (Phase 9), full observability
  (Phase 10), and cloud deployment (Phase 13) remain unvalidated, as
  before.
- **Explicit Phase 7 scope boundaries (not gaps):**
  - No real checkpoint I/O on preemption: ADR-0005 says a preempted job
    with `checkpoint_interval_seconds` configured should be checkpointed
    before it's stopped, and resume from that checkpoint when
    rescheduled. There's no checkpoint store until Phase 8, so a
    preempted (or cancelled) job today always restarts from step 1 —
    demonstrated directly in this phase's live preemption test (the
    preempted job resumed at `steps_completed=0`, not where it left off).
    Documented, not silently dropped.
  - No graceful *process*-shutdown draining: a SIGTERM/SIGINT to the
    worker still abandons in-flight job threads outright (unchanged from
    Phase 6) — only a server-initiated stop signal (cancel/preempt) is
    cooperative. Recovered via the same lease-expiry + `LoseWorker` path
    either way.
  - `ReportJobStopped` (like Phase 6's other Report* RPCs) trusts the
    caller's `worker_id` without verifying assignment ownership — same
    carried-over, low-risk, gRPC-only gap noted in Phase 6.
  - Preemption is strictly per-worker (a pending job only benefits from
    freeing capacity on the one worker that would then fit it, never by
    combining partial capacity across workers) — consistent with `Plan`'s
    own single-worker gang-scheduling model, not a separate limitation.

## Known issues

- Carried over from Phase 6: port workarounds (8080, 5432→5433); no local
  `golangci-lint`/`gcc`; Windows SIGTERM caveat; `internal/persistence` at
  0% coverage under the default (non-Docker) test run by design.
- No new production-code bugs found this phase (the `report_started`
  `FAILED_PRECONDITION` handling above was a genuine gap this phase's own
  new functionality surfaced and fixed, not a regression).

## Measured results

All of the following are actual outputs from this session:

- `go build ./...`, `go vet ./...`, `gofmt -l .`, `buf lint`: clean.
- `go test ./... -cover` (no Docker): all packages pass, **199 Go unit
  test functions total** (up from 169 after Phase 6, via `go test ./...
  -list '.*'`). New this phase, by package: `internal/jobs` (93.0%
  coverage — `Cancel`'s new branches, `ListStoppableForWorker`,
  `ReportStopped`, `Preempt`, `LoseWorker`'s extension),
  `internal/scheduler` (92.6% coverage — `PlanPreemptions` unit tests
  covering minimum-set selection, non-preemptible/equal-priority
  protection, and the "even full preemption isn't enough" case, plus 2
  `RunOnce` integration tests proving the full preempt-then-reschedule
  round trip against real in-memory repositories), `internal/api` (88.6%
  coverage — `CancelJob` reaching RUNNING, `ReportJobStopped`'s two
  resolutions, heartbeat's `stop_job_ids`), and `internal/config`
  (97.3% coverage — `PreemptionEnabled`).
- `go test ./tests/integration/... -tags=integration`: **28/28 passing**
  (~214s) — unchanged count (Phase 7 added no new tables/queries; this
  run confirms the new job states round-trip through real PostgreSQL
  correctly, since `jobs_state_valid`'s `CHECK` constraint already
  whitelisted `CANCEL_REQUESTED`/`PREEMPTED` since Phase 3).
- `worker`: **70 pytest tests passing** (up from 64 after Phase 6).
  `ruff check .` and `black --check .`: clean.
- **Full live-stack verification via `docker compose up --build`**, not
  just automated tests:
  - **Cancellation of a RUNNING job:** submitted a 30-step job, waited for
    `JOB_STATE_RUNNING`, called `POST /api/v1/jobs/{id}/cancel`. Response
    showed `JOB_STATE_CANCEL_REQUESTED` immediately. Worker log recorded
    `"stop signal delivered to job's execution thread"` on its very next
    heartbeat, then `"job stopped (cancelled or preempted)"
    steps_completed=6` one second later (of 30 possible) — proving the
    job was actually interrupted, not left to run to completion. Polling
    `GET /api/v1/jobs/{id}` showed it reach terminal `JOB_STATE_CANCELLED`
    within one heartbeat interval.
  - **Preemption:** started the stack with
    `ORIONQUEUE_PREEMPTION_ENABLED=true` (confirmed in the scheduler's
    startup log: `preemption_enabled:true`). Submitted a 60-step,
    2-GPU, `preemptible:true`, priority-10 job and let it reach
    `JOB_STATE_RUNNING` on a 2-GPU worker (fully committing it), then
    submitted a 1-GPU, priority-90 job. Scheduler log recorded a pass
    with `preempted:1`; the low-priority job's state went
    `RUNNING` → `QUEUED` (worker log:
    `"job stopped (cancelled or preempted)" steps_completed=21`); the
    high-priority job was then scheduled and ran to
    `JOB_STATE_SUCCEEDED` on the freed GPU; the low-priority job was
    automatically rescheduled afterward and resumed running — with
    `current_attempt` unchanged throughout, confirming preemption never
    counts against `retry_limit`.
  - Stack torn down cleanly (`docker compose down`) after each demo.

## Assumptions and environment notes

- Carried over from Phase 0–6 (Windows 11 dev machine, no local GPU,
  missing `make`/`protoc`/`gcc`/`golangci-lint`, port workarounds for
  8080/5432) — see `docs/adr/0000-local-tooling-adaptations.md`.
- No new external dependencies this phase, Go or Python.
- Per explicit user instruction, Claude does not run `git commit` or
  `git push` in this repo — every phase's report includes the exact
  commands for the user to run instead. Phases 0–6 are already committed
  and pushed by the user (Phase 6's commit pending the user's own `git
  commit`/`git push` at the time this phase began).

## Next phase

**Phase 8 — Checkpointing and recovery**: a checkpoint storage
abstraction (per ADR-0004), `ReportCheckpoint`/checkpoint listing RPCs,
worker-side periodic checkpoint writes during execution
(`checkpoint_interval_seconds`, already accepted at submission time but
unused until now), and resuming a requeued (retried, preempted, or
worker-recovered) job from its latest valid checkpoint instead of always
restarting from step 1 — directly closing the gap this phase's own
Measured results section documented in preemption's "resumed from
`steps_completed=0`" behavior.
