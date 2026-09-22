# OrionQueue — Project Status

Last updated: 2026-09-22 (Phase 6)

## Current phase

**Phase 6 — Job execution.** Complete.

A SCHEDULED job now actually runs. The control plane delivers job
assignments to workers piggybacked on the existing heartbeat response
(`WorkerHeartbeatResponse.assigned_jobs`); each worker executes assigned
jobs on its own daemon thread via a deterministic fake-GPU executor
(`worker/executors/fake.py`) so a long job never blocks the heartbeat
loop; the worker reports `ReportJobStarted`/`ReportJobCompleted`/
`ReportJobFailed` back to the control plane around execution; failures
retry (`FAILED` → requeued `QUEUED` with `current_attempt` incremented)
until `retry_limit` is exhausted, then reach terminal `FAILED`; and a
worker that disappears mid-job (lease expiry, connecting Phase 4's
detection to this phase's job lifecycle via `Service.LoseWorker`)
requeues or fails its in-flight jobs exactly like an explicit failure
would. Verified live against the real running stack for all three paths
(success, retry-then-fail, worker loss) — see Measured results.

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
| 7 | Cancellation and preemption | Not started |
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
    gains `assigned_jobs` — job delivery is piggybacked on the existing
    heartbeat cycle rather than a new push/poll RPC, so a worker never has
    to guess when to ask.
  - `proto/orionqueue/v1/job_service.proto`: `ReportJobStarted`,
    `ReportJobCompleted`, `ReportJobFailed` RPCs (gRPC-only, worker→control
    plane, no REST binding — these aren't client-facing).
  - `internal/jobs.Service`: `ListAssignedToWorker` (feeds the heartbeat
    response), `Start`/`Complete`/`Fail` (the SCHEDULED→RUNNING→
    SUCCEEDED/FAILED transitions), `LoseWorker` (bulk-recovers every
    active job assigned to a worker whose lease just expired) — all
    guarded by the same row-locked `Update` pattern used since Phase 3.
    `Fail`'s retry logic (`applyFailureOrRetry`) is shared by both the
    explicit-failure and worker-loss paths, so they behave identically by
    construction rather than by two independently-maintained copies.
  - `internal/api`: `JobServer.ReportJobStarted/Completed/Failed`;
    `WorkerServer.WorkerHeartbeat` now also lists and attaches the
    worker's newly-SCHEDULED jobs to the response.
  - `cmd/api`: the Phase 4 `WatchExpirations` "worker marked LOST" callback
    now also calls `jobSvc.LoseWorker`, connecting lease-expiry detection
    to job recovery for the first time.
  - `worker/executors/fake.py`: the deterministic fake-GPU job executor —
    steps through a configurable number of steps (`--steps`), each a
    configurable sleep (`--step-seconds`), with an optional configurable
    failure point (`--fail-at-step`) for reproducible failure-injection
    demos. Configuration is read from the job's existing `command` field
    (no new proto fields) since fake-GPU mode has no real image/command to
    execute — documented as a deliberate reuse, not a misuse.
  - `worker/agent/grpc_client.py`: `report_started`/`report_completed`/
    `report_failed`; `heartbeat()` now parses and returns `assigned_jobs`
    as plain `AssignedJob` objects, decoupled from the raw protobuf type.
  - `worker/agent/main.py`: each newly assigned job is executed on its own
    daemon thread (`_start_job`), reporting Started before execution and
    Completed/Failed after, tracked in a lock-guarded `running_jobs` dict
    so (a) a job already running is never started a second time even if
    it reappears in `assigned_jobs`, and (b) `running_job_ids` sent on
    later heartbeats reflects reality. Running execution on a background
    thread — rather than inline in the heartbeat loop — is what stops a
    long (even simulated) job from starving heartbeats and getting the
    worker wrongly declared LOST mid-job.
- **Simulated:** job *execution* is fake-GPU only (`executors/fake.py`) —
  no real container/process/GPU compute happens; only the timing, retry,
  and failure-injection behavior is real. GPU inventory/utilization
  reporting is unchanged from Phase 4 (still simulated, still doesn't move
  during execution — that requires Phase 9's real executor or a fake one
  that fakes utilization, neither of which exists yet).
- **Not yet validated:** real GPU/NCCL (Phase 9), full observability
  (Phase 10), and cloud deployment (Phase 13) remain unvalidated, as
  before.
- **Explicit Phase 6 scope boundaries (not gaps):**
  - No graceful cancellation or draining: a SIGTERM/SIGINT to the worker
    process does not attempt to stop or report in-flight jobs — they're
    daemon threads and die with the process, then get recovered through
    the same lease-expiry + `LoseWorker` path as a hard crash. Graceful
    cancellation (`CancelJob` reaching a RUNNING job) is Phase 7 — `Cancel`
    still rejects RUNNING jobs today, unchanged from Phase 3.
  - `ReportJobStarted`/`Completed`/`Failed` trust the caller's
    `worker_id` — there's no check that the reporting worker is actually
    the one the job was assigned to. Low risk today (nothing but the
    worker agent itself calls these RPCs, and they're gRPC-only, not
    REST-exposed), but a real multi-tenant deployment would need it;
    tracked as a gap, not fixed here to stay in scope.
  - Progress reporting (a `Progress` RPC, or per-step progress in the Job
    record) does not exist — `executors.fake.run()`'s `on_step` hook is
    wired to structured logs only. Deferred to Phase 10 once there's a
    metrics/observability pipeline for progress to feed.

## Known issues

- Carried over from Phase 5: port workarounds (8080, 5432→5433); no local
  `golangci-lint`/`gcc`; Windows SIGTERM caveat (now doubly relevant, since
  it's also how a worker's in-flight jobs get abandoned rather than
  drained — see the scope boundary above); `internal/persistence` at 0%
  coverage under the default (non-Docker) test run by design.
- No new production-code bugs found this phase. One pre-existing lint
  finding (`typing.Callable` instead of `collections.abc.Callable` in
  `executors/fake.py`, flagged by ruff's `UP035`) was fixed while running
  the full lint suite for this phase's report.

## Measured results

All of the following are actual outputs from this session:

- `go build ./...`, `go vet ./...`: clean.
- `go test ./... -cover` (no Docker): all packages pass, **169 Go unit
  test functions total** (up from 149 after Phase 5). New this phase:
  `internal/jobs` 92.8% coverage (14 new tests: `ListAssignedToWorker`,
  `Start`/`Complete`/`Fail` transitions and their rejection paths,
  `LoseWorker` requeue/terminal-fail/no-op cases); `internal/api` 88.9%
  coverage (8 new tests covering the 3 new RPCs' full lifecycle path,
  empty-ID validation, and not-found/wrong-state error mapping, plus the
  heartbeat's `assigned_jobs` behavior).
- `gofmt -l .`, `buf lint`: clean.
- `worker`: **64 pytest tests passing** (up from 41 before this phase —
  13 for the new fake executor, plus new/expanded coverage for
  `grpc_client.py`'s lifecycle-reporting methods and `main.py`'s threaded
  job-execution loop, including tests against a real in-process gRPC
  server rather than a mocked stub). `ruff check .` and `black --check .`:
  clean (one pre-existing `ruff` finding, `UP035` in `executors/fake.py`,
  fixed as part of this pass).
- **Full live-stack verification via `docker compose up --build`**, not
  just automated tests — all three job-lifecycle paths exercised against
  the real running stack with real `curl` polling and real worker/API
  container logs, not simulated or asserted from code reading alone:
  - **Success path:** submitted a 3-step job (`--steps=3 --step-seconds=1`).
    Observed `JOB_STATE_QUEUED` → `JOB_STATE_SCHEDULED` →
    `JOB_STATE_RUNNING` → `JOB_STATE_SUCCEEDED` by polling
    `GET /api/v1/jobs/{id}`; worker log showed `"job started"` then
    `"job completed" steps_completed=3` exactly 3 seconds apart.
  - **Retry-then-terminal-fail path:** submitted a job with
    `--fail-at-step=2` and `retry_limit=2`. First attempt (`current_attempt=1`)
    failed and was requeued to `JOB_STATE_QUEUED` with
    `current_attempt=2` and `failure_reason="simulated failure at step 2
    of 3 (--fail-at-step)"` preserved; second attempt ran and reached
    terminal `JOB_STATE_FAILED`. A control job with `retry_limit=1`
    (`current_attempt` starts at 1, so `1 < 1` is false) went straight to
    terminal `FAILED` on its one and only attempt, confirming
    `retry_limit` means "total attempts allowed", not "retries after the
    first".
  - **Worker-loss mid-execution recovery:** submitted a 30-step job, then
    ran `docker kill orionqueue-worker-1` (SIGKILL, no graceful shutdown,
    no restart policy — confirmed by its absence from `docker compose ps`
    afterward and no `"stopped"` log line) while the job was
    SCHEDULED/about to run. The job stayed `JOB_STATE_RUNNING` in the
    database (last known state) until the worker's 20s etcd lease
    expired; api-1's log recorded
    `"worker marked LOST (lease expired without a renewing heartbeat)"`
    followed immediately by `"recovered jobs from lost worker"
    recovered=1`, and the job flipped to `JOB_STATE_QUEUED` with
    `current_attempt=2`, `assigned_worker_ids` cleared. Restarting the
    worker (`docker compose up -d worker`) picked the requeued job back
    up on its next scheduling pass and ran it to `JOB_STATE_SUCCEEDED`.
  - Stack torn down cleanly (`docker compose down`) afterward.

## Assumptions and environment notes

- Carried over from Phase 0–5 (Windows 11 dev machine, no local GPU,
  missing `make`/`protoc`/`gcc`/`golangci-lint`, port workarounds for
  8080/5432) — see `docs/adr/0000-local-tooling-adaptations.md`.
- No new external dependencies this phase, Go or Python.
- Per explicit user instruction, Claude does not run `git commit` or
  `git push` in this repo — every phase's report includes the exact
  commands for the user to run instead. Phases 0–5 are already committed
  and pushed by the user.

## Next phase

**Phase 7 — Cancellation and preemption**: `CancelJob` needs to actually
reach a RUNNING job (today it still rejects that state, unchanged since
Phase 3) — meaning the control plane needs a way to signal a specific
worker/job for graceful stop, and the worker's job-execution thread needs
a cooperative stop mechanism (`executors.fake.run()`'s `should_stop` hook
already exists for exactly this and is unused until now). Priority-based
preemption of a lower-priority RUNNING job to make room for a
higher-priority one builds on the same mechanism, per ADR-0005.
