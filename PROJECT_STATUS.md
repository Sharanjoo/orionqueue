# OrionQueue — Project Status

Last updated: 2026-09-22 (Phase 2)

## Current phase

**Phase 2 — Protobuf and API layer.** Complete.

Jobs can be submitted, looked up, listed, cancelled, and retried through
both a real gRPC server and a REST gateway generated from the same
protobuf source, backed by an in-memory store. See
[docs/architecture/system-overview.md](docs/architecture/system-overview.md)
for the design and [README.md](README.md) for runnable examples.

## Phase tracker

| Phase | Name | Status |
|---|---|---|
| 0 | Repository inspection & technical design | Done |
| 1 | Project foundation | Done |
| 2 | Protobuf and API layer | Done |
| 3 | Persistence (PostgreSQL) | Not started |
| 4 | Worker registration & heartbeats | Not started |
| 5 | Scheduler | Not started |
| 6 | Job execution | Not started |
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
  - `proto/orionqueue/v1/{job,job_service}.proto`: the `Job` domain message
    and `JobService` (SubmitJob, GetJob, ListJobs, CancelJob, RetryJob),
    compiled with `buf` into Go gRPC server/client code, grpc-gateway REST
    handlers, and OpenAPI/Swagger docs (`docs/api/`). `WatchJob` and the
    worker-facing RPCs (RegisterWorker, WorkerHeartbeat, AssignJob, ...)
    are deliberately not in this proto yet — added in Phase 3/4/5 once
    something exists to implement them against.
  - `internal/jobs`: transport-agnostic domain model, state machine,
    request validation, an in-memory `Repository`, and a `Service`
    (submit/get/list/cancel/retry) — no gRPC or protobuf imports.
  - `internal/api`: `JobServer` (adapts `jobs.Service` to the generated
    gRPC interface), domain-error → gRPC-status mapping
    (`ValidationError`→`InvalidArgument`, `ErrNotFound`→`NotFound`,
    `ErrInvalidState`/`ErrRetryLimitExceeded`→`FailedPrecondition`), the
    REST gateway (in-process, no network hop to the gRPC listener), and
    HTTP middleware (`WithRequestID` for correlation IDs + structured
    per-request logs, `WithAuthPlaceholder` as a documented no-op).
  - `cmd/api`: runs a real gRPC server (with server reflection, so
    `grpcurl` works with no local `.proto` files) and the REST gateway
    together on one process, with coordinated graceful shutdown of both.
  - Idempotent job submission (`submission_id`), idempotent cancellation,
    pagination (`page_size`/`page_token`) and state filtering on
    `ListJobs`.
- **Simulated:** nothing yet — no fake-GPU/fake-worker code exists until
  Phase 4/6.
- **Not yet validated:** nothing GPU- or cloud-related exists yet.
- **Explicit Phase 2 scope boundary (not a gap):** `RetryJob` is fully
  implemented and tested, but no job can reach `FAILED` through the API
  yet (nothing executes jobs until Phase 6) — its tests construct a
  `FAILED` job directly via the repository. Cancelling a `RUNNING` job
  (graceful termination) is Phase 7; today only `QUEUED`/`RETRYING` jobs
  can be cancelled, which is every state a job can actually be in right
  now.

## Known issues

- **Job state is in-memory only** — an API restart loses all jobs. This is
  Phase 2's intentional scope; Phase 3 replaces `jobs.MemoryRepository`
  with a PostgreSQL-backed implementation of the same `jobs.Repository`
  interface, so nothing above that layer changes.
- Carried over from Phase 1: `golangci-lint` and `gcc` (for `-race`) aren't
  installed locally; both run in CI. See PROJECT_STATUS.md's Phase 1
  section in git history, or `docs/adr/0000-local-tooling-adaptations.md`.
- Full OS-signal-triggered graceful shutdown of `cmd/api`'s combined
  HTTP+gRPC process isn't independently verified on this Windows dev
  machine (same SIGTERM caveat as Phase 1); the shutdown code path reuses
  the same `ctx.Done()` → `Shutdown()` pattern already unit-tested in
  `internal/health`, plus `grpc.Server.GracefulStop()`, a standard
  library-adjacent primitive.

## Measured results

All of the following are actual outputs from this session:

- `go test ./... -cover`: **all packages passing.** Coverage:
  `internal/config` 97.6%, `internal/health` 81.6%, `internal/jobs` 90.5%,
  `internal/api` 89.9%, `internal/logging` 87.5%. (`cmd/api`/`cmd/scheduler`
  remain 0%-covered process wiring by design, same as Phase 1.)
  `internal/api` includes real HTTP round-trip tests
  (`net/http/httptest`) through the actual grpc-gateway wiring, not just
  direct Go method calls.
- `buf lint`: clean. `buf generate` output matches what's committed (no
  generation drift).
- Manual smoke test against the **compiled, running binary** (not just
  `go test`), both bare and inside `docker compose up`:
  - `POST /api/v1/jobs` → 200, returns a `QUEUED` job.
  - `GET /api/v1/jobs/{id}` → 200 with the submitted job.
  - `GET /api/v1/jobs` → 200, lists it.
  - `POST /api/v1/jobs/{id}/cancel` → 200, job → `CANCELLED`; calling it
    again on the same job → 200 unchanged (idempotent).
  - `POST /api/v1/jobs/{id}/retry` on a `CANCELLED` job → 400
    (`FailedPrecondition`, which is grpc-gateway's standard HTTP mapping
    for that code — not 409, confirmed by actually calling it rather than
    assumed).
  - Two `SubmitJob` calls with the same `submission_id` → identical job ID
    both times, exactly one job stored.
  - `grpcurl -plaintext 127.0.0.1:9080 ...` against the real gRPC
    listener (server reflection): `list`, `SubmitJob`, `ListJobs` all
    confirmed working, and a job submitted via gRPC was visible via REST
    immediately after (same underlying `jobs.Service` instance).
  - `docker compose up`: all 4 images rebuilt and started; `api` reported
    Docker `healthy`; both REST (port 7080) and gRPC (port 9080) worked
    from outside the container.

## Assumptions and environment notes

- Dev machine and toolchain notes carried over from Phase 0/1 (Windows 11,
  no local GPU, missing `make`/`protoc`/`etcd`/`psql`/`golangci-lint`/`gcc`)
  — see `docs/adr/0000-local-tooling-adaptations.md`. Added this phase:
  `buf`, `protoc-gen-go`, `protoc-gen-go-grpc`, `protoc-gen-grpc-gateway`,
  `protoc-gen-openapiv2`, and `grpcurl`, all installed via `go install`
  (no system `protoc` needed, as ADR-0000 planned).
- The Go module now has real external dependencies (`google.golang.org/grpc`,
  `google.golang.org/protobuf`, `github.com/grpc-ecosystem/grpc-gateway/v2`)
  — `go.sum` is committed and both Go Dockerfiles copy it.
- Per explicit user instruction, Claude does not run `git commit` or
  `git push` in this repo — every phase's report includes the exact
  commands for the user to run instead. Phase 0 and Phase 1 have already
  been committed and pushed by the user.

## Next phase

**Phase 3 — Persistence**: PostgreSQL schema and migrations (via
`golang-migrate`) for jobs, job attempts, workers, GPUs, leases,
checkpoints, job events, and scheduling decisions; a PostgreSQL-backed
`jobs.Repository` implementation (swapped in behind the existing
interface, no API-layer changes); integration tests against a real
Postgres (Docker); and confirming job state survives an API restart.
