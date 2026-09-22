# OrionQueue — Project Status

Last updated: 2026-09-22 (Phase 3)

## Current phase

**Phase 3 — Persistence.** Complete.

Job state is now durable: `cmd/api` is backed by PostgreSQL instead of an
in-memory store, and job state survives an API restart — verified by
actually submitting a job, restarting the `api` container, and confirming
the job was still retrievable afterward, not just by unit tests. See
[docs/architecture/system-overview.md](docs/architecture/system-overview.md)
for the design and [README.md](README.md) for runnable examples.

## Phase tracker

| Phase | Name | Status |
|---|---|---|
| 0 | Repository inspection & technical design | Done |
| 1 | Project foundation | Done |
| 2 | Protobuf and API layer | Done |
| 3 | Persistence (PostgreSQL) | Done |
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
  - `migrations/0001_create_jobs`, `0002_create_job_events`: PostgreSQL
    schema with a `CHECK` constraint mirroring `jobs.State`, a partial
    unique index on `submission_id` enforcing idempotent submission at
    the database level, and indexes supporting `ListJobs`' state filter
    and pagination. Applied via `golang-migrate`, embedded in the Go
    binary (`migrations/embed.go`) so tests don't depend on a filesystem
    path, and available as plain SQL files for the `migrate` CLI/Docker
    image.
  - `internal/persistence`: `Connect` (pooled, with bounded startup
    retry — Postgres inside Compose can still be starting when `api`'s
    own container is already up), `RunMigrations`, and `JobRepository`,
    a full `jobs.Repository` implementation with keyset pagination
    (more scalable than the in-memory repository's OFFSET-based scheme)
    and transactional event recording to `job_events` on every
    submission and state transition.
  - `cmd/api` now requires a reachable, migrated PostgreSQL to start —
    `jobs.MemoryRepository` (Phase 2) is no longer wired into it, though
    it remains in place and in active use by `internal/jobs` and
    `internal/api`'s fast, Docker-free unit/gateway tests. `/readyz` now
    performs a real database ping instead of always returning ready.
  - `docker-compose.yml`: a `postgres` service and a one-shot `migrate`
    service (the official `migrate/migrate` image); `api` waits for both
    (`depends_on` with `service_healthy` / `service_completed_successfully`)
    so `docker compose up` migrates and connects automatically.
  - `scripts/migrate.sh` (up/down/version, installs the `migrate` CLI on
    first use) and `scripts/test-integration.sh`.
- **Simulated:** nothing yet — no fake-GPU/fake-worker code exists until
  Phase 4/6.
- **Not yet validated:** nothing GPU- or cloud-related exists yet.

## Known issues

- **Local port collisions, both discovered by actually running things,
  not assumed:**
  - Port 8080: already bound by an unrelated Airflow instance on this
    machine (Phase 1) — `api`/`scheduler` default to 7080/7081.
  - Port 5432: already bound by an unrelated local Postgres on this
    machine (Phase 3) — `docker-compose.yml`'s `postgres` service maps
    to host port **5433** instead (container-internal traffic between
    Compose services is unaffected, since that uses `postgres:5432` over
    the Compose network). Anyone running the native (non-Docker) dev
    path needs to set `ORIONQUEUE_DATABASE_URL` to match — documented in
    README.md's Quick Start, since the package default (`localhost:5432`,
    the standard Postgres port) intentionally assumes a clean machine
    rather than baking in this one machine's workaround.
- Carried over from Phase 1/2 and unchanged: no local `golangci-lint` or
  `gcc` (`-race` doesn't run locally, does in CI); Windows SIGTERM
  delivery is best-effort for manual testing (`os.Interrupt`/Ctrl+C is
  what's actually exercised).
- `internal/persistence` shows 0% coverage under `go test ./...` — its
  logic is exercised entirely by `tests/integration` (tagged
  `integration`, requires Docker), not by the default unit-test run. This
  is intentional (see `docs/architecture/system-overview.md`'s testing
  strategy: DB-touching code belongs in integration tests, not mocked
  unit tests), not an oversight.

## Measured results

All of the following are actual outputs from this session:

- `go test ./... -cover` (no Docker required): all packages pass.
  Coverage: `internal/config` 98.1%, `internal/health` 81.6%,
  `internal/jobs` 90.5%, `internal/api` 89.9%, `internal/logging` 87.5%.
- `go test ./tests/integration/... -tags=integration` (real, disposable
  PostgreSQL via testcontainers-go): **10/10 tests passing**, ~90s total.
  Covers create/get, idempotent submission enforced at the database level
  (not just in-process), keyset-paginated + state-filtered listing,
  transactional update with automatic rollback on error, `job_events`
  rows recorded for both submission and state-change, and — the most
  direct possible test of this phase's acceptance bar —
  `JobSurvivesReconnection`, which writes through one connection pool and
  reads back through a second, independent one against the same database.
  This test suite caught one real bug during development: nil
  `Command`/`AssignedWorkerIDs` slices were sent as SQL `NULL` rather
  than `{}`, violating a `NOT NULL` constraint — fixed, then reverified.
- `scripts/migrate.sh up` / `down 2` / `version`, run directly against a
  live container (not just through the embedded test runner): applied
  both migrations, reported version `2`, rolled both back cleanly in
  reverse order, confirmed via `psql \dt` at every step.
- Full-stack manual verification via `docker compose up`: `postgres`
  reached Docker-healthy, `migrate` ran and exited 0, `api` started and
  reported both Docker-healthy and `/readyz: {"status":"ready"}`
  (a real DB ping, not a stub). Submitted a job over REST, ran
  `docker compose restart api`, and confirmed `GET /api/v1/jobs/{id}`
  still returned the same job afterward — job state survived the
  restart. Stack was torn down cleanly (`docker compose down -v`)
  afterward.

## Assumptions and environment notes

- Carried over from Phase 0–2: Windows 11 dev machine, no local GPU,
  `make`/`protoc`/`etcd`/`psql`/`golangci-lint`/`gcc` not installed
  natively (see `docs/adr/0000-local-tooling-adaptations.md`).
- Added this phase: `github.com/jackc/pgx/v5` (driver + pool),
  `github.com/golang-migrate/migrate/v4` (migrations, embedded via
  `iofs`), `github.com/testcontainers/testcontainers-go` +
  its `postgres` module (integration tests only, gated behind the
  `integration` build tag so the default `go build`/`go test ./...`
  never requires Docker or these packages at all).
- `go.mod`/`go.sum` cover both the default and `-tags=integration` build
  graphs; `go mod tidy` alone does not reliably keep tag-gated
  dependencies (this Go toolchain's `go mod tidy` has no `-tags` flag),
  so verifying `go build -tags=integration ./...` after any dependency
  change is part of this project's own working process now, not just a
  one-off fix.
- Per explicit user instruction, Claude does not run `git commit` or
  `git push` in this repo — every phase's report includes the exact
  commands for the user to run instead. Phases 0–2 are already committed
  and pushed by the user.

## Next phase

**Phase 4 — Worker registration and heartbeats**: worker/GPU/lease
schema and migrations, a `workers`-facing gRPC service (RegisterWorker,
WorkerHeartbeat), etcd-backed leases with expiration detection, and the
Python worker agent's first real registration/heartbeat loop (replacing
Phase 1's idle placeholder loop) against fake GPU inventory.
