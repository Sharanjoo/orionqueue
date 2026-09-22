# OrionQueue — Project Status

Last updated: 2026-09-22 (Phase 1)

## Current phase

**Phase 1 — Project foundation.** Complete.

The Go module, Python worker package, and React/TypeScript dashboard
scaffold all exist, run, and are covered by real unit tests. All four
services (API, scheduler, worker, frontend) build as Docker images and were
verified running together via Docker Compose. See the Phase 1 report in
conversation history for full command-by-command output; this file tracks
the current authoritative state.

## Phase tracker

| Phase | Name | Status |
|---|---|---|
| 0 | Repository inspection & technical design | Done |
| 1 | Project foundation | Done |
| 2 | Protobuf and API layer | Not started |
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
  - `cmd/api`, `cmd/scheduler` (Go): config loading, structured JSON
    logging, `/healthz`/`/readyz`, graceful shutdown. No business logic yet
    (no job/worker APIs — that's Phase 2+).
  - `worker/agent` (Python): config loading, structured JSON logging,
    startup/shutdown loop. No registration, heartbeats, or execution yet
    (Phase 4+).
  - `frontend/` (React + TypeScript, Vite): builds and serves a status
    placeholder page reading nothing from the backend yet (the real
    dashboard is Phase 11).
  - Four Docker images (`Dockerfile`, `deploy/docker/{scheduler,worker,frontend}.Dockerfile`)
    and `docker-compose.yml` wiring them together — built and run-verified
    this phase.
  - `.github/workflows/ci.yml`: format/lint/test for Go, Python, and
    frontend, plus a Docker image build check, on every push/PR to `main`.
- **Simulated:** nothing yet — the fake-GPU simulator doesn't exist until
  Phase 6/9.
- **Not yet validated:** nothing GPU- or cloud-related exists yet to be
  unvalidated; this section stays empty until Phase 9/13 introduce
  real-hardware/real-cloud code paths.

## Known issues

- **`golangci-lint` is not installed on this dev machine.** `scripts/lint.sh`
  runs `gofmt -l`, `go vet`, and every other language's linter locally, and
  skips `golangci-lint` with an explicit message rather than failing;
  `.github/workflows/ci.yml` runs it via the official GitHub Action, so it
  is enforced in CI even though it doesn't run locally yet. Installing it
  locally (`go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest`)
  is a nice-to-have, not a blocker.
- **`go test -race` doesn't run locally.** This machine has no C compiler
  (`gcc` not found), and `-race` requires cgo. `scripts/test.sh` detects
  this and runs without `-race` locally, falling back to it automatically
  if cgo becomes available; CI (Ubuntu, gcc present) always runs with
  `-race`.
- **Windows signal handling is best-effort.** Both `internal/health.Run`
  (Go) and `agent/main.py`'s signal handler register for SIGTERM, but
  reliable SIGTERM delivery is a POSIX concept — on this Windows dev
  machine only `os.Interrupt`/Ctrl+C is dependably deliverable in-process.
  Verified: `os.Interrupt`-triggered shutdown (Go, via `internal/health`
  unit tests) and Ctrl+C both work; a `timeout`-forced kill during manual
  testing did not trigger the Python agent's graceful path, consistent with
  this being a Windows platform limitation rather than a code bug. Full
  SIGTERM-path verification will happen naturally once this runs in Linux
  containers/Kubernetes (Phase 12).
- **Default port 8080 was already in use on this machine** (an unrelated
  Airflow instance from another project). `cmd/api`/`cmd/scheduler` default
  to `:7080`/`:7081` instead — confirmed free before choosing them. Both
  remain overridable via `ORIONQUEUE_HTTP_ADDR`.

## Measured results

All of the following are actual outputs from this session, not estimates:

- `go test ./... -cover`: **8 test functions across 3 packages, all
  passing.** Coverage: `internal/config` 97.1%, `internal/health` 80.6%,
  `internal/logging` 87.5%. (`cmd/api`/`cmd/scheduler` are thin `main`
  wiring with 0% coverage by design — their logic lives in the tested
  packages they call.)
- `pytest` (worker/): **16 tests passing** across config, logging, and
  entry-point error handling.
- `vitest run` (frontend/): **3 tests passing.**
- `ruff check`, `black --check`, `oxlint`, `prettier --check`, `gofmt -l`,
  `go vet`: all clean, no findings.
- `docker compose build`: all four images (`api`, `scheduler`, `worker`,
  `frontend`) built successfully.
- `docker compose up`: all four containers reached a running state; `api`
  and `scheduler` reported Docker healthcheck status `healthy`; `curl` to
  `/healthz` and `/readyz` on both returned `200`; the frontend served its
  HTML on port 8088; the worker's structured startup log appeared as
  expected. Stack was torn down cleanly with `docker compose down`.

Nothing about GPUs, scheduling, checkpointing, or cloud deployment is
measured yet — those don't exist until later phases, consistent with the
project's no-fabricated-results rule.

## Assumptions and environment notes

- Dev machine: Windows 11 with Git Bash (MINGW64), Docker Desktop (Linux
  containers) running, no NVIDIA GPU detected (`nvidia-smi` not found). All
  GPU behavior will be developed and tested through the fake-GPU simulator;
  the real-GPU/NCCL path (Phase 9) will be written but marked unvalidated
  until run on real hardware.
- Toolchain confirmed locally: Go 1.27, Python 3.12.10, Node 24.14 / npm
  11.9, Docker 29.3.1 + Compose v5.1.1, Terraform 1.15.8, kubectl 1.34.1
  (client only), gh CLI 2.98.0. Not present: `make`, `protoc`/`buf`, `etcd`,
  `psql`, `golangci-lint`, `gcc`, `nvidia-smi` — routed around per
  [ADR-0000](docs/adr/0000-local-tooling-adaptations.md).
- This machine already has an unrelated `kind` cluster (`kind-aegisops-dev`)
  active from another project; OrionQueue's Kubernetes phase will create its
  own cluster rather than reuse that one.
- Per explicit user instruction, Claude does not run `git commit` or
  `git push` in this repo — every phase's report includes the exact
  commands for the user to run instead. Phase 0 has already been committed
  and pushed by the user (`b48bc0b`, `origin/main`).
- This is a 14-phase, multi-service system. It is being built incrementally
  across sessions, each phase gated on the previous one's tests passing.

## Next phase

**Phase 2 — Protobuf and API layer**: protobuf contracts (via `buf`), gRPC
server implementation, REST gateway (`grpc-gateway`), job submission /
lookup / listing / cancel / retry, validation and error handling, OpenAPI
docs, and idempotency tests.
