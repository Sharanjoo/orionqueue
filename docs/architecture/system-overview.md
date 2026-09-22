# System Overview

Status: Phase 0 design. Nothing described here is implemented yet — see
[PROJECT_STATUS.md](../../PROJECT_STATUS.md) for what's actually running.

## 1. Repository assessment

`C:\Users\shara\OrionQueue` was an empty folder (one empty `text.txt`) with no
git history before this phase. A dedicated git repository was initialized
locally (branch `master`) and `origin` was pointed at
`https://github.com/Sharanjoo/orionqueue.git`, which is a public, empty
GitHub repo (no commits, no default branch yet). There is no prior code, no
existing conventions to preserve beyond `text.txt` itself, and no other
constraint from an established layout — so the repository structure proposed
below is adopted as-is rather than adapted from something pre-existing.

Local machine capabilities relevant to the design (checked directly, not
assumed):

| Tool | Present | Version |
|---|---|---|
| Go | yes | 1.27.0 (windows/amd64) |
| Python | yes | 3.12.10 |
| Node / npm | yes | v24.14.0 / 11.9.0 |
| Docker / Compose | yes, daemon running | 29.3.1 / Compose v5.1.1 |
| Terraform | yes | 1.15.8 |
| kubectl (client) | yes | 1.34.1 |
| gh CLI | yes | 2.98.0 |
| make | **no** | — |
| protoc / buf | **no** | — |
| etcd (native binary) | **no** | — |
| psql (native client) | **no** | — |
| golangci-lint | **no** | — |
| nvidia-smi / NVIDIA GPU | **no** | — |

This directly shapes several decisions below and in
[ADR-0000](../adr/0000-local-tooling-adaptations.md): no GPU means the
fake-GPU simulator is the only path exercised locally and in CI; missing
`make`/`protoc`/`etcd`/`psql` means the toolchain leans on `go install`-able
Go binaries and Docker containers instead of host package installs, so a
clean checkout only strictly needs Go, Python, Node, and Docker.

The host also already runs an unrelated `kind` cluster (`kind-aegisops-dev`)
from a different project. OrionQueue's Kubernetes work will create and use
its own cluster (proposed name `orionqueue-dev`) rather than touch that one.

## 2. Component architecture

```
                         ┌─────────────────────┐
                         │      Frontend        │
                         │  React + TypeScript  │
                         └──────────┬───────────┘
                                    │ REST / SSE
                                    ▼
┌────────────────────────────────────────────────────────────┐
│                        Control plane (Go)                    │
│                                                                │
│  ┌──────────────┐   gRPC    ┌───────────────┐                │
│  │  REST gateway │◄────────►│  API service   │                │
│  │ (grpc-gateway)│           │  (cmd/api)     │                │
│  └──────────────┘           └───────┬───────┘                │
│                                      │                         │
│                              ┌───────▼────────┐                │
│                              │   Scheduler     │  leader        │
│                              │ (cmd/scheduler) │◄─election──┐   │
│                              └───────┬────────┘             │   │
│                                      │ gRPC AssignJob        │   │
│              ┌───────────────┬──────┴───────┬───────────┐   │   │
│              ▼               ▼              ▼           │   │   │
│  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐  │   │   │
│  │  PostgreSQL    │ │     etcd       │ │  Prometheus   │  │   │   │
│  │ durable state  │ │ leases/leader  │ │   metrics      │  │◄──┘   │
│  └───────────────┘ └───────────────┘ └───────────────┘      │
└────────────────────────────────────────────────────────────┘
                                      │ gRPC (heartbeat, assign,
                                      │       report progress)
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
           ┌────────────────┐┌────────────────┐┌────────────────┐
           │ Worker agent 1  ││ Worker agent 2  ││ Worker agent N  │
           │ (Python)        ││ (Python)        ││ (Python)        │
           │ fake-GPU or     ││ fake-GPU or     ││ fake-GPU or     │
           │ NVML/CUDA GPUs  ││ NVML/CUDA GPUs  ││ NVML/CUDA GPUs  │
           └────────────────┘└────────────────┘└────────────────┘
```

### Control plane (Go)

- `cmd/api` — the API service: hosts the gRPC server and the REST gateway
  (via `grpc-gateway`, generated from the same protobuf definitions so REST
  and gRPC never drift) in one process for the portfolio scope. Handles job
  submission/lookup/cancel/retry, worker/cluster read endpoints, `/healthz`,
  `/readyz`.
- `cmd/scheduler` — the scheduling loop. Runs leader election over etcd so
  only one active scheduler makes assignment decisions at a time even when
  multiple instances are running (see [ADR-0002](../adr/0002-scheduler-design.md)).
- `cmd/worker` — a thin Go-side worker *registrar/test-double* used only for
  Go-level integration tests that don't need the full Python agent; the real
  worker plane is the Python agent below.
- `internal/*` — implementation packages shared by the above binaries
  (`api`, `scheduler`, `workers`, `jobs`, `leases`, `persistence`, `metrics`,
  `config`), not importable outside this module.

### Worker plane (Python)

- `worker/agent` — the long-running process: registers with the control
  plane, sends heartbeats/lease renewals, receives assignments, reports
  progress/completion/failure.
- `worker/gpu` — GPU discovery. Two backends behind one interface: NVML for
  real hardware, and a deterministic fake-GPU backend used whenever no GPU is
  detected or `ORIONQUEUE_FAKE_GPU=1` is set. See
  [ADR-0003](../adr/0003-fake-vs-real-gpu.md).
- `worker/executors` — job execution strategies: the fake deterministic
  workload (Phase 6/8) and the PyTorch/NCCL multi-GPU example (Phase 9,
  real-hardware only).
- `worker/checkpoints` — checkpoint write/read/verify client used by
  executors, talking to the checkpoint storage abstraction (local filesystem
  or S3-compatible / MinIO).

### Frontend (React + TypeScript)

Talks only to the REST gateway (never directly to gRPC or the database).
Overview, Jobs, Job detail, Workers, and Cluster capacity pages as specified;
polls or subscribes over SSE for live updates. No hard-coded data — every
number shown comes from a real API response, and if the backend is in
fake-GPU mode the UI labels those numbers as simulated.

### Data stores

- **PostgreSQL** — the single source of truth for durable state: jobs, job
  attempts, workers, GPUs, checkpoints, job events, scheduling decisions.
  Anything that must survive a restart lives here.
- **etcd** — short-lived coordination only: scheduler leader election and
  worker leases (TTL-based liveness). Never the source of truth for job
  content. Rationale in [ADR-0001](../adr/0001-postgresql-vs-etcd.md).

## 3. Repository structure

Adopted from the spec's recommended layout, since there's no pre-existing
structure to reconcile with:

```
orionqueue/
├── cmd/
│   ├── api/
│   ├── scheduler/
│   └── worker/                # Go test-double registrar, not the real agent
├── internal/
│   ├── api/
│   ├── scheduler/
│   ├── workers/
│   ├── jobs/
│   ├── leases/
│   ├── persistence/
│   ├── metrics/
│   └── config/
├── proto/                     # protobuf sources, compiled with buf
├── migrations/                # golang-migrate SQL migrations
├── worker/                    # Python worker agent package
│   ├── agent/
│   ├── executors/
│   ├── gpu/
│   └── checkpoints/
├── frontend/                  # React + TypeScript dashboard
├── deploy/
│   ├── docker/
│   ├── compose/
│   ├── kubernetes/
│   └── terraform/
├── scripts/                   # primary task runner (bash, see ADR-0000)
├── tests/                     # integration / e2e / load tests
├── docs/
│   ├── architecture/
│   ├── operations/
│   ├── adr/
│   └── benchmarks/
├── Dockerfile
├── docker-compose.yml
├── Makefile                   # thin wrapper around scripts/, for CI/Linux
├── README.md
├── PROJECT_STATUS.md
└── LICENSE
```

Directories are created as each phase populates them, not pre-created empty,
so an empty tree in git never implies unfinished work is "in progress."

## 4. Domain model (summary)

Full field lists are as specified by the project brief (Job, Worker, GPU,
Checkpoint, Execution attempt). They will be formalized as the Phase 2
protobuf messages and Phase 3 SQL schema — repeating the full field-by-field
list here as prose would only get out of sync with those. The state machines
that matter for design purposes:

**Job states:**
`QUEUED → SCHEDULED → RUNNING → {CHECKPOINTING} → SUCCEEDED`
with side paths to `FAILED → RETRYING → SCHEDULED` (up to the retry limit),
`CANCEL_REQUESTED → CANCELLED`, `PREEMPTED → QUEUED`, and `LOST` (worker
disappeared, terminal until requeue decision). Transitions are validated in
`internal/jobs` and enforced at the DB layer (Phase 3).

**Worker status:** `REGISTERING → ACTIVE ⇄ SUSPECT → LOST`, driven by lease
expiration, not just missed heartbeats, to avoid flapping.

## 5. API surface

gRPC services defined in `proto/`, generated with `buf` into `internal/api`
(server) and both Go and Python clients. REST is generated from the same
proto via `grpc-gateway` annotations, so the REST endpoints listed in the
brief (`/api/v1/jobs`, etc.) are a derived view, not hand-written duplicate
handlers, and OpenAPI JSON is emitted by the same codegen step. This is a
concrete, checked-in decision, not yet an ADR of its own — folded into
Phase 2's implementation notes since it's a library choice rather than an
architectural trade-off.

## 6. Testing strategy

- **Unit** — colocated with the code: `internal/**/*_test.go`
  (table-driven, no external services) and `worker/**/test_*.py` (pytest).
- **Integration** — `tests/integration/`: Go tests using `testcontainers-go`
  for a disposable PostgreSQL/etcd where practical, exercising API→DB,
  scheduler→worker, heartbeat, and recovery flows.
- **End-to-end** — `tests/e2e/`: drives the full Docker Compose stack via the
  `scripts/` demo commands and asserts on final job states and metrics.
- **Load** — `tests/load/`: concurrent submission / scheduling latency /
  queue throughput, results written under `docs/benchmarks/` only from actual
  runs.

CI (GitHub Actions, added Phase 1) runs formatting, linting, and unit tests
on every push; integration tests run against service containers in CI;
e2e/load tests run on demand (they're slower and not needed on every commit
for a portfolio project).

## 7. Deviations from the brief's recommended architecture

Tracked explicitly per the project's own rule to document deviations:

1. **Task runner is `scripts/*.sh` first, `Makefile` second**, not
   Makefile-only — `make` isn't installed on this dev machine. See
   [ADR-0000](../adr/0000-local-tooling-adaptations.md).
2. **No system `protoc` install** — `buf` (installed via `go install`) is
   used instead, since it doesn't require a separately managed `protoc`
   binary.
3. **No system `psql`** — migrations run through `golang-migrate` (a Go
   binary); ad hoc DB inspection during development uses
   `docker exec -it orionqueue-postgres psql ...` instead of a host install.
4. **`cmd/worker` (Go) is a test double, not the real worker** — the real
   worker plane is the Python `worker/` package, per the brief. The Go
   `cmd/worker` exists only so Go-side integration tests can register a
   worker without spinning up Python, and is documented as such to avoid
   confusion about there being "two worker implementations."

Everything else follows the brief's recommended architecture as given.

## 8. Risks and assumptions carried into later phases

- **No local GPU hardware.** All GPU-related numbers before Phase 9 (and
  most of Phase 9 itself) are simulated and labeled as such everywhere they
  appear — logs, metrics, dashboard, docs. Real-GPU claims require an actual
  run on hardware this project has access to; none exists yet.
- **Single-developer scope.** etcd/Postgres failure scenarios (section 7 of
  the brief) are tested via Docker Compose fault injection (stop/restart
  containers, network partition via `docker network disconnect`), not a
  chaos-engineering platform.
- **Kubernetes validation is local-cluster-only** unless the user provides
  cloud cluster access — `kind` (already installed) is the target for Phase
  12's "works in a local cluster if available" acceptance criterion.
- **AWS/Terraform (Phase 13) is written and `validate`d, never `apply`d**
  unless the user explicitly asks to run it against a real AWS account they
  control — no deployment will ever be claimed without an actual run's
  output to show for it.
- **Scope realism.** The full brief spans 14 implementation phases across a
  Go control plane, Python worker agent, React dashboard, and multi-cloud
  infra. It will be built phase by phase across multiple sessions with tests
  gating progression, not delivered as one large drop.
