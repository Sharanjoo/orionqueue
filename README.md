# OrionQueue — Distributed GPU Workload Orchestrator

> **Status: Phase 1 (project foundation) complete.** The API, scheduler,
> worker agent, and frontend all start, are unit-tested, and run together via
> Docker Compose — but there is no job/worker business logic yet (no gRPC/REST
> API surface, no scheduling, no GPU simulation). See
> [PROJECT_STATUS.md](PROJECT_STATUS.md) for the live phase tracker and
> [docs/architecture/system-overview.md](docs/architecture/system-overview.md)
> for the full design.

## What this is

OrionQueue is a simplified distributed GPU workload orchestrator — a
scaled-down Kubernetes/SLURM/Ray hybrid built as a portfolio project to
demonstrate distributed coordination, scheduling, concurrency, fault
tolerance, GPU computing, observability, and cloud infrastructure.

It lets a user submit GPU jobs (single- or multi-GPU) with resource
requirements, priority, retries, and checkpoint settings; schedules them onto
a pool of workers; tracks worker health via heartbeats and leases; recovers
and retries jobs when workers fail; supports cancellation and priority
preemption; and exposes the whole thing through REST/gRPC APIs, Prometheus
metrics, and a React dashboard.

The system runs entirely locally with a deterministic **fake-GPU simulator**
(no NVIDIA hardware required), and is designed to also run with real NVIDIA
GPUs and deploy to Kubernetes / AWS — but real-GPU and cloud results are only
ever reported here if they were actually run and measured on this machine or
account. See [ADR-0003](docs/adr/0003-fake-vs-real-gpu.md) for how the two
modes relate.

## Architecture at a glance

- **Control plane (Go):** gRPC services + REST gateway, PostgreSQL for durable
  state, etcd for scheduler leader election and short leases, Prometheus
  metrics, structured JSON logs.
- **Worker plane (Python):** gRPC client agent, GPU discovery (NVML when
  available, deterministic fake-GPU mode otherwise), heartbeats, checkpoint
  read/write.
- **Frontend (React + TypeScript):** jobs, workers, cluster capacity, and
  failure/recovery views backed by the real API.
- **Infra:** Docker Compose for local dev, Kubernetes manifests for cluster
  deployment, optional Terraform for AWS.

Full component breakdown, data flow, and the repository layout are in
[docs/architecture/system-overview.md](docs/architecture/system-overview.md).

## Architecture decisions

Design decisions and their trade-offs are recorded as ADRs in
[docs/adr/](docs/adr/):

- [0000 — Local tooling adaptations](docs/adr/0000-local-tooling-adaptations.md)
- [0001 — PostgreSQL vs. etcd responsibilities](docs/adr/0001-postgresql-vs-etcd.md)
- [0002 — Scheduler design](docs/adr/0002-scheduler-design.md)
- [0003 — Fake GPU vs. real GPU execution](docs/adr/0003-fake-vs-real-gpu.md)
- [0004 — Checkpoint storage abstraction](docs/adr/0004-checkpoint-storage-abstraction.md)
- [0005 — Preemption policy](docs/adr/0005-preemption-policy.md)
- [0006 — Kubernetes deployment strategy](docs/adr/0006-kubernetes-deployment-strategy.md)

## Quick start

What exists today is process-foundation only — config, structured logging,
health endpoints, and (for the frontend) a status placeholder page. There is
no job submission API yet (that's Phase 2). Requires Go 1.27+, Python 3.11+,
Node 20+, and Docker.

**Run everything via Docker Compose:**

```bash
docker compose up --build
# api:       http://localhost:7080/healthz, /readyz
# scheduler: http://localhost:7081/healthz, /readyz
# frontend:  http://localhost:8088
# worker:    logs only (no HTTP endpoint yet)

docker compose down
```

**Or run each service directly, without Docker:**

```bash
./scripts/dev-api.sh         # http://localhost:7080
./scripts/dev-scheduler.sh   # http://localhost:7081
./scripts/dev-worker.sh      # creates worker/.venv on first run
./scripts/dev-frontend.sh    # installs node_modules on first run, then Vite dev server
```

(`make dev-api`, `make dev-scheduler`, etc. do the same thing — see
`docs/adr/0000-local-tooling-adaptations.md` for why scripts/ is primary and
the Makefile is a thin wrapper.)

## Testing

```bash
./scripts/fmt.sh    # gofmt, black, prettier — applies formatting
./scripts/lint.sh   # gofmt -l, go vet, golangci-lint (if installed), ruff, black --check, oxlint, prettier --check
./scripts/test.sh   # go test, pytest, vitest — all current unit tests
./scripts/build.sh  # go build + frontend production build
```

Or `make fmt` / `make lint` / `make test` / `make build`. Current suite: 8 Go
test functions (3 packages), 16 pytest tests, 3 Vitest tests — all passing;
see `PROJECT_STATUS.md` for the latest run's actual numbers and coverage.
Integration, end-to-end, and load test suites are added in later phases as
the functionality they'd exercise (persistence, scheduling, checkpointing)
gets built.

## Limitations

This is a single-developer portfolio project, not a production system.
Deliberately out of scope for now (tracked so it isn't confused with
something that was simply forgotten):

- Authentication/authorization (middleware will be present but disabled
  locally — see ADR-0000).
- Multi-tenant isolation, distributed rate limiting, quota management.
- A real secret-management backend (Docker/K8s secrets only).
- A full tracing backend (OpenTelemetry hooks will exist without a wired
  collector by default).
- Real GPU/NCCL results — only reported if actually run on real hardware.
- AWS deployment — Terraform will be written and `validate`d, but not applied
  or claimed as deployed unless it actually is.
