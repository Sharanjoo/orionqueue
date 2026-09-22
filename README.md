# OrionQueue — Distributed GPU Workload Orchestrator

> **Status: Phase 7 (cancellation and preemption) complete.** A RUNNING job
> can now be gracefully stopped: cancelling one delivers a stop signal to its
> worker on the next heartbeat, which interrupts its execution thread
> cooperatively and confirms back — verified live with a job interrupted at
> step 6 of 30, reaching terminal `CANCELLED` within one heartbeat interval.
> The scheduler can also preempt a lower-priority `preemptible` RUNNING job to
> free capacity for a higher-priority pending one (opt-in, off by default —
> ADR-0005) — verified live: a 2-GPU low-priority job was preempted, a
> 1-GPU high-priority job ran to completion on the freed capacity, and the
> preempted job was automatically rescheduled afterward with its retry count
> untouched. Resuming a preempted/cancelled job from a checkpoint instead of
> step 1 is Phase 8, once a checkpoint store exists. See
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

- **Control plane (Go):** gRPC services + REST gateway, a scheduling loop
  (priority + fairness, GPU/CPU/memory eligibility, gang scheduling, bin
  packing) with etcd-backed leader election, PostgreSQL for durable state,
  etcd for both worker liveness leases and scheduler leadership, Prometheus
  metrics, structured JSON logs.
- **Worker plane (Python):** gRPC client agent, GPU discovery (deterministic
  fake-GPU mode today; NVML for real hardware from Phase 9), registration and
  heartbeats against the control plane, checkpoint read/write (Phase 8).
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

Requires Go 1.27+, Python 3.11+, Node 20+, and Docker.

**Run everything via Docker Compose:**

```bash
docker compose up --build
# postgres:  durable job/worker state (Postgres healthcheck gates everything below)
# etcd:      worker liveness leases + scheduler leader election — see ADR-0001
# migrate:   applies migrations/*.sql once, then exits — api/scheduler wait for this
# api:       http://localhost:7080 (REST + health), :9080 (gRPC, reflection on)
# scheduler: http://localhost:7081/healthz, /readyz, /leader (is this replica leading?)
# frontend:  http://localhost:8088
# worker:    registers with api and heartbeats — see its logs, no HTTP endpoint

docker compose down          # add -v to also drop the postgres/etcd volumes (wipes job/worker history)
```

**Submit and manage a job (REST):**

```bash
curl -X POST http://localhost:7080/api/v1/jobs -d '{
  "name": "train-resnet", "owner": "you", "image": "orionqueue/fake-gpu-job:latest",
  "resources": {"gpu_count": 1, "cpu_cores": 2}, "priority": 50, "retry_limit": 3
}'
# -> {"job": {"id": "job-...", "state": "JOB_STATE_QUEUED", ...}}

curl http://localhost:7080/api/v1/jobs                     # list
curl http://localhost:7080/api/v1/jobs/<id>                # get
curl -X POST http://localhost:7080/api/v1/jobs/<id>/cancel -d '{}'  # QUEUED/SCHEDULED -> CANCELLED immediately; RUNNING -> CANCEL_REQUESTED, then CANCELLED once its worker confirms (see below)
curl -X POST http://localhost:7080/api/v1/jobs/<id>/retry -d '{}'   # only valid once a job has reached terminal FAILED
```

Submitting twice with the same `submission_id` field returns the original
job instead of creating a duplicate (idempotent submission).

**Watch a job actually run (Phase 6, fake-GPU executor):** in fake-GPU
mode a job's `command` doubles as the simulated executor's configuration
(there's no real image/command to run yet) — `--steps=N` (default 5),
`--step-seconds=X` (default 1.0), and `--fail-at-step=N` (default: never
fail) for reproducible failure-injection demos. See
[ADR-0003](docs/adr/0003-fake-vs-real-gpu.md).

```bash
curl -X POST http://localhost:7080/api/v1/jobs -d '{
  "name": "fake-job", "owner": "you", "image": "fake-gpu-executor",
  "command": ["--steps=3", "--step-seconds=1"],
  "resources": {"gpu_count": 1, "cpu_cores": 1}, "priority": 50, "retry_limit": 1
}'
sleep 5
curl http://localhost:7080/api/v1/jobs/<id>
# state moves JOB_STATE_QUEUED -> SCHEDULED -> RUNNING -> SUCCEEDED as the
# assigned worker's heartbeat picks it up, executes it, and reports back

# Add "--fail-at-step=2" and retry_limit>1 to see it requeue
# (current_attempt increments, failure_reason is preserved) and retry
# before eventually reaching terminal JOB_STATE_FAILED once retries are
# exhausted.
```

**View the worker fleet and its GPU inventory (REST):**

```bash
curl http://localhost:7080/api/v1/workers
# -> {"workers": [{"id": "worker-...", "hostname": "...", "status": "WORKER_STATUS_ACTIVE",
#      "gpus": [{"uuid": "GPU-fake-...", "total_memory_bytes": "...", ...}], ...}]}
```

`RegisterWorker`/`WorkerHeartbeat` are gRPC-only (no REST binding — only the
worker agent calls them); `ListWorkers` is REST-exposed. Every GPU value
comes from the deterministic fake-GPU simulator (see
[ADR-0003](docs/adr/0003-fake-vs-real-gpu.md)) unless run on real hardware.

**See automatic worker-loss detection and job recovery:** submit a
longer-running job (e.g. `--steps=30 --step-seconds=1`), let it reach
`JOB_STATE_RUNNING`, then kill its worker (`docker kill
orionqueue-worker-1`, or `docker compose stop worker` for a graceful
stop). Poll `GET /api/v1/workers` — its `status` flips from
`WORKER_STATUS_ACTIVE` to `WORKER_STATUS_LOST` on its own, once its etcd
lease expires without a renewing heartbeat (default: ~20s after the last
heartbeat, no polling or external script required). The same lease
expiry also recovers the job: poll `GET /api/v1/jobs/<id>` and watch it
flip from `JOB_STATE_RUNNING` to `JOB_STATE_QUEUED` with
`current_attempt` incremented (or straight to terminal `JOB_STATE_FAILED`
if retries were already exhausted) — the api log records
`"worker marked LOST"` immediately followed by
`"recovered jobs from lost worker"`. Restart the worker
(`docker compose up -d worker` / `docker compose start worker`) to see it
register again and the requeued job get picked back up and run to
completion.

**Cancel a RUNNING job (Phase 7):** submit a job, let it reach
`JOB_STATE_RUNNING`, then cancel it:

```bash
curl -X POST http://localhost:7080/api/v1/jobs/<id>/cancel -d '{}'
# -> state: JOB_STATE_CANCEL_REQUESTED immediately (nothing has actually
#    stopped the worker's execution yet)
sleep 6
curl http://localhost:7080/api/v1/jobs/<id>
# -> state: JOB_STATE_CANCELLED — the assigned worker picked up the stop
#    signal on its next heartbeat, interrupted the job's executor thread
#    cooperatively, and confirmed back via ReportJobStopped
```

A job that's still `QUEUED`/`SCHEDULED` (nothing executing it yet)
cancels straight to `JOB_STATE_CANCELLED` with no wait, same as before
Phase 7. Cancelling an already-terminal or already-`CANCEL_REQUESTED` job
is a no-op, not an error — safe to retry the same cancel call.

**Watch a lower-priority job get preempted (Phase 7, opt-in):**
preemption is off by default (ADR-0005) — start the stack with it enabled:

```bash
ORIONQUEUE_PREEMPTION_ENABLED=true docker compose up -d
```

Then, with a worker that has (say) 2 GPUs, submit a `preemptible` job
using all of them, let it reach `JOB_STATE_RUNNING`, then submit a
higher-priority job needing 1 GPU:

```bash
curl -X POST http://localhost:7080/api/v1/jobs -d '{
  "name": "low", "owner": "you", "image": "img", "priority": 10, "preemptible": true,
  "command": ["--steps=60", "--step-seconds=1"],
  "resources": {"gpu_count": 2, "cpu_cores": 1}
}'
sleep 8   # let it reach RUNNING and commit both GPUs
curl -X POST http://localhost:7080/api/v1/jobs -d '{
  "name": "high", "owner": "you", "image": "img", "priority": 90,
  "command": ["--steps=3", "--step-seconds=1"],
  "resources": {"gpu_count": 1, "cpu_cores": 1}
}'
sleep 15
curl http://localhost:7080/api/v1/jobs/<low-id>   # JOB_STATE_QUEUED (preempted, then rescheduled once capacity allowed)
curl http://localhost:7080/api/v1/jobs/<high-id>  # JOB_STATE_SUCCEEDED
```

The low-priority job's `current_attempt` never increments from being
preempted — only a genuine failure or worker loss counts against
`retry_limit`. It also restarts from step 1 rather than resuming where it
left off; resuming from a checkpoint is Phase 8.

**Watch a job actually get scheduled:** with a worker registered, submit a
job that fits its capacity and poll it — within one scheduling pass (default
5s) it moves from `JOB_STATE_QUEUED` to `JOB_STATE_SCHEDULED` with
`assigned_worker_ids` set to a real, eligible worker:

```bash
curl -X POST http://localhost:7080/api/v1/jobs -d '{
  "name": "fits", "owner": "you", "image": "img",
  "resources": {"gpu_count": 1, "cpu_cores": 1}, "priority": 50
}'
sleep 6
curl http://localhost:7080/api/v1/jobs/<id>   # state: JOB_STATE_SCHEDULED, assigned_worker_ids: [...]
```

A job requesting more GPUs than any current worker has stays
`JOB_STATE_QUEUED` indefinitely rather than being force-failed — a
larger worker could register later and make it schedulable (see
[ADR-0002](docs/adr/0002-scheduler-design.md)). Every successful
assignment is also recorded in the `scheduling_decisions` table. As of
Phase 6, `assigned_worker_ids` being set means the job is picked up and
actually executed on the worker's next heartbeat — see "Watch a job
actually run" above.

**Or call the real gRPC service directly**, e.g. with
[grpcurl](https://github.com/fullstorydev/grpcurl) (server reflection is on,
so no local `.proto` files are needed):

```bash
grpcurl -plaintext 127.0.0.1:9080 list orionqueue.v1.WorkerService
grpcurl -plaintext -d '{"name":"j1","owner":"you","image":"img"}' 127.0.0.1:9080 orionqueue.v1.JobService/SubmitJob
```

OpenAPI/Swagger definitions generated from the same protobuf source are at
`docs/api/orionqueue/v1/*.swagger.json`.

**Or run each service directly, without Docker.** Both `cmd/api` and
`cmd/scheduler` need a running Postgres (migrations applied) and etcd — the
easiest way is `docker compose up -d postgres migrate etcd`, then run the
rest natively:

```bash
docker compose up -d postgres migrate etcd   # or: your own Postgres + etcd + ./scripts/migrate.sh

# ORIONQUEUE_DATABASE_URL defaults to localhost:5432, the standard Postgres
# port — but this repo's docker-compose.yml maps Postgres to host port 5433
# instead, because 5432 was already taken by an unrelated Postgres on this
# project's dev machine (see docker-compose.yml's postgres service). etcd's
# default (localhost:2379) matches docker-compose.yml directly. If port 5432
# is free on yours, either edit that mapping back to "5432:5432", or override
# the port here to match whatever docker-compose.yml says:
export ORIONQUEUE_DATABASE_URL="postgres://orionqueue:orionqueue@localhost:5433/orionqueue?sslmode=disable"

./scripts/dev-api.sh         # http://localhost:7080
./scripts/dev-scheduler.sh   # http://localhost:7081 — campaigns for leadership, then schedules every 5s while leading
./scripts/dev-worker.sh      # creates worker/.venv on first run; registers against ORIONQUEUE_API_GRPC_ADDR (default localhost:9080)
./scripts/dev-frontend.sh    # installs node_modules on first run, then Vite dev server
```

(`make dev-api`, `make dev-scheduler`, etc. do the same thing — see
`docs/adr/0000-local-tooling-adaptations.md` for why scripts/ is primary and
the Makefile is a thin wrapper.)

## Testing

```bash
./scripts/fmt.sh              # gofmt, black, prettier — applies formatting
./scripts/lint.sh             # buf lint, gofmt -l, go vet, golangci-lint (if installed), ruff, black --check, oxlint, prettier --check
./scripts/test.sh             # go test, pytest, vitest — unit tests only, no Docker required
./scripts/test-integration.sh # go test ./tests/integration/... — needs Docker (spins up real Postgres/etcd)
./scripts/build.sh            # go build + frontend production build
./scripts/migrate.sh up       # apply migrations (also runs automatically inside docker compose up)
./scripts/proto-gen.sh        # regenerate internal/api/gen/ and docs/api/ from proto/*.proto (Go + OpenAPI)
./scripts/proto-gen-python.sh # regenerate worker/gen/ from proto/*.proto (Python gRPC client stubs)
```

Or `make fmt` / `make lint` / `make test` / `make test-integration` / `make build`
/ `make migrate`. See `PROJECT_STATUS.md` for the latest run's actual test
counts and coverage. End-to-end and load test suites are added in later
phases as the functionality they'd exercise (checkpointing, real execution
under load) gets built. What exists today already goes beyond bare unit
tests: `internal/api` includes real HTTP round-trips through the REST
gateway; `internal/scheduler` includes a test that runs two scheduler
instances concurrently against the same in-memory job to prove they can't
double-assign it; and `tests/integration` runs `JobRepository`/
`WorkerRepository`/scheduling-decision persistence against a real,
disposable PostgreSQL container and both `leases.EtcdManager` and
`leases.RunElection` (leader election, including mutual exclusion and
failover between two candidates) against a real, disposable etcd container
— all via testcontainers-go, none of it mocked.

## Limitations

This is a single-developer portfolio project, not a production system.
Deliberately out of scope for now (tracked so it isn't confused with
something that was simply forgotten):

- Authentication/authorization (a request-ID/logging middleware exists;
  `WithAuthPlaceholder` is a deliberate no-op today — see
  `internal/api/middleware.go`).
- Multi-tenant isolation, distributed rate limiting, quota management.
- A real secret-management backend (Docker/K8s secrets only).
- A full tracing backend (OpenTelemetry hooks will exist without a wired
  collector by default).
- Real GPU/NCCL results — only reported if actually run on real hardware.
- AWS deployment — Terraform will be written and `validate`d, but not applied
  or claimed as deployed unless it actually is.
