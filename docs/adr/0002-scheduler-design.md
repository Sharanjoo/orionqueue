# ADR-0002: Scheduler design

Status: Accepted (Phase 0)

## Context

The scheduler must turn a queue of jobs with heterogeneous GPU/CPU/memory
requirements, priorities, and gang-scheduling needs (multi-GPU jobs) into
assignments onto a fluctuating pool of workers, while:

- staying correct if run as multiple replicas (only one may act at a time),
- not starving low-priority jobs indefinitely,
- never assigning to a worker whose lease has expired,
- making multi-GPU assignments atomic (all-or-nothing across the set of GPUs
  a job needs, not partially assigned).

## Decision

**Algorithm (deterministic, single active scheduler per election term):**

1. Candidate jobs are ordered by `(priority DESC, submitted_at ASC)` — strict
   priority with FIFO tie-break within a priority band. This is simple to
   reason about and to unit test exhaustively, which matters for a project
   meant to demonstrate correct scheduling logic.
2. For each job, filter workers to those that are `ACTIVE` with a
   non-expired etcd lease, satisfy label/capability constraints, and have
   enough *free* CPU, system memory, and per-GPU memory.
3. Among eligible workers, prefer the tightest fit (bin-packing / least
   fragmentation) over spreading load — deliberately chosen over spread
   scheduling as the default because bin-packing keeps more whole workers
   free for large multi-GPU jobs, which this system is specifically meant to
   exercise. Spread-scheduling behavior is documented as an alternative
   policy, not implemented as a second live mode in the initial phases.
4. **Gang scheduling:** a multi-GPU job's assignment is computed and
   committed as a single scheduling decision (one row in
   `scheduling_decisions`, one transaction) across all GPUs it needs, either
   on one worker or a coordinated set — the job is never partially assigned.
   If a full gang can't be placed, the job stays `QUEUED` and is
   reconsidered next scheduling pass, it does not hold partial resources.
5. **Fairness:** strict priority ordering alone can starve low-priority jobs
   forever under sustained high-priority load. A bounded fairness mechanism
   (aging: a job's effective priority increases the longer it waits in
   `QUEUED`) prevents indefinite starvation without abandoning priority
   semantics. Exact aging curve is a tunable, tested with unit tests that
   assert a low-priority job eventually schedules under continuous
   high-priority pressure within a bounded number of scheduling passes.
6. **Preemption (configurable, off by default):** when enabled and no
   eligible worker exists for a high-priority job, the scheduler may preempt
   the lowest-priority *preemptible* running job(s) that free enough
   resources, request a checkpoint if configured, and requeue the preempted
   job. Non-preemptible jobs are never candidates. See
   [ADR-0005](0005-preemption-policy.md) for the full policy.
7. Jobs that can never fit the cluster's total capacity (not just current
   free capacity) are rejected at submission time with a clear error, rather
   than sitting in `QUEUED` forever.

**Leader election and duplicate-decision safety:** the scheduler binary can
run as multiple replicas for availability, but only the etcd election winner
runs the scheduling loop. Every scheduling decision is written to Postgres
inside a transaction that also updates the job's state and (for the assigned
worker) reserves its resources, guarded by optimistic concurrency (a version
column) so that even a brief split-brain window (e.g. an old leader finishing
an in-flight pass just as a new one is elected) cannot double-assign the same
job — the second writer's transaction fails the version check and re-reads
current state.

## Alternatives considered

- **Fair-share scheduling as the primary policy** — more complex to implement
  and test correctly than priority+aging for a single-developer scope;
  documented as a possible extension, not built as the default.
- **Spread scheduling as default** — better for isolating noisy neighbors,
  worse for packing multi-GPU jobs, which is the more interesting scheduling
  problem this project wants to demonstrate. Bin-packing chosen as default;
  spread is documented as an alternative.
- **Always-on preemption** — rejected as the default because it makes
  scheduling behavior harder to reason about and test; preemption is
  implemented but configurable and off by default.

## Consequences

- Scheduling logic is unit-testable in isolation (no network/DB needed for
  the core filtering/ranking/gang-assignment algorithm) — the actual
  Postgres/etcd writes are a thin layer on top, tested separately in
  integration tests.
- Aging-based fairness and gang scheduling add real complexity; both get
  dedicated unit test suites per the brief's testing requirements (Phase 5).
