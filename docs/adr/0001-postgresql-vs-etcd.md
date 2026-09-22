# ADR-0001: PostgreSQL vs. etcd responsibilities

Status: Accepted (Phase 0)

## Context

The system needs two different kinds of coordination state:

1. Durable, queryable, relational records — jobs, job attempts, workers, GPU
   inventory, checkpoints, job events, scheduling decisions — that must
   survive restarts, support complex queries (dashboard filters, pagination,
   joins across jobs/attempts/checkpoints), and participate in transactions.
2. Short-lived liveness/coordination primitives — "is there exactly one
   active scheduler right now," "is this worker's lease still valid" — that
   need strong consistency and TTL expiry, but never need SQL-style queries
   or long-term retention.

Using a single store for both is possible (e.g. everything in Postgres with
polling-based leader election, or everything in etcd as a KV store) but each
choice is a bad fit for the other half of the problem: Postgres has no native
lease/TTL/watch primitive suited to sub-second leader election; etcd has no
relational model, joins, or the query flexibility the dashboard needs and its
recommended dataset size is much smaller than a growing job history table.

## Decision

Split responsibilities:

- **PostgreSQL is the single source of truth for durable state.** Jobs, job
  attempts, workers, GPU inventory, checkpoints, job events, and scheduling
  decisions are all Postgres tables (Phase 3). Any state that must survive an
  API or scheduler restart lives here, full stop.
- **etcd is used only for short-lived coordination:**
  - Scheduler leader election (`internal/scheduler` uses etcd's
    `concurrency.Election`), so exactly one scheduler instance makes
    assignment decisions even when multiple replicas run for availability.
  - Worker leases: a worker holds a short-TTL etcd lease, renewed on every
    heartbeat; lease expiry is what marks a worker `LOST`, not merely a
    missed heartbeat counter, to avoid flapping on transient delays.
  - etcd never stores job content or anything that needs to survive being
    wiped — if etcd data is lost, the system loses only in-flight leader
    election / lease state, which self-heals (a new leader is elected, lost
    workers are re-detected), not durable job history.

## Consequences

- Two stateful dependencies to run in Docker Compose / Kubernetes instead of
  one, which adds operational surface for a single-developer project — judged
  worth it because it mirrors how real orchestrators (Kubernetes itself:
  etcd for cluster state + no separate durable store; SLURM: separate DB for
  accounting) separate these concerns, and it's a deliberate demonstration
  point for this portfolio project.
- Failure-mode tests (section 7 of the brief) must cover etcd-unavailable and
  Postgres-unavailable independently, since they fail differently: etcd down
  blocks new scheduling decisions and lease renewal but existing job data is
  untouched; Postgres down blocks state persistence but doesn't affect who
  currently holds scheduler leadership.
- Local dev needs both containers up (`docker-compose.yml`), covered by
  Phase 1/12 tooling.
