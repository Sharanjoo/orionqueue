# ADR-0005: Preemption policy

Status: Accepted (Phase 0)

## Context

Higher-priority jobs may need to run immediately even when the cluster is
full of lower-priority work. Preemption must be powerful enough to make that
possible, but bounded enough that it can't be misused to silently kill
protected work or thrash the cluster.

## Decision

- Preemption is **configurable and off by default** at the cluster level
  (see [ADR-0002](0002-scheduler-design.md)); when off, jobs only wait in
  queue or aging-boosted priority, never get preempted.
- A job is only preemptible if it explicitly sets `preemptible: true` in its
  spec. Jobs without that flag are never preemption candidates, full stop —
  no priority differential overrides that protection.
- When enabled, the scheduler may preempt the **lowest-priority eligible**
  running job(s) needed to free enough resources for a strictly
  higher-priority pending job — never preempts to make room for an equal-or
  lower-priority job, and never preempts more jobs than necessary to satisfy
  the pending request.
- If the preempted job has `checkpoint_interval` configured, the scheduler
  requests a checkpoint before sending the termination signal, and the
  execution attempt is marked `PREEMPTED` (not `FAILED`) so it doesn't count
  against the job's retry limit.
- Preempted jobs are automatically **requeued** at their original priority
  (aging continues to accrue from original submission time, not reset), and
  will resume from their latest valid checkpoint if one exists when
  rescheduled.
- **Cancellation vs. preemption** are distinct code paths with distinct audit
  event types, even though both terminate a running job, because they have
  different causes (user action vs. scheduler decision) and different
  outcomes (`CANCELLED`, terminal, vs. `PREEMPTED → QUEUED`, resumable).
- Both cancellation and preemption go through the same graceful-termination
  sequence: signal → grace period → forced termination, and both are
  idempotent — a duplicate cancel/preempt request against an already
  terminal job is a no-op, not an error, and does not double-fire audit
  events or double-decrement resources.

## Consequences

- Requires the scheduler to compute "minimum set of preemptible jobs to free
  X resources" rather than a naive "kill everything lower priority," adding
  real logic that needs dedicated unit tests (minimum-set selection,
  non-preemptible protection, requeue-with-original-priority).
- Every preemption and cancellation is an auditable event
  (`job_events` table, Phase 3), which the brief requires and which also
  makes the failure/recovery dashboard view meaningful.
- Idempotency requires cancellation/preemption handlers to check current job
  state transactionally before acting, not just fire termination signals
  optimistically.
