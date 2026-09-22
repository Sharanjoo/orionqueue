-- Jobs is the durable record of every job OrionQueue knows about. This is
-- the table that makes "job state survives an API restart" (Phase 3's
-- acceptance criterion) true: internal/persistence.JobRepository is the
-- only thing that writes here, and it's the repository cmd/api uses from
-- Phase 3 onward instead of the in-memory one.
--
-- Deliberately NOT in this migration (added by the phase that actually
-- uses them, not speculatively now): workers, gpus, leases (Phase 4),
-- scheduling_decisions (Phase 5), job_attempts (Phase 6, once a job can
-- have more than one real execution attempt), checkpoints (Phase 8). This
-- keeps each migration matched to working, tested code rather than
-- speculative schema — see docs/architecture/system-overview.md.
CREATE TABLE jobs (
    id                           TEXT PRIMARY KEY,
    submission_id                TEXT NOT NULL DEFAULT '',
    name                         TEXT NOT NULL,
    owner                        TEXT NOT NULL,
    image                        TEXT NOT NULL,
    command                      TEXT[] NOT NULL DEFAULT '{}',

    gpu_count                    INTEGER NOT NULL DEFAULT 0,
    min_gpu_memory_bytes         BIGINT NOT NULL DEFAULT 0,
    cpu_cores                    DOUBLE PRECISION NOT NULL DEFAULT 0,
    memory_bytes                 BIGINT NOT NULL DEFAULT 0,

    priority                     INTEGER NOT NULL DEFAULT 0,
    preemptible                  BOOLEAN NOT NULL DEFAULT FALSE,

    retry_limit                  INTEGER NOT NULL DEFAULT 0,
    current_attempt              INTEGER NOT NULL DEFAULT 1,

    timeout_seconds              BIGINT NOT NULL DEFAULT 0,
    checkpoint_interval_seconds  BIGINT NOT NULL DEFAULT 0,

    state                        TEXT NOT NULL,
    assigned_worker_ids          TEXT[] NOT NULL DEFAULT '{}',
    failure_reason               TEXT NOT NULL DEFAULT '',

    -- All timestamps are UTC (TIMESTAMPTZ, always written/read as UTC by
    -- internal/persistence — never the server's local time zone).
    created_at                   TIMESTAMPTZ NOT NULL,
    started_at                   TIMESTAMPTZ,
    completed_at                 TIMESTAMPTZ,
    failed_at                    TIMESTAMPTZ,

    -- Mirrors jobs.State in internal/jobs/job.go; a CHECK constraint here
    -- means an invalid state can never land in the database even if a
    -- future bug in application code tried to write one.
    CONSTRAINT jobs_state_valid CHECK (state IN (
        'QUEUED', 'SCHEDULED', 'RUNNING', 'CHECKPOINTING', 'SUCCEEDED',
        'FAILED', 'RETRYING', 'CANCEL_REQUESTED', 'CANCELLED', 'PREEMPTED',
        'LOST'
    ))
);

-- Enforces SubmitJob's idempotency contract (the same guarantee
-- internal/jobs.MemoryRepository provides in-process) at the database
-- level, so it holds even across multiple API replicas — a real
-- improvement over the in-memory repository, not just a port of it.
-- Partial (WHERE submission_id <> '') so jobs submitted without an
-- idempotency key, which is most of them, never collide with each other.
CREATE UNIQUE INDEX jobs_submission_id_unique_idx
    ON jobs (submission_id) WHERE submission_id <> '';

-- Supports ListJobs' state_filter.
CREATE INDEX jobs_state_idx ON jobs (state);

-- Supports ListJobs' stable pagination order (created_at, id).
CREATE INDEX jobs_created_at_id_idx ON jobs (created_at, id);
