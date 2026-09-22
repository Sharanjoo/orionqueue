-- scheduling_decisions is the audit trail of what the scheduler actually
-- decided, distinct from job_events' generic state-transition log: it
-- records which workers were chosen for a job and why, giving the
-- scheduling algorithm (internal/scheduler) a durable history separate
-- from the job's own lifecycle.
--
-- Recorded only for successful assignments (decision = 'ASSIGNED') —
-- every scheduling pass that *doesn't* place a job (no eligible worker
-- yet) is not logged here, since that would be an unbounded number of
-- "considered but skipped" rows for every pass over every still-queued
-- job. A job simply staying QUEUED is already visible via its own state.
CREATE TABLE scheduling_decisions (
    id          BIGSERIAL PRIMARY KEY,
    job_id      TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_ids  TEXT[] NOT NULL,
    decision    TEXT NOT NULL,
    reason      TEXT NOT NULL DEFAULT '',
    decided_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT scheduling_decisions_decision_valid CHECK (decision IN ('ASSIGNED'))
);

CREATE INDEX scheduling_decisions_job_id_idx ON scheduling_decisions (job_id);
CREATE INDEX scheduling_decisions_decided_at_idx ON scheduling_decisions (decided_at);
