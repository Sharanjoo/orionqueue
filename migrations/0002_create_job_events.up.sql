-- job_events is the audit/event-history trail for jobs: one row per
-- submission and per state transition. internal/persistence.JobRepository
-- writes these as part of the same transaction as the jobs table change
-- that caused them, so the two can never disagree.
CREATE TABLE job_events (
    id          BIGSERIAL PRIMARY KEY,
    job_id      TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event_type  TEXT NOT NULL,
    from_state  TEXT,
    to_state    TEXT,
    detail      TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT job_events_type_valid CHECK (event_type IN ('SUBMITTED', 'STATE_CHANGED'))
);

-- Supports "get this job's event history in order," the natural access
-- pattern (e.g. a future GET /api/v1/jobs/{id}/events endpoint, listed in
-- the project brief's REST surface but not yet implemented — this table
-- is what it will read from).
CREATE INDEX job_events_job_id_created_at_idx ON job_events (job_id, created_at);
