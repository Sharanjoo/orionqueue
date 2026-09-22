-- Workers is the durable record of every worker OrionQueue has ever
-- registered. Liveness itself is judged by etcd lease expiration (see
-- internal/leases and ADR-0001), not by this table alone — but every
-- status transition (ACTIVE on register/heartbeat, LOST when the
-- control plane observes the worker's etcd lease expire) is written
-- here, which is what makes "workers appear in the database" and
-- "GPU inventory is visible" (Phase 4's acceptance criteria) true.
CREATE TABLE workers (
    id                     TEXT PRIMARY KEY,
    hostname               TEXT NOT NULL,
    status                 TEXT NOT NULL,

    -- etcd lease backing this worker's liveness key
    -- (/orionqueue/workers/leases/<id>). NULL once the worker is LOST —
    -- the lease that key was bound to has already expired in etcd by
    -- then, so there's nothing left to reference.
    lease_id               BIGINT,

    cpu_capacity           DOUBLE PRECISION NOT NULL DEFAULT 0,
    memory_capacity_bytes  BIGINT NOT NULL DEFAULT 0,
    software_version       TEXT NOT NULL DEFAULT '',
    labels                 JSONB NOT NULL DEFAULT '{}',
    running_job_ids        TEXT[] NOT NULL DEFAULT '{}',

    registered_at          TIMESTAMPTZ NOT NULL,
    last_heartbeat_at      TIMESTAMPTZ NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL,

    -- Mirrors workers.Status in internal/workers/worker.go. Phase 4 only
    -- ever produces ACTIVE and LOST; a SUSPECT state (heartbeat running
    -- late but the etcd lease hasn't expired yet) is a documented
    -- possible refinement for a later phase, not implemented now — see
    -- docs/architecture/system-overview.md.
    CONSTRAINT workers_status_valid CHECK (status IN ('ACTIVE', 'LOST'))
);

CREATE INDEX workers_status_idx ON workers (status);

-- GPU inventory reported at registration (and refreshed on heartbeat).
-- One row per physical (or, from fake-GPU mode, simulated) device.
CREATE TABLE gpus (
    id                      TEXT PRIMARY KEY,
    worker_id               TEXT NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
    device_index             INTEGER NOT NULL,
    uuid                     TEXT NOT NULL,
    total_memory_bytes       BIGINT NOT NULL,
    allocated_memory_bytes   BIGINT NOT NULL DEFAULT 0,
    utilization_percent      DOUBLE PRECISION NOT NULL DEFAULT 0,
    temperature_celsius      DOUBLE PRECISION,
    mig_profile              TEXT NOT NULL DEFAULT '',
    health_state             TEXT NOT NULL DEFAULT 'HEALTHY',

    CONSTRAINT gpus_device_index_unique_per_worker UNIQUE (worker_id, device_index),
    CONSTRAINT gpus_health_state_valid CHECK (health_state IN ('HEALTHY', 'DEGRADED', 'UNHEALTHY'))
);

CREATE INDEX gpus_worker_id_idx ON gpus (worker_id);
