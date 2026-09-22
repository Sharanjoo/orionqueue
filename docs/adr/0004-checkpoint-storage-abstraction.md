# ADR-0004: Checkpoint storage abstraction

Status: Accepted (Phase 0)

## Context

Checkpoints need a storage backend for local development (no external
dependencies) and a path toward something closer to production (S3 or
S3-compatible object storage), plus metadata (job/attempt, storage URI, size,
checksum, completion status) that must be queryable — "what's the latest
valid checkpoint for this job" is a relational question, not a
blob-storage one.

## Decision

- Define a `CheckpointStore` interface (`worker/checkpoints`) with two
  implementations: a **local filesystem backend** (default for `docker
  compose` dev, writes under a bind-mounted volume) and an **S3-compatible
  backend** (works against real S3 or MinIO via the same S3 API, MinIO
  offered as an optional Compose service so the S3 path is exercisable
  without an AWS account).
- Checkpoint **content** lives in whichever store is configured; checkpoint
  **metadata** (checkpoint ID, job ID, attempt, storage URI, created
  timestamp, size, checksum, completion status) always lives in PostgreSQL,
  consistent with ADR-0001 — Postgres is the source of truth for "which
  checkpoints exist and are they valid," object storage just holds bytes.
- **Checksum validation** (e.g. SHA-256) is computed on write and re-verified
  on read before a job is allowed to resume from a checkpoint; a checksum
  mismatch marks that checkpoint invalid and recovery falls back to the next
  most recent valid one (or a clean restart if none exists).
- **Latest-valid-checkpoint lookup** is a single indexed query:
  `checkpoints WHERE job_id = ? AND completion_status = 'valid' ORDER BY
  created_at DESC LIMIT 1`.
- **Cleanup policy:** keep the N most recent valid checkpoints per job
  (configurable, default small N) plus the one currently referenced by an
  in-progress recovery; older ones are deleted from both the object store and
  the metadata table in the same cleanup pass to avoid orphaned rows or
  orphaned blobs.

## Consequences

- Local dev never requires AWS credentials or a real S3 bucket — MinIO in
  Compose is enough to exercise the exact same code path as production S3.
- The interface boundary means adding another backend later (e.g. GCS) is a
  new implementation of `CheckpointStore`, not a scheduler/worker change.
- Recovery-time tests can inject a corrupted or missing checkpoint and assert
  the system falls back correctly, which is one of the explicit test
  scenarios in the brief (Phase 8).
