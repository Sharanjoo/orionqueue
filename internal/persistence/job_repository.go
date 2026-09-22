package persistence

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sharanjoo/orionqueue/internal/jobs"
)

const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// jobColumnList is the single source of truth for column order, shared by
// every query in this file, so a column can never silently end up in a
// different position in a SELECT than in its corresponding Scan call.
const jobColumnList = `id, submission_id, name, owner, image, command,
	gpu_count, min_gpu_memory_bytes, cpu_cores, memory_bytes,
	priority, preemptible, retry_limit, current_attempt,
	timeout_seconds, checkpoint_interval_seconds,
	state, assigned_worker_ids, failure_reason,
	created_at, started_at, completed_at, failed_at`

var (
	insertJobSQL = fmt.Sprintf(`
INSERT INTO jobs (%s)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
ON CONFLICT (submission_id) WHERE submission_id <> '' DO NOTHING
RETURNING %s`, jobColumnList, jobColumnList)

	selectJobByIDSQL = fmt.Sprintf(`SELECT %s FROM jobs WHERE id = $1`, jobColumnList)

	selectJobBySubmissionIDSQL = fmt.Sprintf(`SELECT %s FROM jobs WHERE submission_id = $1`, jobColumnList)

	selectJobByIDForUpdateSQL = fmt.Sprintf(`SELECT %s FROM jobs WHERE id = $1 FOR UPDATE`, jobColumnList)

	// updateJobSQL intentionally never touches submission_id — it's
	// immutable after creation, so it isn't in the SET list even though
	// it's part of jobColumnList for reads.
	updateJobSQL = fmt.Sprintf(`
UPDATE jobs SET
	name=$2, owner=$3, image=$4, command=$5,
	gpu_count=$6, min_gpu_memory_bytes=$7, cpu_cores=$8, memory_bytes=$9,
	priority=$10, preemptible=$11, retry_limit=$12, current_attempt=$13,
	timeout_seconds=$14, checkpoint_interval_seconds=$15,
	state=$16, assigned_worker_ids=$17, failure_reason=$18,
	created_at=$19, started_at=$20, completed_at=$21, failed_at=$22
WHERE id=$1
RETURNING %s`, jobColumnList)
)

// JobRepository is a PostgreSQL-backed implementation of jobs.Repository.
// It's a drop-in replacement for jobs.MemoryRepository: cmd/api
// constructs this one starting Phase 3, and nothing in internal/jobs or
// internal/api needed to change to make that possible — that decoupling
// was the point of defining jobs.Repository as an interface in Phase 2.
//
// Two things it does that MemoryRepository couldn't: enforce SubmitJob's
// idempotency contract at the database level (via a partial unique index
// on submission_id), so it holds across concurrent API replicas, not just
// within one process; and record every submission and state transition
// to job_events in the same transaction as the change that caused it.
type JobRepository struct {
	pool *pgxpool.Pool
}

// NewJobRepository returns a JobRepository backed by pool. Migrations
// must already be applied (see RunMigrations / scripts/migrate.sh) —
// this constructor does not create or alter schema.
func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

func (r *JobRepository) Create(ctx context.Context, job jobs.Job) (jobs.Job, bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("persistence: begin create tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once committed

	created, err := scanJob(tx.QueryRow(ctx, insertJobSQL, jobInsertArgs(job)...))
	switch {
	case err == nil:
		if err := insertJobEvent(ctx, tx, created.ID, "SUBMITTED", nil, string(created.State), ""); err != nil {
			return jobs.Job{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return jobs.Job{}, false, fmt.Errorf("persistence: commit create: %w", err)
		}
		return created, true, nil

	case errors.Is(err, pgx.ErrNoRows):
		// ON CONFLICT ... DO NOTHING matched: a job with this
		// submission_id already exists. There's nothing to commit;
		// return the existing job instead, per SubmitJob's idempotency
		// contract.
		if job.SubmissionID == "" {
			return jobs.Job{}, false, fmt.Errorf("persistence: unexpected insert conflict with an empty submission_id")
		}
		existing, getErr := r.getBySubmissionID(ctx, job.SubmissionID)
		if getErr != nil {
			return jobs.Job{}, false, getErr
		}
		return existing, false, nil

	default:
		return jobs.Job{}, false, fmt.Errorf("persistence: insert job: %w", err)
	}
}

func (r *JobRepository) Get(ctx context.Context, id string) (jobs.Job, error) {
	job, err := scanJob(r.pool.QueryRow(ctx, selectJobByIDSQL, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.Job{}, jobs.ErrNotFound
		}
		return jobs.Job{}, fmt.Errorf("persistence: get job: %w", err)
	}
	return job, nil
}

func (r *JobRepository) getBySubmissionID(ctx context.Context, submissionID string) (jobs.Job, error) {
	job, err := scanJob(r.pool.QueryRow(ctx, selectJobBySubmissionIDSQL, submissionID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.Job{}, jobs.ErrNotFound
		}
		return jobs.Job{}, fmt.Errorf("persistence: get job by submission_id: %w", err)
	}
	return job, nil
}

func (r *JobRepository) List(ctx context.Context, opts jobs.ListOptions) (jobs.ListResult, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 || pageSize > maxPageSize {
		pageSize = defaultPageSize
	}

	var (
		conditions []string
		args       = []any{pageSize + 1} // fetch one extra row to detect a next page
	)
	nextArg := func() string { return fmt.Sprintf("$%d", len(args)) }

	if opts.StateFilter != "" {
		args = append(args, string(opts.StateFilter))
		conditions = append(conditions, "state = "+nextArg())
	}
	if opts.PageToken != "" {
		createdAt, id, err := decodeListToken(opts.PageToken)
		if err != nil {
			return jobs.ListResult{}, fmt.Errorf("invalid page_token: %w", err)
		}
		args = append(args, createdAt)
		createdArg := nextArg()
		args = append(args, id)
		idArg := nextArg()
		conditions = append(conditions, fmt.Sprintf("(created_at, id) > (%s, %s)", createdArg, idArg))
	}

	query := fmt.Sprintf(`SELECT %s FROM jobs`, jobColumnList)
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY created_at, id LIMIT $1"

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return jobs.ListResult{}, fmt.Errorf("persistence: list jobs: %w", err)
	}
	defer rows.Close()

	var page []jobs.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return jobs.ListResult{}, fmt.Errorf("persistence: scan listed job: %w", err)
		}
		page = append(page, job)
	}
	if err := rows.Err(); err != nil {
		return jobs.ListResult{}, fmt.Errorf("persistence: iterate listed jobs: %w", err)
	}

	result := jobs.ListResult{}
	if len(page) > int(pageSize) {
		last := page[pageSize-1]
		result.NextPageToken = encodeListToken(last.CreatedAt, last.ID)
		page = page[:pageSize]
	}
	result.Jobs = page
	return result, nil
}

func (r *JobRepository) Update(ctx context.Context, id string, fn func(jobs.Job) (jobs.Job, error)) (jobs.Job, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return jobs.Job{}, fmt.Errorf("persistence: begin update tx: %w", err)
	}
	defer tx.Rollback(ctx)

	current, err := scanJob(tx.QueryRow(ctx, selectJobByIDForUpdateSQL, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.Job{}, jobs.ErrNotFound
		}
		return jobs.Job{}, fmt.Errorf("persistence: select job for update: %w", err)
	}

	updated, err := fn(current)
	if err != nil {
		return jobs.Job{}, err
	}

	stored, err := scanJob(tx.QueryRow(ctx, updateJobSQL, jobUpdateArgs(updated)...))
	if err != nil {
		return jobs.Job{}, fmt.Errorf("persistence: update job: %w", err)
	}

	if current.State != stored.State {
		fromState := string(current.State)
		if err := insertJobEvent(ctx, tx, stored.ID, "STATE_CHANGED", &fromState, string(stored.State), ""); err != nil {
			return jobs.Job{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, fmt.Errorf("persistence: commit update: %w", err)
	}
	return stored, nil
}

func insertJobEvent(ctx context.Context, tx pgx.Tx, jobID, eventType string, fromState *string, toState, detail string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO job_events (job_id, event_type, from_state, to_state, detail) VALUES ($1,$2,$3,$4,$5)`,
		jobID, eventType, fromState, toState, detail,
	)
	if err != nil {
		return fmt.Errorf("persistence: insert job event: %w", err)
	}
	return nil
}

// pgxRow is satisfied by both pgx.Row (QueryRow) and pgx.Rows (Query,
// iterated with Next), so scanJob works for both single-row and
// multi-row queries without duplicating the column mapping.
type pgxRow interface {
	Scan(dest ...any) error
}

func scanJob(row pgxRow) (jobs.Job, error) {
	var (
		j        jobs.Job
		state    string
		gpuCount int32
	)
	err := row.Scan(
		&j.ID, &j.SubmissionID, &j.Name, &j.Owner, &j.Image, &j.Command,
		&gpuCount, &j.Resources.MinGPUMemoryBytes, &j.Resources.CPUCores, &j.Resources.MemoryBytes,
		&j.Priority, &j.Preemptible, &j.RetryLimit, &j.CurrentAttempt,
		&j.TimeoutSeconds, &j.CheckpointIntervalSeconds,
		&state, &j.AssignedWorkerIDs, &j.FailureReason,
		&j.CreatedAt, &j.StartedAt, &j.CompletedAt, &j.FailedAt,
	)
	if err != nil {
		return jobs.Job{}, err
	}
	j.State = jobs.State(state)
	j.Resources.GPUCount = uint32(gpuCount)
	j.CreatedAt = j.CreatedAt.UTC()
	if j.StartedAt != nil {
		t := j.StartedAt.UTC()
		j.StartedAt = &t
	}
	if j.CompletedAt != nil {
		t := j.CompletedAt.UTC()
		j.CompletedAt = &t
	}
	if j.FailedAt != nil {
		t := j.FailedAt.UTC()
		j.FailedAt = &t
	}
	return j, nil
}

func jobInsertArgs(j jobs.Job) []any {
	return []any{
		j.ID, j.SubmissionID, j.Name, j.Owner, j.Image, nonNilStrings(j.Command),
		int32(j.Resources.GPUCount), j.Resources.MinGPUMemoryBytes, j.Resources.CPUCores, j.Resources.MemoryBytes,
		j.Priority, j.Preemptible, j.RetryLimit, j.CurrentAttempt,
		j.TimeoutSeconds, j.CheckpointIntervalSeconds,
		string(j.State), nonNilStrings(j.AssignedWorkerIDs), j.FailureReason,
		j.CreatedAt, j.StartedAt, j.CompletedAt, j.FailedAt,
	}
}

func jobUpdateArgs(j jobs.Job) []any {
	return []any{
		j.ID, j.Name, j.Owner, j.Image, nonNilStrings(j.Command),
		int32(j.Resources.GPUCount), j.Resources.MinGPUMemoryBytes, j.Resources.CPUCores, j.Resources.MemoryBytes,
		j.Priority, j.Preemptible, j.RetryLimit, j.CurrentAttempt,
		j.TimeoutSeconds, j.CheckpointIntervalSeconds,
		string(j.State), nonNilStrings(j.AssignedWorkerIDs), j.FailureReason,
		j.CreatedAt, j.StartedAt, j.CompletedAt, j.FailedAt,
	}
}

// nonNilStrings converts a nil slice to an empty (non-nil) one. jobs.Job
// fields like Command and AssignedWorkerIDs are frequently nil (e.g. no
// command override, no worker assigned yet) rather than an explicit empty
// slice, but the jobs/assigned_worker_ids and jobs/command columns are
// NOT NULL — a Postgres column DEFAULT only fills in a value that's
// omitted from the INSERT entirely, not one explicitly bound to NULL, so
// without this the driver would send NULL for a nil Go slice and violate
// the constraint.
func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// encodeListToken/decodeListToken implement keyset pagination (WHERE
// (created_at, id) > (token_created_at, token_id)) rather than OFFSET,
// which stays efficient and stable even as the jobs table grows — an
// improvement over the OFFSET-based scheme jobs.MemoryRepository uses,
// made possible because this is now an internal detail of JobRepository,
// not something callers depend on the format of.
func encodeListToken(t time.Time, id string) string {
	raw := t.UTC().Format(time.RFC3339Nano) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeListToken(token string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}, "", err
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", fmt.Errorf("malformed page_token")
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("malformed page_token timestamp: %w", err)
	}
	return t, parts[1], nil
}
