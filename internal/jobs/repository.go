package jobs

import (
	"context"
	"errors"
)

// ErrNotFound is returned by Repository.Get and Repository.Update when no
// job with the given ID exists.
var ErrNotFound = errors.New("job not found")

// ListOptions filters and paginates Repository.List.
type ListOptions struct {
	PageSize  int32
	PageToken string
	// StateFilter restricts results to jobs in this state; the zero value
	// ("") means no filter.
	StateFilter State
}

// ListResult is a page of jobs plus the token to fetch the next page, if
// any.
type ListResult struct {
	Jobs          []Job
	NextPageToken string
}

// Repository stores and retrieves Job records. Phase 2 provides only
// MemoryRepository; Phase 3 adds a PostgreSQL-backed implementation of
// this same interface, so neither the API server nor its tests need to
// change when persistence is introduced — only which Repository gets
// constructed in cmd/api/main.go.
type Repository interface {
	// Create inserts job. If job.SubmissionID is non-empty and a job with
	// that SubmissionID already exists, Create returns the existing job
	// and created=false instead of inserting a duplicate — this is what
	// makes SubmitJob idempotent.
	Create(ctx context.Context, job Job) (result Job, created bool, err error)

	// Get returns the job with the given ID, or ErrNotFound.
	Get(ctx context.Context, id string) (Job, error)

	// List returns a page of jobs matching opts, ordered by (CreatedAt,
	// ID) ascending so pagination is stable even as new jobs are created.
	List(ctx context.Context, opts ListOptions) (ListResult, error)

	// Update atomically reads the job with the given ID, passes it to fn,
	// and stores whatever fn returns — or leaves the job unchanged and
	// returns fn's error if fn fails. The whole read-modify-write happens
	// under the repository's own lock (or, from Phase 3, a database
	// transaction), so concurrent callers acting on the same job can't
	// race with each other.
	Update(ctx context.Context, id string, fn func(Job) (Job, error)) (Job, error)

	// ListActive returns every job in a non-terminal state (QUEUED,
	// SCHEDULED, RUNNING, CHECKPOINTING, RETRYING, CANCEL_REQUESTED,
	// PREEMPTED), ordered by (Priority DESC, CreatedAt ASC) — the exact
	// set and order internal/scheduler needs to compute cluster
	// availability and candidate order in a single pass, unpaginated
	// (the scheduler needs the complete picture, not a page of it).
	ListActive(ctx context.Context) ([]Job, error)
}
