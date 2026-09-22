package workers

import (
	"context"
	"errors"
)

// ErrNotFound is returned by Repository.Get and Repository.Update when no
// worker with the given ID exists.
var ErrNotFound = errors.New("worker not found")

// ListOptions filters and paginates Repository.List.
type ListOptions struct {
	PageSize  int32
	PageToken string
	// StatusFilter restricts results to workers in this status; the zero
	// value ("") means no filter.
	StatusFilter Status
}

// ListResult is a page of workers plus the token to fetch the next page,
// if any.
type ListResult struct {
	Workers       []Worker
	NextPageToken string
}

// Repository stores and retrieves Worker records. Phase 4 provides only
// MemoryRepository (internal/workers) and a PostgreSQL-backed
// implementation (internal/persistence) — the same two-implementation
// pattern internal/jobs established in Phase 2/3.
type Repository interface {
	// Create inserts worker and returns the stored record.
	Create(ctx context.Context, worker Worker) (Worker, error)

	// Get returns the worker with the given ID, or ErrNotFound.
	Get(ctx context.Context, id string) (Worker, error)

	// List returns a page of workers matching opts, ordered by
	// (RegisteredAt, ID) ascending.
	List(ctx context.Context, opts ListOptions) (ListResult, error)

	// Update atomically reads the worker with the given ID, passes it to
	// fn, and stores whatever fn returns — or leaves the worker unchanged
	// and returns fn's error if fn fails.
	Update(ctx context.Context, id string, fn func(Worker) (Worker, error)) (Worker, error)

	// ListActive returns every worker with Status == StatusActive
	// (including their GPU inventory), unpaginated — internal/scheduler
	// needs the complete current fleet in one call to compute placement
	// decisions, not a page of it.
	ListActive(ctx context.Context) ([]Worker, error)
}
