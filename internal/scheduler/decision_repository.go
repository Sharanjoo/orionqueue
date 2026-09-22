package scheduler

import (
	"context"
	"sync"
)

// DecisionRepository records scheduling decisions for audit/history,
// distinct from jobs.Repository — see migrations/0004_create_scheduling_decisions.up.sql.
// Only successful assignments are recorded (see Record's doc comment).
type DecisionRepository interface {
	// Record persists that jobID was assigned to workerID, for reason.
	Record(ctx context.Context, jobID, workerID, reason string) error
}

// MemoryDecisionRepository is an in-memory DecisionRepository, safe for
// concurrent use — used by internal/scheduler's own tests and by
// internal/api's fast, Docker-free tests. cmd/scheduler uses the
// PostgreSQL-backed implementation in internal/persistence instead.
type MemoryDecisionRepository struct {
	mu        sync.Mutex
	Decisions []RecordedDecision
}

// RecordedDecision is one row MemoryDecisionRepository has recorded.
type RecordedDecision struct {
	JobID    string
	WorkerID string
	Reason   string
}

// NewMemoryDecisionRepository returns an empty MemoryDecisionRepository.
func NewMemoryDecisionRepository() *MemoryDecisionRepository {
	return &MemoryDecisionRepository{}
}

func (r *MemoryDecisionRepository) Record(_ context.Context, jobID, workerID, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Decisions = append(r.Decisions, RecordedDecision{JobID: jobID, WorkerID: workerID, Reason: reason})
	return nil
}

// Snapshot returns a copy of the decisions recorded so far, safe to read
// concurrently with further Record calls.
func (r *MemoryDecisionRepository) Snapshot() []RecordedDecision {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RecordedDecision(nil), r.Decisions...)
}
