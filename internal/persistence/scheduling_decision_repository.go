package persistence

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sharanjoo/orionqueue/internal/scheduler"
)

// SchedulingDecisionRepository is a PostgreSQL-backed
// scheduler.DecisionRepository — see migrations/0004_create_scheduling_decisions.up.sql.
type SchedulingDecisionRepository struct {
	pool *pgxpool.Pool
}

// NewSchedulingDecisionRepository returns a SchedulingDecisionRepository
// backed by pool. Migrations must already be applied.
func NewSchedulingDecisionRepository(pool *pgxpool.Pool) *SchedulingDecisionRepository {
	return &SchedulingDecisionRepository{pool: pool}
}

// compile-time check: SchedulingDecisionRepository must satisfy
// scheduler.DecisionRepository.
var _ scheduler.DecisionRepository = (*SchedulingDecisionRepository)(nil)

func (r *SchedulingDecisionRepository) Record(ctx context.Context, jobID, workerID, reason string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO scheduling_decisions (job_id, worker_ids, decision, reason) VALUES ($1, $2, 'ASSIGNED', $3)`,
		jobID, []string{workerID}, reason,
	)
	if err != nil {
		return fmt.Errorf("persistence: record scheduling decision: %w", err)
	}
	return nil
}
