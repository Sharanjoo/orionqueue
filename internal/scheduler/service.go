package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

// Service runs scheduling passes: fetch the current cluster state, plan
// assignments (Plan), and apply them. It knows nothing about etcd leader
// election or process lifecycle — cmd/scheduler wraps this with the
// leader-election loop (internal/leases.RunElection) so only the elected
// leader's Service ever calls RunOnce at a time.
//
// Safety against a brief split-brain window during a leadership
// transition doesn't rely on leader election alone: RunOnce applies each
// assignment through jobs.Service.AssignToWorkers, which only succeeds if
// the job is still QUEUED at the moment of a row-locked update. If a
// departing and an incoming leader both attempt to assign the same job,
// the loser simply gets ErrInvalidState back (counted as Result.Conflicted,
// not an error) rather than corrupting the assignment — see ADR-0002 and
// jobs.Service.AssignToWorkers' own doc comment.
type Service struct {
	jobs      *jobs.Service
	workers   *workers.Service
	decisions DecisionRepository
	now       func() time.Time
}

// NewService returns a Service.
func NewService(jobSvc *jobs.Service, workerSvc *workers.Service, decisions DecisionRepository) *Service {
	return &Service{jobs: jobSvc, workers: workerSvc, decisions: decisions, now: time.Now}
}

// Result summarizes one RunOnce call.
type Result struct {
	// Considered is how many QUEUED jobs existed at the start of this pass.
	Considered int
	// Assigned is how many were successfully placed this pass.
	Assigned int
	// Conflicted is how many assignments Plan proposed but
	// AssignToWorkers rejected because the job's state had already
	// changed by the time the write was attempted (a losing race against
	// another scheduler instance, or a client cancelling the job
	// mid-pass) — not treated as an error.
	Conflicted int
}

// RunOnce performs a single scheduling pass: fetch active jobs/workers,
// compute a Plan, and apply every assignment it produces.
func (s *Service) RunOnce(ctx context.Context) (Result, error) {
	activeJobs, err := s.jobs.ListActive(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("scheduler: list active jobs: %w", err)
	}
	activeWorkers, err := s.workers.ListActive(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("scheduler: list active workers: %w", err)
	}

	var result Result
	for _, j := range activeJobs {
		if j.State == jobs.StateQueued {
			result.Considered++
		}
	}

	for _, a := range Plan(activeJobs, activeWorkers, s.now()) {
		if _, err := s.jobs.AssignToWorkers(ctx, a.JobID, []string{a.WorkerID}); err != nil {
			if errors.Is(err, jobs.ErrInvalidState) || errors.Is(err, jobs.ErrNotFound) {
				result.Conflicted++
				continue
			}
			return result, fmt.Errorf("scheduler: assign job %s to %s: %w", a.JobID, a.WorkerID, err)
		}
		result.Assigned++

		if err := s.decisions.Record(ctx, a.JobID, a.WorkerID, a.Reason); err != nil {
			// The assignment itself already committed correctly; failing
			// to record its audit-trail row is worth surfacing to the
			// caller (cmd/scheduler logs it) but must not be treated as
			// though the assignment itself failed.
			return result, fmt.Errorf("scheduler: record decision for job %s: %w", a.JobID, err)
		}
	}
	return result, nil
}
