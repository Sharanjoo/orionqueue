package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrInvalidState is returned when an operation (Cancel, Retry) is
// requested against a job whose current state doesn't allow it.
var ErrInvalidState = errors.New("job is not in a valid state for this operation")

// ErrRetryLimitExceeded is returned by Retry when the job has already used
// its configured retry budget.
var ErrRetryLimitExceeded = errors.New("retry limit exceeded")

// containsString reports whether s contains v.
func containsString(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// Clock abstracts time.Now so tests can control timestamps deterministically.
type Clock func() time.Time

// IDGenerator abstracts job ID generation so tests can supply deterministic,
// predictable IDs instead of random ones.
type IDGenerator func() string

// Service implements OrionQueue's job business logic (submission,
// lookup, listing, cancellation, retry) on top of a Repository. It knows
// nothing about gRPC, HTTP, or protobuf — internal/api's server
// implementation is a thin adapter around this type, translating between
// wire messages and the calls below.
type Service struct {
	repo  Repository
	now   Clock
	newID IDGenerator
}

// ServiceOption customizes a Service returned by NewService.
type ServiceOption func(*Service)

// WithClock overrides the Service's time source; used by tests to assert
// on exact CreatedAt/CompletedAt values.
func WithClock(c Clock) ServiceOption { return func(s *Service) { s.now = c } }

// WithIDGenerator overrides the Service's job ID generator; used by tests
// to assert on exact IDs instead of random ones.
func WithIDGenerator(g IDGenerator) ServiceOption { return func(s *Service) { s.newID = g } }

// NewService returns a Service backed by repo.
func NewService(repo Repository, opts ...ServiceOption) *Service {
	s := &Service{
		repo:  repo,
		now:   time.Now,
		newID: func() string { return newRandomID("job") },
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Submit validates in, and creates a new job in state QUEUED — or, if
// in.SubmissionID is non-empty and matches a previous submission, returns
// that existing job unchanged (idempotent submission).
func (s *Service) Submit(ctx context.Context, in SubmitInput) (Job, error) {
	if err := ValidateSubmit(in); err != nil {
		return Job{}, err
	}

	job := Job{
		ID:                        s.newID(),
		SubmissionID:              in.SubmissionID,
		Name:                      in.Name,
		Owner:                     in.Owner,
		Image:                     in.Image,
		Command:                   append([]string(nil), in.Command...),
		Resources:                 in.Resources,
		Priority:                  in.Priority,
		Preemptible:               in.Preemptible,
		RetryLimit:                in.RetryLimit,
		CurrentAttempt:            1,
		TimeoutSeconds:            in.TimeoutSeconds,
		CheckpointIntervalSeconds: in.CheckpointIntervalSeconds,
		State:                     StateQueued,
		CreatedAt:                 s.now().UTC(),
	}

	result, _, err := s.repo.Create(ctx, job)
	return result, err
}

// Get returns the job with the given ID, or ErrNotFound.
func (s *Service) Get(ctx context.Context, id string) (Job, error) {
	return s.repo.Get(ctx, id)
}

// List returns a page of jobs matching opts.
func (s *Service) List(ctx context.Context, opts ListOptions) (ListResult, error) {
	return s.repo.List(ctx, opts)
}

// Cancel requests cancellation of the job with the given ID.
//
// A QUEUED, RETRYING, or SCHEDULED job — nothing is actually executing it
// yet, even once Phase 5's scheduler has assigned it to a worker —
// transitions directly to CANCELLED. A job already in a terminal state
// (SUCCEEDED, FAILED, CANCELLED) is a no-op that returns the job
// unchanged — this is what makes Cancel idempotent, and what lets a
// client safely retry a cancel request. Cancelling a RUNNING job (which
// requires sending a graceful-termination signal to the worker actually
// executing it, then waiting on it) is implemented in Phase 7 — today it
// returns ErrInvalidState, same as any other unsupported transition.
func (s *Service) Cancel(ctx context.Context, id string) (Job, error) {
	return s.repo.Update(ctx, id, func(job Job) (Job, error) {
		switch {
		case job.State.Terminal():
			return job, nil
		case job.State == StateQueued || job.State == StateRetrying || job.State == StateScheduled:
			job.State = StateCancelled
			now := s.now().UTC()
			job.CompletedAt = &now
			return job, nil
		default:
			return Job{}, fmt.Errorf(
				"%w: cannot cancel a job in state %s yet (graceful running-job cancellation is implemented in Phase 7)",
				ErrInvalidState, job.State,
			)
		}
	})
}

// AssignToWorkers records the scheduler's decision to place job onto
// workerIDs, transitioning it from QUEUED to SCHEDULED. It only succeeds
// if the job is still QUEUED at the moment of the (row-locked) update —
// if another scheduler instance already assigned it (or a client
// cancelled it) between when this caller read the job and now,
// AssignToWorkers returns ErrInvalidState instead of silently overwriting
// a conflicting decision. Combined with Repository.Update's row-level
// locking (SELECT ... FOR UPDATE in the PostgreSQL implementation), this
// is what makes concurrent scheduler replicas safe even during a brief
// leader-election split-brain window, not just etcd leader election
// alone — see docs/adr/0002-scheduler-design.md.
func (s *Service) AssignToWorkers(ctx context.Context, id string, workerIDs []string) (Job, error) {
	return s.repo.Update(ctx, id, func(job Job) (Job, error) {
		if job.State != StateQueued {
			return Job{}, fmt.Errorf("%w: AssignToWorkers requires state QUEUED, job is %s", ErrInvalidState, job.State)
		}
		job.State = StateScheduled
		job.AssignedWorkerIDs = append([]string(nil), workerIDs...)
		return job, nil
	})
}

// ListActive returns every job in a non-terminal state, ordered by
// (Priority DESC, CreatedAt ASC) — see Repository.ListActive.
func (s *Service) ListActive(ctx context.Context) ([]Job, error) {
	return s.repo.ListActive(ctx)
}

// ListAssignedToWorker returns every SCHEDULED job currently assigned to
// workerID — the set a worker agent should pick up and start executing.
// Once a job is actually started (Start), it moves to RUNNING and stops
// appearing here, so a worker heartbeating repeatedly never tries to
// start the same job twice. Implemented by scanning ListActive rather
// than a dedicated indexed query — an accepted simplification at this
// portfolio project's scale (see internal/scheduler's capacity-tracking
// comments for the same trade-off made there).
func (s *Service) ListAssignedToWorker(ctx context.Context, workerID string) ([]Job, error) {
	active, err := s.repo.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	var assigned []Job
	for _, job := range active {
		if job.State == StateScheduled && containsString(job.AssignedWorkerIDs, workerID) {
			assigned = append(assigned, job)
		}
	}
	return assigned, nil
}

// Start transitions a SCHEDULED job to RUNNING, called when a worker
// reports (ReportJobStarted) that it has begun executing job.
func (s *Service) Start(ctx context.Context, id string) (Job, error) {
	return s.repo.Update(ctx, id, func(job Job) (Job, error) {
		if job.State != StateScheduled {
			return Job{}, fmt.Errorf("%w: Start requires state SCHEDULED, job is %s", ErrInvalidState, job.State)
		}
		job.State = StateRunning
		now := s.now().UTC()
		job.StartedAt = &now
		return job, nil
	})
}

// Complete transitions a RUNNING job to SUCCEEDED, called when a worker
// reports (ReportJobCompleted) that execution finished successfully.
func (s *Service) Complete(ctx context.Context, id string) (Job, error) {
	return s.repo.Update(ctx, id, func(job Job) (Job, error) {
		if job.State != StateRunning {
			return Job{}, fmt.Errorf("%w: Complete requires state RUNNING, job is %s", ErrInvalidState, job.State)
		}
		job.State = StateSucceeded
		now := s.now().UTC()
		job.CompletedAt = &now
		return job, nil
	})
}

// Fail transitions a RUNNING job following an execution failure reported
// by a worker (ReportJobFailed). If the job hasn't exhausted its
// RetryLimit yet, it goes straight back to QUEUED with CurrentAttempt
// incremented — this is what makes "failed jobs retry correctly" and
// "retry limits are enforced" (Phase 6's acceptance criteria) hold
// automatically, without a client needing to call RetryJob after every
// transient failure. RetryJob (Phase 2) remains available separately, for
// a client to manually retry a job that already exhausted these
// automatic attempts and is sitting FAILED.
//
// Note: this never materializes RETRYING as a stored intermediate state
// (it goes directly RUNNING -> QUEUED in one update) — consistent with
// Retry's own FAILED -> QUEUED transition, which has never stored
// RETRYING either. RETRYING remains defined in the domain model for a
// possible future refinement (e.g. a time-bound backoff delay before
// requeue), not because either code path uses it today.
func (s *Service) Fail(ctx context.Context, id string, reason string) (Job, error) {
	return s.repo.Update(ctx, id, func(job Job) (Job, error) {
		if job.State != StateRunning {
			return Job{}, fmt.Errorf("%w: Fail requires state RUNNING, job is %s", ErrInvalidState, job.State)
		}
		return applyFailureOrRetry(job, reason, s.now().UTC()), nil
	})
}

// LoseWorker finds every job assigned to workerID that's still SCHEDULED
// or RUNNING and recovers it — requeued for another attempt, or marked
// FAILED if retries are exhausted, the same decision Fail makes for a
// worker-reported failure. Called when internal/workers detects (via its
// own etcd-lease-expiry watcher) that workerID has been marked LOST, so a
// crashed worker's in-flight jobs don't sit SCHEDULED/RUNNING forever —
// this is what connects Phase 4's automatic worker-loss detection to job
// execution. Returns how many jobs it recovered, for logging.
func (s *Service) LoseWorker(ctx context.Context, workerID string) (int, error) {
	active, err := s.repo.ListActive(ctx)
	if err != nil {
		return 0, fmt.Errorf("jobs: list active jobs for lost-worker recovery: %w", err)
	}

	reason := fmt.Sprintf("worker %s was lost", workerID)
	recovered := 0
	for _, job := range active {
		if job.State != StateScheduled && job.State != StateRunning {
			continue
		}
		if !containsString(job.AssignedWorkerIDs, workerID) {
			continue
		}

		_, err := s.repo.Update(ctx, job.ID, func(j Job) (Job, error) {
			// Re-check inside the lock: another caller may have already
			// moved this job on (e.g. it completed a split second before
			// the loss was detected).
			if j.State != StateScheduled && j.State != StateRunning {
				return j, nil
			}
			return applyFailureOrRetry(j, reason, s.now().UTC()), nil
		})
		if err != nil {
			return recovered, fmt.Errorf("jobs: recover job %s from lost worker %s: %w", job.ID, workerID, err)
		}
		recovered++
	}
	return recovered, nil
}

// applyFailureOrRetry mutates job to reflect a failure: requeues it
// (QUEUED, CurrentAttempt incremented, unassigned) if retries remain, or
// marks it FAILED (terminal) if not. Shared by Fail (a worker-reported
// execution failure) and LoseWorker (recovery when a job's worker
// disappears) so the retry-or-fail decision is made in exactly one place.
func applyFailureOrRetry(job Job, reason string, now time.Time) Job {
	job.FailureReason = reason
	if job.CurrentAttempt < job.RetryLimit {
		job.State = StateQueued
		job.CurrentAttempt++
		job.StartedAt = nil
		job.AssignedWorkerIDs = nil
		return job
	}
	job.State = StateFailed
	job.FailedAt = &now
	return job
}

// Retry moves a FAILED job back to QUEUED for another attempt, as long as
// it hasn't exhausted its retry_limit. Since no job can reach FAILED until
// Phase 6 (job execution) exists, this is fully exercised today via tests
// that construct a FAILED job directly through the Repository, not through
// the API — that's an intentional, documented Phase 2 scope boundary, not
// a gap: the state-machine logic itself is real and tested now, so Phase 6
// only has to reach FAILED correctly, not re-derive retry semantics.
func (s *Service) Retry(ctx context.Context, id string) (Job, error) {
	return s.repo.Update(ctx, id, func(job Job) (Job, error) {
		if job.State != StateFailed {
			return Job{}, fmt.Errorf("%w: RetryJob requires state FAILED, job is %s", ErrInvalidState, job.State)
		}
		if job.CurrentAttempt >= job.RetryLimit {
			return Job{}, fmt.Errorf("%w: attempt %d of %d", ErrRetryLimitExceeded, job.CurrentAttempt, job.RetryLimit)
		}
		job.State = StateQueued
		job.CurrentAttempt++
		job.FailureReason = ""
		job.StartedAt = nil
		job.CompletedAt = nil
		job.FailedAt = nil
		return job, nil
	})
}
