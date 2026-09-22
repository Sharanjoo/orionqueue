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
// client safely retry a cancel request. A job already CANCEL_REQUESTED is
// likewise a no-op (a duplicate cancel call while the worker hasn't
// confirmed yet), not an error.
//
// A RUNNING (or CHECKPOINTING) job transitions to CANCEL_REQUESTED, not
// directly to CANCELLED: something is actually executing it on a worker,
// and jumping straight to CANCELLED here — before that worker has
// actually stopped — would let it keep running unobserved (and risk a
// stale ReportJobCompleted/Failed racing in afterward). CANCEL_REQUESTED
// is delivered to the worker via WorkerHeartbeatResponse.stop_job_ids
// (see ListStoppableForWorker); the worker signals its execution thread
// to stop and calls ReportJobStopped once it has, which is what actually
// reaches CANCELLED (see ReportStopped). See ADR-0005 for why
// cancellation and preemption share this same stop-and-confirm mechanism.
func (s *Service) Cancel(ctx context.Context, id string) (Job, error) {
	return s.repo.Update(ctx, id, func(job Job) (Job, error) {
		switch {
		case job.State.Terminal(), job.State == StateCancelRequested:
			return job, nil
		case job.State == StateQueued || job.State == StateRetrying || job.State == StateScheduled:
			job.State = StateCancelled
			now := s.now().UTC()
			job.CompletedAt = &now
			return job, nil
		case job.State == StateRunning || job.State == StateCheckpointing:
			job.State = StateCancelRequested
			return job, nil
		default:
			return Job{}, fmt.Errorf("%w: cannot cancel a job in state %s", ErrInvalidState, job.State)
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

// ListStoppableForWorker returns every job assigned to workerID that's
// CANCEL_REQUESTED or PREEMPTED — the set a worker agent should
// cooperatively stop, delivered via WorkerHeartbeatResponse.stop_job_ids.
// Mirrors ListAssignedToWorker's ListActive-scan approach and the same
// documented trade-off.
func (s *Service) ListStoppableForWorker(ctx context.Context, workerID string) ([]Job, error) {
	active, err := s.repo.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	var stoppable []Job
	for _, job := range active {
		if (job.State == StateCancelRequested || job.State == StatePreempted) &&
			containsString(job.AssignedWorkerIDs, workerID) {
			stoppable = append(stoppable, job)
		}
	}
	return stoppable, nil
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

// ReportStopped is called when a worker reports (ReportJobStopped) that it
// has cooperatively stopped a job's execution in response to a stop
// signal (see ListStoppableForWorker). What happens next depends on why
// the job was stopped, read from the job's own current state rather than
// anything the worker tells us — the worker doesn't need to know or care
// which:
//
//   - CANCEL_REQUESTED -> CANCELLED (terminal): a user asked for this.
//   - PREEMPTED -> QUEUED (CurrentAttempt unchanged, unassigned): the
//     scheduler asked for this to make room for a higher-priority job;
//     per ADR-0005 this must not count against the job's retry limit, so
//     — unlike Fail/LoseWorker's applyFailureOrRetry — CurrentAttempt is
//     deliberately left untouched here.
//   - Any other state (job already moved on some other way — e.g. a
//     duplicate report, or LoseWorker already resolved it) is a no-op,
//     for the same idempotency reasons Cancel and Complete already rely
//     on.
func (s *Service) ReportStopped(ctx context.Context, id string) (Job, error) {
	return s.repo.Update(ctx, id, func(job Job) (Job, error) {
		switch job.State {
		case StateCancelRequested:
			job.State = StateCancelled
			now := s.now().UTC()
			job.CompletedAt = &now
			return job, nil
		case StatePreempted:
			job.State = StateQueued
			job.StartedAt = nil
			job.AssignedWorkerIDs = nil
			return job, nil
		default:
			return job, nil
		}
	})
}

// Preempt transitions a RUNNING job to PREEMPTED, so the scheduler
// (internal/scheduler.Service.RunOnce, when preemption is cluster-enabled
// — see ADR-0005) can free a lower-priority job's resources for a
// strictly higher-priority pending one. Only a job that opted in via
// Preemptible=true may be preempted — Preempt refuses (ErrInvalidState)
// otherwise, the same protection SubmitJob's Preemptible field exists
// for.
//
// Like Cancel, this doesn't jump straight to QUEUED: PREEMPTED is a
// holding state until the worker actually stops execution and confirms
// via ReportStopped (which is what performs the PREEMPTED -> QUEUED
// requeue) — see ReportStopped and ListStoppableForWorker.
// CurrentAttempt is untouched (preemption never counts against
// RetryLimit).
func (s *Service) Preempt(ctx context.Context, id string, reason string) (Job, error) {
	return s.repo.Update(ctx, id, func(job Job) (Job, error) {
		if job.State != StateRunning {
			return Job{}, fmt.Errorf("%w: Preempt requires state RUNNING, job is %s", ErrInvalidState, job.State)
		}
		if !job.Preemptible {
			return Job{}, fmt.Errorf("%w: job %s is not preemptible", ErrInvalidState, job.ID)
		}
		job.State = StatePreempted
		job.FailureReason = reason
		return job, nil
	})
}

// LoseWorker finds every job assigned to workerID that's still
// SCHEDULED, RUNNING, CANCEL_REQUESTED, or PREEMPTED and recovers it, so
// a crashed worker's in-flight jobs don't sit stuck forever. Called when
// internal/workers detects (via its own etcd-lease-expiry watcher) that
// workerID has been marked LOST — this is what connects Phase 4's
// automatic worker-loss detection to job execution.
//
// SCHEDULED/RUNNING jobs get the same requeue-or-fail decision Fail makes
// for a worker-reported failure (applyFailureOrRetry). CANCEL_REQUESTED
// and PREEMPTED jobs no longer need the worker's confirmation to move
// on — a LOST worker can't possibly still be executing anything, so it's
// safe to finalize immediately exactly as ReportStopped would have:
// CANCEL_REQUESTED -> CANCELLED, PREEMPTED -> QUEUED. Returns how many
// jobs it recovered, for logging.
func (s *Service) LoseWorker(ctx context.Context, workerID string) (int, error) {
	active, err := s.repo.ListActive(ctx)
	if err != nil {
		return 0, fmt.Errorf("jobs: list active jobs for lost-worker recovery: %w", err)
	}

	reason := fmt.Sprintf("worker %s was lost", workerID)
	recovered := 0
	for _, job := range active {
		switch job.State {
		case StateScheduled, StateRunning, StateCancelRequested, StatePreempted:
		default:
			continue
		}
		if !containsString(job.AssignedWorkerIDs, workerID) {
			continue
		}

		_, err := s.repo.Update(ctx, job.ID, func(j Job) (Job, error) {
			// Re-check inside the lock: another caller may have already
			// moved this job on (e.g. it completed a split second before
			// the loss was detected).
			switch j.State {
			case StateScheduled, StateRunning:
				return applyFailureOrRetry(j, reason, s.now().UTC()), nil
			case StateCancelRequested:
				j.State = StateCancelled
				now := s.now().UTC()
				j.CompletedAt = &now
				return j, nil
			case StatePreempted:
				j.State = StateQueued
				j.StartedAt = nil
				j.AssignedWorkerIDs = nil
				return j, nil
			default:
				return j, nil
			}
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
