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
// Phase 2 scope: a QUEUED or RETRYING job (nothing is executing it yet)
// transitions directly to CANCELLED. A job already in a terminal state
// (SUCCEEDED, FAILED, CANCELLED) is a no-op that returns the job
// unchanged — this is what makes Cancel idempotent, and what lets a
// client safely retry a cancel request. Cancelling a RUNNING job (which
// requires a graceful-termination signal to a worker) is implemented in
// Phase 7, once the scheduler/worker execution loop exists for a job to
// actually be running on.
func (s *Service) Cancel(ctx context.Context, id string) (Job, error) {
	return s.repo.Update(ctx, id, func(job Job) (Job, error) {
		switch {
		case job.State.Terminal():
			return job, nil
		case job.State == StateQueued || job.State == StateRetrying:
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
