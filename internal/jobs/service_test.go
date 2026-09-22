package jobs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestService() (*Service, Repository) {
	repo := NewMemoryRepository()
	fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	n := 0
	svc := NewService(repo,
		WithClock(func() time.Time { return fixedNow }),
		WithIDGenerator(func() string {
			n++
			return "job-test-" + string(rune('0'+n))
		}),
	)
	return svc, repo
}

func TestServiceSubmitCreatesQueuedJob(t *testing.T) {
	svc, _ := newTestService()
	job, err := svc.Submit(context.Background(), validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if job.State != StateQueued {
		t.Errorf("State = %q, want %q", job.State, StateQueued)
	}
	if job.CurrentAttempt != 1 {
		t.Errorf("CurrentAttempt = %d, want 1", job.CurrentAttempt)
	}
	if job.ID == "" {
		t.Error("expected a non-empty job ID")
	}
	if job.CreatedAt.IsZero() {
		t.Error("expected CreatedAt to be set")
	}
}

func TestServiceSubmitRejectsInvalidInput(t *testing.T) {
	svc, _ := newTestService()
	_, err := svc.Submit(context.Background(), SubmitInput{})
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("expected *ValidationError, got %v (%T)", err, err)
	}
}

func TestServiceSubmitIsIdempotentBySubmissionID(t *testing.T) {
	svc, _ := newTestService()
	in := validSubmitInput()
	in.SubmissionID = "client-request-42"

	first, err := svc.Submit(context.Background(), in)
	if err != nil {
		t.Fatalf("first Submit returned error: %v", err)
	}

	second, err := svc.Submit(context.Background(), in)
	if err != nil {
		t.Fatalf("second Submit returned error: %v", err)
	}

	if second.ID != first.ID {
		t.Errorf("second submission got a different job ID (%q) than the first (%q); SubmitJob must be idempotent for a repeated submission_id", second.ID, first.ID)
	}

	list, err := svc.List(context.Background(), ListOptions{})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(list.Jobs) != 1 {
		t.Fatalf("expected exactly 1 job to exist after 2 submits with the same submission_id, got %d", len(list.Jobs))
	}
}

func TestServiceSubmitWithDifferentSubmissionIDsCreatesDistinctJobs(t *testing.T) {
	svc, _ := newTestService()

	in1 := validSubmitInput()
	in1.SubmissionID = "req-1"
	in2 := validSubmitInput()
	in2.SubmissionID = "req-2"

	job1, err := svc.Submit(context.Background(), in1)
	if err != nil {
		t.Fatalf("Submit 1 returned error: %v", err)
	}
	job2, err := svc.Submit(context.Background(), in2)
	if err != nil {
		t.Fatalf("Submit 2 returned error: %v", err)
	}
	if job1.ID == job2.ID {
		t.Error("expected distinct submission IDs to produce distinct jobs")
	}
}

func TestServiceGetReturnsErrNotFound(t *testing.T) {
	svc, _ := newTestService()
	_, err := svc.Get(context.Background(), "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestServiceCancelQueuedJobSucceeds(t *testing.T) {
	svc, _ := newTestService()
	job, err := svc.Submit(context.Background(), validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	cancelled, err := svc.Cancel(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}
	if cancelled.State != StateCancelled {
		t.Errorf("State = %q, want %q", cancelled.State, StateCancelled)
	}
	if cancelled.CompletedAt == nil {
		t.Error("expected CompletedAt to be set after cancellation")
	}
}

func TestServiceCancelIsIdempotent(t *testing.T) {
	svc, _ := newTestService()
	job, err := svc.Submit(context.Background(), validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	first, err := svc.Cancel(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("first Cancel returned error: %v", err)
	}
	second, err := svc.Cancel(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("second Cancel on an already-cancelled job returned an error, want idempotent success: %v", err)
	}
	if second.State != StateCancelled {
		t.Errorf("second Cancel State = %q, want %q", second.State, StateCancelled)
	}
	if !first.CompletedAt.Equal(*second.CompletedAt) {
		t.Error("expected the second (idempotent) Cancel to leave CompletedAt unchanged from the first")
	}
}

func TestServiceCancelRunningJobIsRejectedForNow(t *testing.T) {
	svc, repo := newTestService()
	job, err := svc.Submit(context.Background(), validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := repo.Update(context.Background(), job.ID, func(j Job) (Job, error) {
		j.State = StateRunning
		return j, nil
	}); err != nil {
		t.Fatalf("test setup: failed to force job into RUNNING: %v", err)
	}

	_, err = svc.Cancel(context.Background(), job.ID)
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("expected ErrInvalidState for cancelling a RUNNING job (Phase 7 territory), got %v", err)
	}
}

func TestServiceCancelMissingJobReturnsErrNotFound(t *testing.T) {
	svc, _ := newTestService()
	_, err := svc.Cancel(context.Background(), "does-not-exist")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestServiceRetryRequiresFailedState(t *testing.T) {
	svc, _ := newTestService()
	job, err := svc.Submit(context.Background(), validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	_, err = svc.Retry(context.Background(), job.ID)
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("expected ErrInvalidState when retrying a QUEUED job, got %v", err)
	}
}

func TestServiceRetrySucceedsForFailedJobUnderLimit(t *testing.T) {
	svc, repo := newTestService()
	in := validSubmitInput()
	in.RetryLimit = 3
	job, err := svc.Submit(context.Background(), in)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	if _, err := repo.Update(context.Background(), job.ID, func(j Job) (Job, error) {
		j.State = StateFailed
		j.FailureReason = "simulated failure for test"
		return j, nil
	}); err != nil {
		t.Fatalf("test setup: failed to force job into FAILED: %v", err)
	}

	retried, err := svc.Retry(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("Retry returned error: %v", err)
	}
	if retried.State != StateQueued {
		t.Errorf("State = %q, want %q", retried.State, StateQueued)
	}
	if retried.CurrentAttempt != 2 {
		t.Errorf("CurrentAttempt = %d, want 2", retried.CurrentAttempt)
	}
	if retried.FailureReason != "" {
		t.Errorf("FailureReason = %q, want empty after retry", retried.FailureReason)
	}
}

func TestServiceRetryEnforcesRetryLimit(t *testing.T) {
	svc, repo := newTestService()
	in := validSubmitInput()
	in.RetryLimit = 1
	job, err := svc.Submit(context.Background(), in)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	// CurrentAttempt starts at 1 and RetryLimit is 1, so the very first
	// retry attempt should already be rejected — no attempts remain.
	if _, err := repo.Update(context.Background(), job.ID, func(j Job) (Job, error) {
		j.State = StateFailed
		return j, nil
	}); err != nil {
		t.Fatalf("test setup: failed to force job into FAILED: %v", err)
	}

	_, err = svc.Retry(context.Background(), job.ID)
	if !errors.Is(err, ErrRetryLimitExceeded) {
		t.Fatalf("expected ErrRetryLimitExceeded, got %v", err)
	}
}

func TestServiceRetryMissingJobReturnsErrNotFound(t *testing.T) {
	svc, _ := newTestService()
	_, err := svc.Retry(context.Background(), "does-not-exist")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
