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

func TestServiceAssignToWorkersTransitionsQueuedToScheduled(t *testing.T) {
	svc, _ := newTestService()
	job, err := svc.Submit(context.Background(), validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	assigned, err := svc.AssignToWorkers(context.Background(), job.ID, []string{"worker-1", "worker-2"})
	if err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}
	if assigned.State != StateScheduled {
		t.Errorf("State = %q, want %q", assigned.State, StateScheduled)
	}
	if len(assigned.AssignedWorkerIDs) != 2 || assigned.AssignedWorkerIDs[0] != "worker-1" {
		t.Errorf("AssignedWorkerIDs = %v, want [worker-1 worker-2]", assigned.AssignedWorkerIDs)
	}
}

func TestServiceAssignToWorkersRejectsNonQueuedJob(t *testing.T) {
	svc, _ := newTestService()
	job, err := svc.Submit(context.Background(), validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(context.Background(), job.ID, []string{"worker-1"}); err != nil {
		t.Fatalf("first AssignToWorkers returned error: %v", err)
	}

	// A second assignment attempt on the now-SCHEDULED job — simulating a
	// second scheduler instance racing on the same job — must be rejected,
	// not silently overwrite the first decision.
	_, err = svc.AssignToWorkers(context.Background(), job.ID, []string{"worker-2"})
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("expected ErrInvalidState for a duplicate assignment, got %v", err)
	}
}

func TestServiceAssignToWorkersMissingJobReturnsErrNotFound(t *testing.T) {
	svc, _ := newTestService()
	_, err := svc.AssignToWorkers(context.Background(), "does-not-exist", []string{"worker-1"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestServiceCancelScheduledJobSucceeds(t *testing.T) {
	svc, _ := newTestService()
	job, err := svc.Submit(context.Background(), validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(context.Background(), job.ID, []string{"worker-1"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}

	cancelled, err := svc.Cancel(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}
	if cancelled.State != StateCancelled {
		t.Errorf("State = %q, want %q", cancelled.State, StateCancelled)
	}
}

func TestServiceListActiveExcludesTerminalJobs(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()

	queued, err := svc.Submit(ctx, validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	cancelled, err := svc.Submit(ctx, validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.Cancel(ctx, cancelled.ID); err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}

	active, err := svc.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive returned error: %v", err)
	}
	if len(active) != 1 || active[0].ID != queued.ID {
		t.Fatalf("expected only the still-QUEUED job, got %+v", active)
	}
}

func TestServiceListActiveOrdersByPriorityThenCreatedAt(t *testing.T) {
	repo := NewMemoryRepository()
	svc := NewService(repo)
	ctx := context.Background()

	base := validSubmitInput()
	low := base
	low.Priority = 10
	high := base
	high.Priority = 90
	mid := base
	mid.Priority = 50

	lowJob, err := svc.Submit(ctx, low)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	highJob, err := svc.Submit(ctx, high)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	midJob, err := svc.Submit(ctx, mid)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	active, err := svc.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive returned error: %v", err)
	}
	if len(active) != 3 {
		t.Fatalf("expected 3 active jobs, got %d", len(active))
	}
	want := []string{highJob.ID, midJob.ID, lowJob.ID}
	for i, id := range want {
		if active[i].ID != id {
			t.Errorf("position %d: got job %q (priority %d), want %q", i, active[i].ID, active[i].Priority, id)
		}
	}
}

func TestServiceListAssignedToWorkerReturnsOnlyScheduledJobsForThatWorker(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()

	forA, err := svc.Submit(ctx, validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(ctx, forA.ID, []string{"worker-a"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}

	forB, err := svc.Submit(ctx, validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(ctx, forB.ID, []string{"worker-b"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}

	stillQueued, err := svc.Submit(ctx, validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	_ = stillQueued

	assigned, err := svc.ListAssignedToWorker(ctx, "worker-a")
	if err != nil {
		t.Fatalf("ListAssignedToWorker returned error: %v", err)
	}
	if len(assigned) != 1 || assigned[0].ID != forA.ID {
		t.Fatalf("expected only forA assigned to worker-a, got %+v", assigned)
	}
}

func TestServiceListAssignedToWorkerExcludesAlreadyStartedJobs(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()

	job, err := svc.Submit(ctx, validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(ctx, job.ID, []string{"worker-a"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}
	if _, err := svc.Start(ctx, job.ID); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	assigned, err := svc.ListAssignedToWorker(ctx, "worker-a")
	if err != nil {
		t.Fatalf("ListAssignedToWorker returned error: %v", err)
	}
	if len(assigned) != 0 {
		t.Fatalf("expected an already-started job to not appear again, got %+v", assigned)
	}
}

func TestServiceStartTransitionsScheduledToRunning(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()

	job, err := svc.Submit(ctx, validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(ctx, job.ID, []string{"worker-a"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}

	started, err := svc.Start(ctx, job.ID)
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if started.State != StateRunning {
		t.Errorf("State = %q, want %q", started.State, StateRunning)
	}
	if started.StartedAt == nil {
		t.Error("expected StartedAt to be set")
	}
}

func TestServiceStartRejectsNonScheduledJob(t *testing.T) {
	svc, _ := newTestService()
	job, err := svc.Submit(context.Background(), validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.Start(context.Background(), job.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("expected ErrInvalidState for a QUEUED (not SCHEDULED) job, got %v", err)
	}
}

func TestServiceCompleteTransitionsRunningToSucceeded(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()
	job, err := svc.Submit(ctx, validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(ctx, job.ID, []string{"worker-a"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}
	if _, err := svc.Start(ctx, job.ID); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	completed, err := svc.Complete(ctx, job.ID)
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if completed.State != StateSucceeded {
		t.Errorf("State = %q, want %q", completed.State, StateSucceeded)
	}
	if completed.CompletedAt == nil {
		t.Error("expected CompletedAt to be set")
	}
}

func TestServiceCompleteRejectsNonRunningJob(t *testing.T) {
	svc, _ := newTestService()
	job, err := svc.Submit(context.Background(), validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.Complete(context.Background(), job.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("expected ErrInvalidState for a QUEUED (not RUNNING) job, got %v", err)
	}
}

func TestServiceFailRequeuesWhenRetriesRemain(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()
	in := validSubmitInput()
	in.RetryLimit = 3
	job, err := svc.Submit(ctx, in)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(ctx, job.ID, []string{"worker-a"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}
	if _, err := svc.Start(ctx, job.ID); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	failed, err := svc.Fail(ctx, job.ID, "simulated failure")
	if err != nil {
		t.Fatalf("Fail returned error: %v", err)
	}
	if failed.State != StateQueued {
		t.Errorf("State = %q, want %q (retries remain)", failed.State, StateQueued)
	}
	if failed.CurrentAttempt != 2 {
		t.Errorf("CurrentAttempt = %d, want 2", failed.CurrentAttempt)
	}
	if failed.FailureReason != "simulated failure" {
		t.Errorf("FailureReason = %q, want %q", failed.FailureReason, "simulated failure")
	}
	if len(failed.AssignedWorkerIDs) != 0 {
		t.Errorf("expected AssignedWorkerIDs cleared on requeue, got %v", failed.AssignedWorkerIDs)
	}
	if failed.StartedAt != nil {
		t.Error("expected StartedAt cleared on requeue")
	}
}

func TestServiceFailReachesTerminalStateWhenRetriesExhausted(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()
	in := validSubmitInput()
	in.RetryLimit = 1
	job, err := svc.Submit(ctx, in) // CurrentAttempt starts at 1 == RetryLimit
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(ctx, job.ID, []string{"worker-a"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}
	if _, err := svc.Start(ctx, job.ID); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	failed, err := svc.Fail(ctx, job.ID, "out of retries")
	if err != nil {
		t.Fatalf("Fail returned error: %v", err)
	}
	if failed.State != StateFailed {
		t.Errorf("State = %q, want %q (retry limit exhausted)", failed.State, StateFailed)
	}
	if failed.FailedAt == nil {
		t.Error("expected FailedAt to be set")
	}
}

func TestServiceFailRejectsNonRunningJob(t *testing.T) {
	svc, _ := newTestService()
	job, err := svc.Submit(context.Background(), validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.Fail(context.Background(), job.ID, "x"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("expected ErrInvalidState for a QUEUED (not RUNNING) job, got %v", err)
	}
}

func TestServiceLoseWorkerRequeuesRunningAndScheduledJobs(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()

	running, err := svc.Submit(ctx, validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(ctx, running.ID, []string{"worker-lost"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}
	if _, err := svc.Start(ctx, running.ID); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	scheduled, err := svc.Submit(ctx, validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(ctx, scheduled.ID, []string{"worker-lost"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}

	unrelated, err := svc.Submit(ctx, validSubmitInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(ctx, unrelated.ID, []string{"worker-other"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}

	recovered, err := svc.LoseWorker(ctx, "worker-lost")
	if err != nil {
		t.Fatalf("LoseWorker returned error: %v", err)
	}
	if recovered != 2 {
		t.Fatalf("recovered = %d, want 2", recovered)
	}

	for _, id := range []string{running.ID, scheduled.ID} {
		got, err := svc.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get(%s) returned error: %v", id, err)
		}
		if got.State != StateQueued {
			t.Errorf("job %s: State = %q, want %q", id, got.State, StateQueued)
		}
	}

	gotUnrelated, err := svc.Get(ctx, unrelated.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if gotUnrelated.State != StateScheduled {
		t.Errorf("unrelated job's State = %q, want unchanged %q", gotUnrelated.State, StateScheduled)
	}
}

func TestServiceLoseWorkerMarksFailedWhenRetriesExhausted(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()
	in := validSubmitInput()
	in.RetryLimit = 1
	job, err := svc.Submit(ctx, in)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := svc.AssignToWorkers(ctx, job.ID, []string{"worker-lost"}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}
	if _, err := svc.Start(ctx, job.ID); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if _, err := svc.LoseWorker(ctx, "worker-lost"); err != nil {
		t.Fatalf("LoseWorker returned error: %v", err)
	}

	got, err := svc.Get(ctx, job.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != StateFailed {
		t.Errorf("State = %q, want %q", got.State, StateFailed)
	}
}

func TestServiceLoseWorkerReturnsZeroWhenNoJobsAssigned(t *testing.T) {
	svc, _ := newTestService()
	recovered, err := svc.LoseWorker(context.Background(), "worker-nonexistent")
	if err != nil {
		t.Fatalf("LoseWorker returned error: %v", err)
	}
	if recovered != 0 {
		t.Errorf("recovered = %d, want 0", recovered)
	}
}
