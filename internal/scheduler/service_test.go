package scheduler

import (
	"context"
	"sync"
	"testing"

	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/leases"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

func newTestServiceStack(t *testing.T) (*Service, *jobs.Service, *workers.Service, *MemoryDecisionRepository) {
	t.Helper()
	jobSvc := jobs.NewService(jobs.NewMemoryRepository())
	workerSvc := workers.NewService(workers.NewMemoryRepository(), leases.NewFakeManager())
	decisions := NewMemoryDecisionRepository()
	svc := NewService(jobSvc, workerSvc, decisions)
	return svc, jobSvc, workerSvc, decisions
}

func validJobInput() jobs.SubmitInput {
	return jobs.SubmitInput{
		Name:       "train",
		Owner:      "sharan",
		Image:      "img",
		Resources:  jobs.ResourceRequest{GPUCount: 1, MinGPUMemoryBytes: 1 << 30, CPUCores: 1, MemoryBytes: 1 << 30},
		Priority:   50,
		RetryLimit: 1,
	}
}

func validWorkerInput() workers.RegisterInput {
	return workers.RegisterInput{
		Hostname:            "worker-1.local",
		CPUCapacity:         8,
		MemoryCapacityBytes: 32 << 30,
		GPUs:                []workers.GPU{{DeviceIndex: 0, TotalMemoryBytes: 16 << 30}},
	}
}

func TestServiceRunOnceAssignsAQueuedJobAndRecordsADecision(t *testing.T) {
	svc, jobSvc, workerSvc, decisions := newTestServiceStack(t)
	ctx := context.Background()

	job, err := jobSvc.Submit(ctx, validJobInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	worker, err := workerSvc.Register(ctx, validWorkerInput())
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	result, err := svc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if result.Assigned != 1 || result.Considered != 1 || result.Conflicted != 0 {
		t.Fatalf("unexpected Result: %+v", result)
	}

	got, err := jobSvc.Get(ctx, job.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != jobs.StateScheduled {
		t.Errorf("State = %q, want %q", got.State, jobs.StateScheduled)
	}
	if len(got.AssignedWorkerIDs) != 1 || got.AssignedWorkerIDs[0] != worker.ID {
		t.Errorf("AssignedWorkerIDs = %v, want [%s]", got.AssignedWorkerIDs, worker.ID)
	}

	recorded := decisions.Snapshot()
	if len(recorded) != 1 {
		t.Fatalf("expected 1 recorded decision, got %d", len(recorded))
	}
	if recorded[0].JobID != job.ID || recorded[0].WorkerID != worker.ID {
		t.Errorf("recorded decision = %+v, want job %s -> worker %s", recorded[0], job.ID, worker.ID)
	}
}

func TestServiceRunOnceLeavesUnschedulableJobsQueued(t *testing.T) {
	svc, jobSvc, _, _ := newTestServiceStack(t)
	ctx := context.Background()

	// No workers registered at all.
	job, err := jobSvc.Submit(ctx, validJobInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	result, err := svc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if result.Assigned != 0 {
		t.Errorf("Assigned = %d, want 0 (no workers)", result.Assigned)
	}

	got, err := jobSvc.Get(ctx, job.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != jobs.StateQueued {
		t.Errorf("State = %q, want %q (still queued, not failed)", got.State, jobs.StateQueued)
	}
}

func TestServiceRunOnceIsSafeAcrossConcurrentSchedulerInstances(t *testing.T) {
	// Two independent scheduler.Service instances sharing the same
	// underlying jobs/workers services (as two scheduler replicas would
	// share the same PostgreSQL database) both attempt to schedule the
	// same single QUEUED job onto the same single available GPU,
	// concurrently. Exactly one must win — this is the most direct
	// possible proof of "duplicate scheduler instances do not make
	// conflicting assignments" (Phase 5's 4th acceptance criterion).
	jobSvc := jobs.NewService(jobs.NewMemoryRepository())
	workerSvc := workers.NewService(workers.NewMemoryRepository(), leases.NewFakeManager())
	decisionsA := NewMemoryDecisionRepository()
	decisionsB := NewMemoryDecisionRepository()
	schedulerA := NewService(jobSvc, workerSvc, decisionsA)
	schedulerB := NewService(jobSvc, workerSvc, decisionsB)

	ctx := context.Background()
	job, err := jobSvc.Submit(ctx, validJobInput())
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := workerSvc.Register(ctx, validWorkerInput()); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	var wg sync.WaitGroup
	results := make([]Result, 2)
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); results[0], errs[0] = schedulerA.RunOnce(ctx) }()
	go func() { defer wg.Done(); results[1], errs[1] = schedulerB.RunOnce(ctx) }()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("RunOnce[%d] returned error: %v", i, err)
		}
	}

	// The one invariant that must always hold, regardless of how the Go
	// scheduler happens to interleave the two goroutines: exactly one
	// successful assignment, never zero (both losing) and never two
	// (double-booking). How that shows up in Result varies legitimately
	// with timing: if both instances' RunOnce read the job as QUEUED
	// before either commits, the loser's AssignToWorkers call is rejected
	// and counted as Conflicted; if one instance's RunOnce completes
	// entirely (list, plan, assign) before the other even starts, the
	// second one's own fresh read already sees the job as SCHEDULED, so
	// it correctly finds nothing left to do (Considered/Assigned/Conflicted
	// all 0 for that instance) — both outcomes are correct, so this test
	// only asserts the invariant, not which mechanism produced it.
	totalAssigned := results[0].Assigned + results[1].Assigned
	if totalAssigned != 1 {
		t.Fatalf("expected exactly 1 successful assignment across both concurrent schedulers, got %d (results: %+v)", totalAssigned, results)
	}

	got, err := jobSvc.Get(ctx, job.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != jobs.StateScheduled {
		t.Errorf("State = %q, want %q", got.State, jobs.StateScheduled)
	}
	if len(got.AssignedWorkerIDs) != 1 {
		t.Errorf("AssignedWorkerIDs = %v, want exactly 1 worker (no double-booking)", got.AssignedWorkerIDs)
	}

	// Exactly one of the two decision repositories should have recorded
	// the (single) successful decision.
	totalRecorded := len(decisionsA.Snapshot()) + len(decisionsB.Snapshot())
	if totalRecorded != 1 {
		t.Errorf("expected exactly 1 recorded decision across both repositories, got %d", totalRecorded)
	}
}

func TestServiceRunOnceDoesNotPreemptWhenDisabled(t *testing.T) {
	svc, jobSvc, workerSvc, _ := newTestServiceStack(t) // preemption off by default
	ctx := context.Background()

	worker, err := workerSvc.Register(ctx, validWorkerInput()) // 1 GPU
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	lowIn := validJobInput()
	lowIn.Priority = 10
	lowIn.Preemptible = true
	low, err := jobSvc.Submit(ctx, lowIn)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := jobSvc.AssignToWorkers(ctx, low.ID, []string{worker.ID}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}
	if _, err := jobSvc.Start(ctx, low.ID); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	highIn := validJobInput()
	highIn.Priority = 90
	if _, err := jobSvc.Submit(ctx, highIn); err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	result, err := svc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if result.Preempted != 0 {
		t.Fatalf("Preempted = %d, want 0 (preemption disabled)", result.Preempted)
	}

	got, err := jobSvc.Get(ctx, low.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != jobs.StateRunning {
		t.Errorf("low-priority job's State = %q, want unchanged %q", got.State, jobs.StateRunning)
	}
}

func TestServiceRunOnceWithPreemptionEnabledPreemptsThenReschedulesAfterStop(t *testing.T) {
	jobSvc := jobs.NewService(jobs.NewMemoryRepository())
	workerSvc := workers.NewService(workers.NewMemoryRepository(), leases.NewFakeManager())
	decisions := NewMemoryDecisionRepository()
	svc := NewService(jobSvc, workerSvc, decisions, WithPreemptionEnabled(true))
	ctx := context.Background()

	worker, err := workerSvc.Register(ctx, validWorkerInput()) // 1 GPU total
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	lowIn := validJobInput()
	lowIn.Priority = 10
	lowIn.Preemptible = true
	low, err := jobSvc.Submit(ctx, lowIn)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := jobSvc.AssignToWorkers(ctx, low.ID, []string{worker.ID}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}
	if _, err := jobSvc.Start(ctx, low.ID); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	highIn := validJobInput()
	highIn.Priority = 90
	high, err := jobSvc.Submit(ctx, highIn)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	// Pass 1: no capacity is free (worker's only GPU is held by the
	// RUNNING low-priority job), so the high-priority job can't be
	// placed outright — this pass should preempt low instead.
	result, err := svc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce (pass 1) returned error: %v", err)
	}
	if result.Preempted != 1 {
		t.Fatalf("Preempted = %d, want 1", result.Preempted)
	}
	if result.Assigned != 0 {
		t.Fatalf("Assigned = %d, want 0 (capacity isn't freed until the worker confirms the stop)", result.Assigned)
	}

	got, err := jobSvc.Get(ctx, low.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != jobs.StatePreempted {
		t.Fatalf("low-priority job's State = %q, want %q", got.State, jobs.StatePreempted)
	}

	// The worker "confirms" it stopped the preempted job.
	if _, err := jobSvc.ReportStopped(ctx, low.ID); err != nil {
		t.Fatalf("ReportStopped returned error: %v", err)
	}

	// Pass 2: capacity is genuinely free now — the high-priority job
	// should win the freed GPU (it's the higher-priority QUEUED job).
	result, err = svc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce (pass 2) returned error: %v", err)
	}
	if result.Assigned != 1 {
		t.Fatalf("Assigned = %d, want 1", result.Assigned)
	}

	got, err = jobSvc.Get(ctx, high.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != jobs.StateScheduled {
		t.Errorf("high-priority job's State = %q, want %q", got.State, jobs.StateScheduled)
	}
	if len(got.AssignedWorkerIDs) != 1 || got.AssignedWorkerIDs[0] != worker.ID {
		t.Errorf("AssignedWorkerIDs = %v, want [%s]", got.AssignedWorkerIDs, worker.ID)
	}

	got, err = jobSvc.Get(ctx, low.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != jobs.StateQueued {
		t.Errorf("preempted job's State = %q, want %q (requeued, no free capacity left for it)", got.State, jobs.StateQueued)
	}
	if got.CurrentAttempt != 1 {
		t.Errorf("preempted job's CurrentAttempt = %d, want unchanged 1", got.CurrentAttempt)
	}
}
