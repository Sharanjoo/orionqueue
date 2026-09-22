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
