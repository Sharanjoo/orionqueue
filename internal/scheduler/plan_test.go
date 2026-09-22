package scheduler

import (
	"testing"
	"time"

	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

var baseTime = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func queuedJob(id string, priority int32, gpuCount uint32, minGPUMemory int64, cpu float64, mem int64) jobs.Job {
	return jobs.Job{
		ID:       id,
		Priority: priority,
		State:    jobs.StateQueued,
		Resources: jobs.ResourceRequest{
			GPUCount:          gpuCount,
			MinGPUMemoryBytes: minGPUMemory,
			CPUCores:          cpu,
			MemoryBytes:       mem,
		},
		CreatedAt: baseTime,
	}
}

func activeWorker(id string, cpu float64, mem int64, gpus ...int64) workers.Worker {
	w := workers.Worker{
		ID:                  id,
		Status:              workers.StatusActive,
		CPUCapacity:         cpu,
		MemoryCapacityBytes: mem,
	}
	for i, memBytes := range gpus {
		w.GPUs = append(w.GPUs, workers.GPU{DeviceIndex: int32(i), TotalMemoryBytes: memBytes})
	}
	return w
}

func assignmentFor(t *testing.T, assignments []Assignment, jobID string) (Assignment, bool) {
	t.Helper()
	for _, a := range assignments {
		if a.JobID == jobID {
			return a, true
		}
	}
	return Assignment{}, false
}

func TestPlanAssignsAJobToAnEligibleWorker(t *testing.T) {
	job := queuedJob("job-1", 50, 1, 8<<30, 2, 4<<30)
	worker := activeWorker("worker-1", 8, 32<<30, 16<<30)

	assignments := Plan([]jobs.Job{job}, []workers.Worker{worker}, baseTime)

	a, ok := assignmentFor(t, assignments, "job-1")
	if !ok {
		t.Fatal("expected job-1 to be assigned")
	}
	if a.WorkerID != "worker-1" {
		t.Errorf("WorkerID = %q, want %q", a.WorkerID, "worker-1")
	}
}

func TestPlanRejectsWorkerWithInsufficientGPUCount(t *testing.T) {
	job := queuedJob("job-1", 50, 2, 8<<30, 1, 1<<30)     // needs 2 GPUs
	worker := activeWorker("worker-1", 8, 32<<30, 16<<30) // has only 1

	assignments := Plan([]jobs.Job{job}, []workers.Worker{worker}, baseTime)

	if _, ok := assignmentFor(t, assignments, "job-1"); ok {
		t.Fatal("expected job-1 to remain unassigned (insufficient GPU count)")
	}
}

func TestPlanRejectsWorkerWithInsufficientGPUMemory(t *testing.T) {
	job := queuedJob("job-1", 50, 1, 16<<30, 1, 1<<30)   // needs 16GiB/GPU
	worker := activeWorker("worker-1", 8, 32<<30, 8<<30) // GPU only has 8GiB

	assignments := Plan([]jobs.Job{job}, []workers.Worker{worker}, baseTime)

	if _, ok := assignmentFor(t, assignments, "job-1"); ok {
		t.Fatal("expected job-1 to remain unassigned (insufficient GPU memory)")
	}
}

func TestPlanRejectsWorkerWithInsufficientCPU(t *testing.T) {
	job := queuedJob("job-1", 50, 0, 0, 16, 1<<30) // needs 16 cores
	worker := activeWorker("worker-1", 8, 32<<30)  // has only 8

	assignments := Plan([]jobs.Job{job}, []workers.Worker{worker}, baseTime)

	if _, ok := assignmentFor(t, assignments, "job-1"); ok {
		t.Fatal("expected job-1 to remain unassigned (insufficient CPU)")
	}
}

func TestPlanRejectsWorkerWithInsufficientMemory(t *testing.T) {
	job := queuedJob("job-1", 50, 0, 0, 1, 64<<30) // needs 64GiB RAM
	worker := activeWorker("worker-1", 8, 32<<30)  // has only 32GiB

	assignments := Plan([]jobs.Job{job}, []workers.Worker{worker}, baseTime)

	if _, ok := assignmentFor(t, assignments, "job-1"); ok {
		t.Fatal("expected job-1 to remain unassigned (insufficient memory)")
	}
}

func TestPlanIgnoresNonActiveWorkers(t *testing.T) {
	job := queuedJob("job-1", 50, 1, 1<<30, 1, 1<<30)
	lost := activeWorker("worker-1", 8, 32<<30, 16<<30)
	lost.Status = workers.StatusLost

	assignments := Plan([]jobs.Job{job}, []workers.Worker{lost}, baseTime)

	if _, ok := assignmentFor(t, assignments, "job-1"); ok {
		t.Fatal("expected job-1 to remain unassigned (only worker is LOST)")
	}
}

func TestPlanRespectsPriorityOrderWhenResourcesAreScarce(t *testing.T) {
	// Only one worker, only enough for one of the two jobs.
	low := queuedJob("job-low", 10, 1, 1<<30, 1, 1<<30)
	high := queuedJob("job-high", 90, 1, 1<<30, 1, 1<<30)
	worker := activeWorker("worker-1", 8, 32<<30, 16<<30) // 1 GPU total

	assignments := Plan([]jobs.Job{low, high}, []workers.Worker{worker}, baseTime)

	if _, ok := assignmentFor(t, assignments, "job-high"); !ok {
		t.Error("expected the higher-priority job to be assigned")
	}
	if _, ok := assignmentFor(t, assignments, "job-low"); ok {
		t.Error("expected the lower-priority job to remain unassigned (no capacity left)")
	}
}

func TestPlanAgingLetsALongWaitingLowPriorityJobWinOverAFreshHighPriorityJob(t *testing.T) {
	// job-low has been waiting long enough to accrue the max aging bonus;
	// job-high was just submitted. Only one GPU available.
	low := queuedJob("job-low", 10, 1, 1<<30, 1, 1<<30)
	low.CreatedAt = baseTime.Add(-time.Duration(MaxAgingBonus+5) * AgingInterval)
	high := queuedJob("job-high", 10+MaxAgingBonus-1, 1, 1<<30, 1, 1<<30) // still below low's aged priority
	high.CreatedAt = baseTime

	worker := activeWorker("worker-1", 8, 32<<30, 16<<30)

	assignments := Plan([]jobs.Job{low, high}, []workers.Worker{worker}, baseTime)

	if _, ok := assignmentFor(t, assignments, "job-low"); !ok {
		t.Error("expected the aged low-priority job to win the single slot")
	}
	if _, ok := assignmentFor(t, assignments, "job-high"); ok {
		t.Error("expected the fresh higher-nominal-priority job to lose to the aged job")
	}
}

func TestPlanGangSchedulingIsAllOrNothing(t *testing.T) {
	// Job needs 4 GPUs; worker only has 3. Must not be partially assigned.
	job := queuedJob("job-1", 50, 4, 1<<30, 1, 1<<30)
	worker := activeWorker("worker-1", 8, 32<<30, 16<<30, 16<<30, 16<<30) // only 3 GPUs

	assignments := Plan([]jobs.Job{job}, []workers.Worker{worker}, baseTime)

	if len(assignments) != 0 {
		t.Fatalf("expected no assignment for an unfulfillable gang request, got %+v", assignments)
	}
}

func TestPlanGangSchedulingPlacesAllGPUsOnOneWorkerWhenAvailable(t *testing.T) {
	job := queuedJob("job-1", 50, 4, 1<<30, 1, 1<<30)
	worker := activeWorker("worker-1", 8, 32<<30, 16<<30, 16<<30, 16<<30, 16<<30) // exactly 4 GPUs

	assignments := Plan([]jobs.Job{job}, []workers.Worker{worker}, baseTime)

	a, ok := assignmentFor(t, assignments, "job-1")
	if !ok {
		t.Fatal("expected job-1 to be assigned")
	}
	if a.WorkerID != "worker-1" {
		t.Errorf("WorkerID = %q, want %q", a.WorkerID, "worker-1")
	}
}

func TestPlanPrefersTightestFitBinPacking(t *testing.T) {
	job := queuedJob("job-1", 50, 1, 1<<30, 1, 1<<30)                                // needs 1 GPU
	roomy := activeWorker("worker-roomy", 8, 32<<30, 16<<30, 16<<30, 16<<30, 16<<30) // 4 free GPUs
	tight := activeWorker("worker-tight", 8, 32<<30, 16<<30)                         // 1 free GPU

	assignments := Plan([]jobs.Job{job}, []workers.Worker{roomy, tight}, baseTime)

	a, ok := assignmentFor(t, assignments, "job-1")
	if !ok {
		t.Fatal("expected job-1 to be assigned")
	}
	if a.WorkerID != "worker-tight" {
		t.Errorf("WorkerID = %q, want %q (tightest fit / bin-packing)", a.WorkerID, "worker-tight")
	}
}

func TestPlanDoesNotDoubleBookTheSameGPUsWithinOnePass(t *testing.T) {
	// Two jobs, each needing 1 GPU; worker has exactly 1 GPU. Only one of
	// the two should be assigned, not both.
	jobA := queuedJob("job-a", 50, 1, 1<<30, 1, 1<<30)
	jobB := queuedJob("job-b", 50, 1, 1<<30, 1, 1<<30)
	jobB.CreatedAt = baseTime.Add(time.Second) // tie-break: job-a is earlier
	worker := activeWorker("worker-1", 8, 32<<30, 16<<30)

	assignments := Plan([]jobs.Job{jobA, jobB}, []workers.Worker{worker}, baseTime)

	if len(assignments) != 1 {
		t.Fatalf("expected exactly 1 assignment (only 1 GPU available), got %d: %+v", len(assignments), assignments)
	}
	if assignments[0].JobID != "job-a" {
		t.Errorf("expected job-a (earlier submission) to win the single GPU, got %q", assignments[0].JobID)
	}
}

func TestPlanAccountsForCapacityAlreadyCommittedBySCHEDULEDJobs(t *testing.T) {
	alreadyScheduled := queuedJob("job-running", 50, 1, 1<<30, 1, 1<<30)
	alreadyScheduled.State = jobs.StateScheduled
	alreadyScheduled.AssignedWorkerIDs = []string{"worker-1"}

	newJob := queuedJob("job-new", 50, 1, 1<<30, 1, 1<<30)
	worker := activeWorker("worker-1", 8, 32<<30, 16<<30) // only 1 GPU total, already committed

	assignments := Plan([]jobs.Job{alreadyScheduled, newJob}, []workers.Worker{worker}, baseTime)

	if _, ok := assignmentFor(t, assignments, "job-new"); ok {
		t.Fatal("expected job-new to remain unassigned (its only GPU is already committed to job-running)")
	}
}

func TestPlanIgnoresCapacityHeldByTerminalOrNotYetActiveJobs(t *testing.T) {
	// A CANCELLED job's old assignment must not hold capacity, and a
	// RETRYING job (queued again, not yet re-scheduled) must not either.
	cancelled := queuedJob("job-cancelled", 50, 1, 1<<30, 1, 1<<30)
	cancelled.State = jobs.StateCancelled
	cancelled.AssignedWorkerIDs = []string{"worker-1"}

	retrying := queuedJob("job-retrying", 50, 1, 1<<30, 1, 1<<30)
	retrying.State = jobs.StateRetrying
	retrying.AssignedWorkerIDs = []string{"worker-1"} // stale from a previous attempt

	newJob := queuedJob("job-new", 50, 1, 1<<30, 1, 1<<30)
	worker := activeWorker("worker-1", 8, 32<<30, 16<<30)

	assignments := Plan([]jobs.Job{cancelled, retrying, newJob}, []workers.Worker{worker}, baseTime)

	if _, ok := assignmentFor(t, assignments, "job-new"); !ok {
		t.Fatal("expected job-new to be assigned; CANCELLED/RETRYING jobs must not hold capacity")
	}
}

func TestPlanReturnsEmptyForNoQueuedJobs(t *testing.T) {
	worker := activeWorker("worker-1", 8, 32<<30, 16<<30)
	assignments := Plan(nil, []workers.Worker{worker}, baseTime)
	if len(assignments) != 0 {
		t.Fatalf("expected no assignments, got %+v", assignments)
	}
}

func TestPlanReturnsEmptyForNoWorkers(t *testing.T) {
	job := queuedJob("job-1", 50, 1, 1<<30, 1, 1<<30)
	assignments := Plan([]jobs.Job{job}, nil, baseTime)
	if len(assignments) != 0 {
		t.Fatalf("expected no assignments with zero workers, got %+v", assignments)
	}
}

func TestPlanSkipsUnschedulableJobButStillSchedulesOthersBehindIt(t *testing.T) {
	tooBig := queuedJob("job-toobig", 90, 8, 1<<30, 1, 1<<30) // needs 8 GPUs, nothing has that many
	fits := queuedJob("job-fits", 10, 1, 1<<30, 1, 1<<30)
	worker := activeWorker("worker-1", 8, 32<<30, 16<<30)

	assignments := Plan([]jobs.Job{tooBig, fits}, []workers.Worker{worker}, baseTime)

	if _, ok := assignmentFor(t, assignments, "job-toobig"); ok {
		t.Error("expected job-toobig to remain unassigned")
	}
	if _, ok := assignmentFor(t, assignments, "job-fits"); !ok {
		t.Error("expected job-fits to still be scheduled despite the higher-priority job ahead of it not fitting")
	}
}

func TestEffectivePriorityAppliesNoAgingBeforeFirstInterval(t *testing.T) {
	p := EffectivePriority(50, baseTime, baseTime.Add(AgingInterval-time.Second))
	if p != 50 {
		t.Errorf("EffectivePriority = %d, want 50 (no full interval elapsed)", p)
	}
}

func TestEffectivePriorityCapsAtMaxAgingBonus(t *testing.T) {
	p := EffectivePriority(50, baseTime, baseTime.Add(1000*AgingInterval))
	want := int32(50) + MaxAgingBonus
	if p != want {
		t.Errorf("EffectivePriority = %d, want %d (capped)", p, want)
	}
}

func TestEffectivePriorityHandlesFutureCreatedAtGracefully(t *testing.T) {
	p := EffectivePriority(50, baseTime.Add(time.Hour), baseTime)
	if p != 50 {
		t.Errorf("EffectivePriority = %d, want 50 (createdAt in the future relative to now)", p)
	}
}
