package scheduler

import (
	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

// committedResources is what's already claimed on one worker by jobs that
// are actively holding capacity (SCHEDULED, RUNNING, CHECKPOINTING) —
// QUEUED/RETRYING/CANCEL_REQUESTED/PREEMPTED jobs don't hold anything
// yet, so they're excluded.
//
// GPU commitment is tracked as an aggregate count per worker, not against
// specific device indices — until Phase 6 introduces real execution with
// heartbeat-reported per-device utilization, there's no way to know
// which physical GPU a running job is actually using. Subtracting "N
// GPUs committed" from "N GPUs meeting this job's memory bar" is a
// documented, conservative approximation for this phase — see
// docs/adr/0002-scheduler-design.md.
type committedResources struct {
	gpuCount    int
	cpuCores    float64
	memoryBytes int64
}

// committedByWorker sums the resource requests of every actively-held job
// in activeJobs, keyed by the worker(s) it's assigned to.
func committedByWorker(activeJobs []jobs.Job) map[string]committedResources {
	out := make(map[string]committedResources)
	for _, j := range activeJobs {
		switch j.State {
		case jobs.StateScheduled, jobs.StateRunning, jobs.StateCheckpointing:
		default:
			continue
		}
		for _, workerID := range j.AssignedWorkerIDs {
			c := out[workerID]
			c.gpuCount += int(j.Resources.GPUCount)
			c.cpuCores += j.Resources.CPUCores
			c.memoryBytes += j.Resources.MemoryBytes
			out[workerID] = c
		}
	}
	return out
}

// freeGPUCount returns how many of w's GPUs currently qualify for a job
// needing at least minMemoryBytes free per GPU, after subtracting GPUs
// already committed on this worker.
func freeGPUCount(w workers.Worker, minMemoryBytes int64, committedGPUCount int) int {
	qualifying := 0
	for _, g := range w.GPUs {
		if g.TotalMemoryBytes >= minMemoryBytes {
			qualifying++
		}
	}
	free := qualifying - committedGPUCount
	if free < 0 {
		return 0
	}
	return free
}
