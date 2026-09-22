// Package scheduler implements OrionQueue's scheduling algorithm: turning
// a snapshot of QUEUED jobs and ACTIVE workers into placement decisions.
// The algorithm itself (Plan, in this file) is a pure function — no I/O,
// no mutation of its inputs — so it's exhaustively unit testable without
// a database, etcd, or any running service. Service (service.go) wraps it
// with the actual fetch/apply/persist cycle and etcd leader election.
//
// See docs/adr/0002-scheduler-design.md for the full design rationale.
package scheduler

import (
	"sort"
	"time"

	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

// Assignment is one scheduling decision: job should be placed on worker.
// Gang scheduling in this project's scope means every GPU a job needs
// comes from a single worker — not spread across multiple workers, which
// would require cross-worker network-topology awareness this portfolio
// project doesn't model. So WorkerID is singular, not a slice: a job is
// either placed entirely on one worker or not placed at all this pass.
type Assignment struct {
	JobID    string
	WorkerID string
	Reason   string
}

// Plan computes scheduling assignments for the given snapshot of active
// jobs and active workers, as of now.
//
// Only QUEUED jobs are ever assigned; SCHEDULED/RUNNING/CHECKPOINTING
// jobs in activeJobs are considered only for the capacity they've
// already committed (see committedByWorker). Jobs are considered in
// (EffectivePriority DESC, CreatedAt ASC, ID ASC) order, so the aging
// bonus is what keeps a long-waiting low-priority job from being starved
// forever by a steady stream of higher-priority submissions. A job with
// no currently eligible worker is skipped — it stays QUEUED and does not
// block jobs behind it in the queue from being considered; this project
// does not attempt to permanently fail a job as "can never fit," since
// that's genuinely undecidable in a cluster where new, larger workers can
// register later (see ADR-0002's "alternatives considered").
//
// Within a single Plan call, each successful assignment immediately
// reserves that worker's capacity for the remainder of the pass, so two
// jobs considered in the same call can never be double-assigned the same
// GPUs — this is gang scheduling's atomicity guarantee at the planning
// level; Service.RunOnce provides the second half (the actual database
// write is atomic per job via jobs.Service.AssignToWorkers).
func Plan(activeJobs []jobs.Job, activeWorkers []workers.Worker, now time.Time) []Assignment {
	committed := committedByWorker(activeJobs)

	queued := make([]jobs.Job, 0, len(activeJobs))
	for _, j := range activeJobs {
		if j.State == jobs.StateQueued {
			queued = append(queued, j)
		}
	}
	sortByEffectivePriority(queued, now)

	var assignments []Assignment
	for _, job := range queued {
		workerID, ok := selectWorker(job, activeWorkers, committed)
		if !ok {
			continue
		}
		assignments = append(assignments, Assignment{
			JobID:    job.ID,
			WorkerID: workerID,
			Reason:   "eligible worker found",
		})

		c := committed[workerID]
		c.gpuCount += int(job.Resources.GPUCount)
		c.cpuCores += job.Resources.CPUCores
		c.memoryBytes += job.Resources.MemoryBytes
		committed[workerID] = c
	}
	return assignments
}

func sortByEffectivePriority(queued []jobs.Job, now time.Time) {
	sort.SliceStable(queued, func(i, k int) bool {
		pi := EffectivePriority(queued[i].Priority, queued[i].CreatedAt, now)
		pk := EffectivePriority(queued[k].Priority, queued[k].CreatedAt, now)
		if pi != pk {
			return pi > pk
		}
		if !queued[i].CreatedAt.Equal(queued[k].CreatedAt) {
			return queued[i].CreatedAt.Before(queued[k].CreatedAt)
		}
		return queued[i].ID < queued[k].ID
	})
}

// selectWorker finds the best-fit eligible worker for job among
// activeWorkers, given what's already committed this pass. "Best fit"
// means the worker with the fewest qualifying GPUs left over after
// placing this job (tightest fit / bin-packing), preferred over spread
// scheduling because it keeps more whole workers free for large
// multi-GPU jobs — see ADR-0002.
func selectWorker(job jobs.Job, activeWorkers []workers.Worker, committed map[string]committedResources) (string, bool) {
	type candidate struct {
		workerID string
		surplus  int
	}
	var candidates []candidate

	for _, w := range activeWorkers {
		// Defense in depth: only ACTIVE workers are ever eligible, even
		// if a caller's activeWorkers slice wasn't already filtered —
		// see internal/workers.Repository.ListActive, which is the
		// normal source of this slice.
		if w.Status != workers.StatusActive {
			continue
		}

		c := committed[w.ID]
		freeCPU := w.CPUCapacity - c.cpuCores
		freeMemory := w.MemoryCapacityBytes - c.memoryBytes
		freeGPUs := freeGPUCount(w, job.Resources.MinGPUMemoryBytes, c.gpuCount)

		if freeCPU < job.Resources.CPUCores {
			continue
		}
		if freeMemory < job.Resources.MemoryBytes {
			continue
		}
		if freeGPUs < int(job.Resources.GPUCount) {
			continue
		}
		candidates = append(candidates, candidate{
			workerID: w.ID,
			surplus:  freeGPUs - int(job.Resources.GPUCount),
		})
	}

	if len(candidates) == 0 {
		return "", false
	}

	sort.SliceStable(candidates, func(i, k int) bool {
		if candidates[i].surplus != candidates[k].surplus {
			return candidates[i].surplus < candidates[k].surplus
		}
		return candidates[i].workerID < candidates[k].workerID
	})
	return candidates[0].workerID, true
}
