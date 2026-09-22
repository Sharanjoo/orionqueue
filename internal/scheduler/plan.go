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
	"fmt"
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

// Preemption is one scheduler decision to preempt a RUNNING, Preemptible
// job in order to free capacity for a strictly higher-priority pending
// job — see ADR-0005.
type Preemption struct {
	JobID    string
	WorkerID string
	Reason   string
}

// PlanPreemptions computes which RUNNING, Preemptible jobs (if any) should
// be preempted to free enough capacity for QUEUED jobs that the
// already-computed placed assignments (Plan's result, on the same
// activeJobs/activeWorkers/now snapshot) left unplaced. It is strictly a
// second, opt-in pass — Plan itself never preempts anything, and
// PlanPreemptions never produces new Assignments itself; the pending job
// is picked up by Plan on a later pass, once the preempted job's worker
// actually confirms it stopped (jobs.Service.ReportStopped) and its
// capacity is genuinely free — see docs/adr/0005-preemption-policy.md.
// internal/scheduler.Service only calls this when preemption is enabled
// cluster-wide.
//
// Preemption is strictly per-worker, mirroring Plan's own single-worker
// gang-scheduling model: a pending job only benefits from preempting jobs
// on the ONE worker that would then have enough freed capacity, never
// from combining partial capacity freed across several workers. For each
// worker considered, RUNNING Preemptible jobs with a strictly lower
// EffectivePriority than the pending job are preempted lowest-priority
// first, stopping as soon as enough capacity would be freed — so never
// more jobs than necessary, and never a job at an equal or higher
// effective priority than the pending job. Pending jobs are themselves
// considered highest-effective-priority first, so a pass can free
// capacity for more than one pending job, later ones benefiting from
// capacity already freed for earlier ones.
func PlanPreemptions(activeJobs []jobs.Job, activeWorkers []workers.Worker, placed []Assignment, now time.Time) []Preemption {
	committed := committedByWorker(activeJobs)
	placedJobIDs := make(map[string]bool, len(placed))
	for _, a := range placed {
		placedJobIDs[a.JobID] = true
		// placed jobs aren't reflected as SCHEDULED in activeJobs's
		// snapshot yet (Service.RunOnce applies them separately, after
		// computing both Plan and PlanPreemptions from the same
		// snapshot) — account for their resource consumption here so
		// remaining candidates are evaluated against the true post-Plan
		// capacity, not the stale pre-Plan one.
		job, ok := jobByID(activeJobs, a.JobID)
		if !ok {
			continue
		}
		c := committed[a.WorkerID]
		c.gpuCount += int(job.Resources.GPUCount)
		c.cpuCores += job.Resources.CPUCores
		c.memoryBytes += job.Resources.MemoryBytes
		committed[a.WorkerID] = c
	}

	var queued []jobs.Job
	for _, j := range activeJobs {
		if j.State == jobs.StateQueued && !placedJobIDs[j.ID] {
			queued = append(queued, j)
		}
	}
	sortByEffectivePriority(queued, now)

	runningByWorker := runningPreemptibleByWorker(activeJobs)
	preemptedThisPass := make(map[string]bool)

	var preemptions []Preemption
	for _, job := range queued {
		pendingPriority := EffectivePriority(job.Priority, job.CreatedAt, now)

		for _, w := range activeWorkers {
			if w.Status != workers.StatusActive {
				continue
			}
			c := committed[w.ID]
			freeCPU := w.CPUCapacity - c.cpuCores
			freeMemory := w.MemoryCapacityBytes - c.memoryBytes
			freeGPUs := freeGPUCount(w, job.Resources.MinGPUMemoryBytes, c.gpuCount)

			if freeCPU >= job.Resources.CPUCores &&
				freeMemory >= job.Resources.MemoryBytes &&
				freeGPUs >= int(job.Resources.GPUCount) {
				// Already fits without preempting anything on this
				// worker (capacity freed earlier this same pass, for an
				// even-higher-priority pending job) — nothing to do for
				// this job.
				break
			}

			candidates := append([]jobs.Job(nil), runningByWorker[w.ID]...)
			sort.SliceStable(candidates, func(i, k int) bool {
				pi := EffectivePriority(candidates[i].Priority, candidates[i].CreatedAt, now)
				pk := EffectivePriority(candidates[k].Priority, candidates[k].CreatedAt, now)
				if pi != pk {
					return pi < pk
				}
				return candidates[i].ID < candidates[k].ID
			})

			var toPreempt []jobs.Job
			gpuFreed, cpuFreed, memFreed := 0, 0.0, int64(0)
			for _, cand := range candidates {
				if preemptedThisPass[cand.ID] {
					continue
				}
				if EffectivePriority(cand.Priority, cand.CreatedAt, now) >= pendingPriority {
					// Candidates are sorted ascending by priority, so no
					// later candidate in this worker's list can qualify
					// either — stop scanning this worker.
					break
				}
				toPreempt = append(toPreempt, cand)
				gpuFreed += int(cand.Resources.GPUCount)
				cpuFreed += cand.Resources.CPUCores
				memFreed += cand.Resources.MemoryBytes

				if freeCPU+cpuFreed >= job.Resources.CPUCores &&
					freeMemory+memFreed >= job.Resources.MemoryBytes &&
					freeGPUs+gpuFreed >= int(job.Resources.GPUCount) {
					break
				}
			}

			if freeCPU+cpuFreed < job.Resources.CPUCores ||
				freeMemory+memFreed < job.Resources.MemoryBytes ||
				freeGPUs+gpuFreed < int(job.Resources.GPUCount) {
				// Even preempting everything eligible on this worker
				// isn't enough to fit job — try the next worker.
				continue
			}

			for _, cand := range toPreempt {
				preemptions = append(preemptions, Preemption{
					JobID:    cand.ID,
					WorkerID: w.ID,
					Reason:   fmt.Sprintf("preempted to free resources for higher-priority job %s", job.ID),
				})
				preemptedThisPass[cand.ID] = true
			}
			nc := committed[w.ID]
			nc.gpuCount -= gpuFreed
			nc.cpuCores -= cpuFreed
			nc.memoryBytes -= memFreed
			committed[w.ID] = nc
			break
		}
	}
	return preemptions
}

// runningPreemptibleByWorker groups every RUNNING, Preemptible job in
// activeJobs by the worker it's assigned to — the pool of jobs
// PlanPreemptions is allowed to consider preempting.
func runningPreemptibleByWorker(activeJobs []jobs.Job) map[string][]jobs.Job {
	out := make(map[string][]jobs.Job)
	for _, j := range activeJobs {
		if j.State != jobs.StateRunning || !j.Preemptible {
			continue
		}
		for _, workerID := range j.AssignedWorkerIDs {
			out[workerID] = append(out[workerID], j)
		}
	}
	return out
}

// jobByID finds the job with the given ID in js, if present.
func jobByID(js []jobs.Job, id string) (jobs.Job, bool) {
	for _, j := range js {
		if j.ID == id {
			return j, true
		}
	}
	return jobs.Job{}, false
}
