package api

import (
	pb "github.com/Sharanjoo/orionqueue/internal/api/gen/orionqueue/v1"
	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// pbStateToDomain and its inverse are the single source of truth for the
// JobState wire enum <-> jobs.State mapping, so both directions stay in
// sync by construction instead of by convention.
var pbStateToDomain = map[pb.JobState]jobs.State{
	pb.JobState_JOB_STATE_QUEUED:           jobs.StateQueued,
	pb.JobState_JOB_STATE_SCHEDULED:        jobs.StateScheduled,
	pb.JobState_JOB_STATE_RUNNING:          jobs.StateRunning,
	pb.JobState_JOB_STATE_CHECKPOINTING:    jobs.StateCheckpointing,
	pb.JobState_JOB_STATE_SUCCEEDED:        jobs.StateSucceeded,
	pb.JobState_JOB_STATE_FAILED:           jobs.StateFailed,
	pb.JobState_JOB_STATE_RETRYING:         jobs.StateRetrying,
	pb.JobState_JOB_STATE_CANCEL_REQUESTED: jobs.StateCancelRequested,
	pb.JobState_JOB_STATE_CANCELLED:        jobs.StateCancelled,
	pb.JobState_JOB_STATE_PREEMPTED:        jobs.StatePreempted,
	pb.JobState_JOB_STATE_LOST:             jobs.StateLost,
}

var domainStateToPB = invert(pbStateToDomain)

func invert(m map[pb.JobState]jobs.State) map[jobs.State]pb.JobState {
	out := make(map[jobs.State]pb.JobState, len(m))
	for k, v := range m {
		out[v] = k
	}
	return out
}

// resourcesToDomain converts a possibly-nil ResourceRequest; SubmitJob
// callers that omit resources entirely get the zero value, which
// ValidateSubmit's gpu_count/memory checks handle the same as any other
// input.
func resourcesToDomain(r *pb.ResourceRequest) jobs.ResourceRequest {
	if r == nil {
		return jobs.ResourceRequest{}
	}
	return jobs.ResourceRequest{
		GPUCount:          r.GetGpuCount(),
		MinGPUMemoryBytes: r.GetMinGpuMemoryBytes(),
		CPUCores:          r.GetCpuCores(),
		MemoryBytes:       r.GetMemoryBytes(),
	}
}

func resourcesToProto(r jobs.ResourceRequest) *pb.ResourceRequest {
	return &pb.ResourceRequest{
		GpuCount:          r.GPUCount,
		MinGpuMemoryBytes: r.MinGPUMemoryBytes,
		CpuCores:          r.CPUCores,
		MemoryBytes:       r.MemoryBytes,
	}
}

// jobToProto converts a domain Job to its wire representation. Unset
// (nil) optional timestamps stay nil on the wire rather than being
// rendered as a zero-value Timestamp, so a client can distinguish "not
// started yet" from "started at the Unix epoch."
func jobToProto(j jobs.Job) *pb.Job {
	out := &pb.Job{
		Id:                        j.ID,
		Name:                      j.Name,
		Owner:                     j.Owner,
		Image:                     j.Image,
		Command:                   j.Command,
		Resources:                 resourcesToProto(j.Resources),
		Priority:                  j.Priority,
		Preemptible:               j.Preemptible,
		RetryLimit:                j.RetryLimit,
		CurrentAttempt:            j.CurrentAttempt,
		TimeoutSeconds:            j.TimeoutSeconds,
		CheckpointIntervalSeconds: j.CheckpointIntervalSeconds,
		State:                     domainStateToPB[j.State],
		AssignedWorkerIds:         j.AssignedWorkerIDs,
		FailureReason:             j.FailureReason,
		CreatedAt:                 timestamppb.New(j.CreatedAt),
	}
	if j.StartedAt != nil {
		out.StartedAt = timestamppb.New(*j.StartedAt)
	}
	if j.CompletedAt != nil {
		out.CompletedAt = timestamppb.New(*j.CompletedAt)
	}
	if j.FailedAt != nil {
		out.FailedAt = timestamppb.New(*j.FailedAt)
	}
	return out
}
