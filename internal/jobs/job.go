// Package jobs defines OrionQueue's core Job domain type, state machine,
// and business logic (validation, submission, cancellation, retry) —
// independent of the wire format (protobuf) used by internal/api and of
// how records are stored (internal/persistence, added Phase 3). Keeping
// this layer free of both lets it be unit-tested directly, with no gRPC
// or database involved.
package jobs

import "time"

// State is a Job's position in its lifecycle. See
// docs/architecture/system-overview.md#4-domain-model-summary for the full
// state diagram.
type State string

const (
	StateQueued          State = "QUEUED"
	StateScheduled       State = "SCHEDULED"
	StateRunning         State = "RUNNING"
	StateCheckpointing   State = "CHECKPOINTING"
	StateSucceeded       State = "SUCCEEDED"
	StateFailed          State = "FAILED"
	StateRetrying        State = "RETRYING"
	StateCancelRequested State = "CANCEL_REQUESTED"
	StateCancelled       State = "CANCELLED"
	StatePreempted       State = "PREEMPTED"
	StateLost            State = "LOST"
)

// Terminal reports whether a job in this state will never transition again
// without an explicit new action from the user (e.g. RetryJob) — used by
// Cancel to make cancellation of an already-finished job a safe no-op
// instead of an error.
func (s State) Terminal() bool {
	switch s {
	case StateSucceeded, StateFailed, StateCancelled:
		return true
	default:
		return false
	}
}

// ResourceRequest describes the compute a job needs. Eligibility checking
// against actual worker/GPU capacity is the scheduler's job (Phase 5) —
// this struct only carries the request.
type ResourceRequest struct {
	GPUCount          uint32
	MinGPUMemoryBytes int64
	CPUCores          float64
	MemoryBytes       int64
}

// Job is OrionQueue's internal job record — the type the API server,
// scheduler, and persistence layer all operate on. internal/api's gRPC
// handlers translate between this and the generated protobuf Job message
// at the transport boundary; nothing below internal/api ever imports
// generated protobuf types.
type Job struct {
	ID   string
	Name string
	// SubmissionID is an optional client-supplied idempotency key: a
	// second SubmitJob call with the same non-empty SubmissionID returns
	// the original job instead of creating a duplicate.
	SubmissionID string

	Owner   string
	Image   string
	Command []string

	Resources   ResourceRequest
	Priority    int32
	Preemptible bool

	RetryLimit     int32
	CurrentAttempt int32

	TimeoutSeconds            int64
	CheckpointIntervalSeconds int64

	State             State
	AssignedWorkerIDs []string
	FailureReason     string

	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	FailedAt    *time.Time
}
