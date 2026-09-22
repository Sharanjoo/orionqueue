package jobs

import (
	"fmt"
	"strings"
)

// Limits enforced on job submission. These are sanity ceilings on the
// shape of the request, not cluster-capacity checks — "can this actually
// fit the cluster" is the scheduler's job (Phase 5).
const (
	MaxNameLength         = 253
	MaxOwnerLength        = 253
	MaxCommandArgs        = 256
	MaxCommandArgLength   = 4096
	MaxSubmissionIDLength = 256
	MaxGPUCount           = 64
	MaxPriority           = 100
	MaxRetryLimit         = 100
)

// SubmitInput is the validated set of fields accepted from a SubmitJob
// request, kept separate from Job so validation runs once at the boundary
// and every field the rest of the system relies on has already been
// checked.
type SubmitInput struct {
	Name    string
	Owner   string
	Image   string
	Command []string

	Resources   ResourceRequest
	Priority    int32
	Preemptible bool

	RetryLimit                int32
	TimeoutSeconds            int64
	CheckpointIntervalSeconds int64

	SubmissionID string
}

// ValidationError reports every problem found with a request at once, so a
// client can fix them all in one round trip instead of one error at a
// time.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid request: %s", strings.Join(e.Problems, "; "))
}

// ValidateSubmit checks whether in is well-formed. It never touches
// storage or the scheduler — it's pure validation of the request shape.
func ValidateSubmit(in SubmitInput) error {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	if strings.TrimSpace(in.Name) == "" {
		add("name must not be empty")
	} else if len(in.Name) > MaxNameLength {
		add("name must be at most %d characters", MaxNameLength)
	}

	if strings.TrimSpace(in.Owner) == "" {
		add("owner must not be empty")
	} else if len(in.Owner) > MaxOwnerLength {
		add("owner must be at most %d characters", MaxOwnerLength)
	}

	if strings.TrimSpace(in.Image) == "" {
		add("image must not be empty")
	}

	if len(in.Command) > MaxCommandArgs {
		add("command must have at most %d arguments", MaxCommandArgs)
	}
	for i, arg := range in.Command {
		if len(arg) > MaxCommandArgLength {
			add("command[%d] exceeds max length %d", i, MaxCommandArgLength)
			break
		}
	}

	if in.Resources.GPUCount > MaxGPUCount {
		add("gpu_count must be at most %d", MaxGPUCount)
	}
	if in.Resources.MinGPUMemoryBytes < 0 {
		add("min_gpu_memory_bytes must not be negative")
	}
	if in.Resources.CPUCores < 0 {
		add("cpu_cores must not be negative")
	}
	if in.Resources.MemoryBytes < 0 {
		add("memory_bytes must not be negative")
	}

	if in.Priority < 0 || in.Priority > MaxPriority {
		add("priority must be between 0 and %d", MaxPriority)
	}

	if in.RetryLimit < 0 {
		add("retry_limit must not be negative")
	} else if in.RetryLimit > MaxRetryLimit {
		add("retry_limit must be at most %d", MaxRetryLimit)
	}

	if in.TimeoutSeconds < 0 {
		add("timeout_seconds must not be negative")
	}
	if in.CheckpointIntervalSeconds < 0 {
		add("checkpoint_interval_seconds must not be negative")
	}

	if len(in.SubmissionID) > MaxSubmissionIDLength {
		add("submission_id must be at most %d characters", MaxSubmissionIDLength)
	}

	if len(problems) == 0 {
		return nil
	}
	return &ValidationError{Problems: problems}
}
