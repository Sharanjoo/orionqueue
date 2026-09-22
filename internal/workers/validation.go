package workers

import (
	"fmt"
	"strings"
)

const (
	MaxHostnameLength = 253
	MaxGPUCount       = 64
)

// RegisterInput is the validated set of fields accepted from a
// RegisterWorker request.
type RegisterInput struct {
	Hostname            string
	CPUCapacity         float64
	MemoryCapacityBytes int64
	GPUs                []GPU
	SoftwareVersion     string
	Labels              map[string]string
}

// ValidationError reports every problem found with a request at once.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid request: %s", strings.Join(e.Problems, "; "))
}

// ValidateRegister checks whether in is well-formed.
func ValidateRegister(in RegisterInput) error {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	if strings.TrimSpace(in.Hostname) == "" {
		add("hostname must not be empty")
	} else if len(in.Hostname) > MaxHostnameLength {
		add("hostname must be at most %d characters", MaxHostnameLength)
	}

	if in.CPUCapacity < 0 {
		add("cpu_capacity must not be negative")
	}
	if in.MemoryCapacityBytes < 0 {
		add("memory_capacity_bytes must not be negative")
	}

	if len(in.GPUs) > MaxGPUCount {
		add("gpus must have at most %d entries", MaxGPUCount)
	}
	seenIndex := make(map[int32]bool, len(in.GPUs))
	for i, g := range in.GPUs {
		if g.DeviceIndex < 0 {
			add("gpus[%d].device_index must not be negative", i)
		}
		if seenIndex[g.DeviceIndex] {
			add("gpus[%d].device_index %d is a duplicate", i, g.DeviceIndex)
		}
		seenIndex[g.DeviceIndex] = true
		if g.TotalMemoryBytes < 0 {
			add("gpus[%d].total_memory_bytes must not be negative", i)
		}
		if g.AllocatedMemoryBytes < 0 {
			add("gpus[%d].allocated_memory_bytes must not be negative", i)
		}
		if g.AllocatedMemoryBytes > g.TotalMemoryBytes {
			add("gpus[%d].allocated_memory_bytes must not exceed total_memory_bytes", i)
		}
	}

	if len(problems) == 0 {
		return nil
	}
	return &ValidationError{Problems: problems}
}
