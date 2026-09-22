// Package workers defines OrionQueue's Worker/GPU domain types and
// business logic (registration, heartbeat, automatic loss detection via
// etcd lease expiration) — independent of the wire format (protobuf,
// internal/api) and of storage (internal/persistence, Phase 3's pattern
// applied here too). See docs/architecture/system-overview.md.
package workers

import "time"

// Status is a worker's liveness state. Phase 4 only ever produces ACTIVE
// and LOST — a SUSPECT state (heartbeat running late but the etcd lease
// hasn't expired yet) is a documented possible refinement for a later
// phase, not implemented now.
type Status string

const (
	StatusActive Status = "ACTIVE"
	StatusLost   Status = "LOST"
)

// GPUHealthState is a GPU device's reported health.
type GPUHealthState string

const (
	GPUHealthy   GPUHealthState = "HEALTHY"
	GPUDegraded  GPUHealthState = "DEGRADED"
	GPUUnhealthy GPUHealthState = "UNHEALTHY"
)

// GPU is one device (real or, in fake-GPU mode, simulated — see
// ADR-0003) in a worker's inventory.
type GPU struct {
	ID                   string
	DeviceIndex          int32
	UUID                 string
	TotalMemoryBytes     int64
	AllocatedMemoryBytes int64
	UtilizationPercent   float64
	// TemperatureCelsius is nil when unavailable — fake-GPU mode always
	// leaves this nil; real NVML mode sets it from Phase 9.
	TemperatureCelsius *float64
	MIGProfile         string
	HealthState        GPUHealthState
}

// Worker is OrionQueue's internal worker record.
type Worker struct {
	ID       string
	Hostname string
	Status   Status
	// LeaseID is the etcd lease backing this worker's liveness key. Nil
	// once the worker is LOST — the lease that key was bound to has
	// already expired in etcd by then.
	LeaseID *int64

	CPUCapacity         float64
	MemoryCapacityBytes int64
	GPUs                []GPU

	SoftwareVersion string
	Labels          map[string]string
	RunningJobIDs   []string

	RegisteredAt    time.Time
	LastHeartbeatAt time.Time
}
