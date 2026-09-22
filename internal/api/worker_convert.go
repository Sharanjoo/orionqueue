package api

import (
	pb "github.com/Sharanjoo/orionqueue/internal/api/gen/orionqueue/v1"
	"github.com/Sharanjoo/orionqueue/internal/workers"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var pbWorkerStatusToDomain = map[pb.WorkerStatus]workers.Status{
	pb.WorkerStatus_WORKER_STATUS_ACTIVE: workers.StatusActive,
	pb.WorkerStatus_WORKER_STATUS_LOST:   workers.StatusLost,
}

var domainWorkerStatusToPB = invertWorkerStatus(pbWorkerStatusToDomain)

func invertWorkerStatus(m map[pb.WorkerStatus]workers.Status) map[workers.Status]pb.WorkerStatus {
	out := make(map[workers.Status]pb.WorkerStatus, len(m))
	for k, v := range m {
		out[v] = k
	}
	return out
}

var pbHealthStateToDomain = map[pb.GPUHealthState]workers.GPUHealthState{
	pb.GPUHealthState_GPU_HEALTH_STATE_HEALTHY:   workers.GPUHealthy,
	pb.GPUHealthState_GPU_HEALTH_STATE_DEGRADED:  workers.GPUDegraded,
	pb.GPUHealthState_GPU_HEALTH_STATE_UNHEALTHY: workers.GPUUnhealthy,
}

var domainHealthStateToPB = invertHealthState(pbHealthStateToDomain)

func invertHealthState(m map[pb.GPUHealthState]workers.GPUHealthState) map[workers.GPUHealthState]pb.GPUHealthState {
	out := make(map[workers.GPUHealthState]pb.GPUHealthState, len(m))
	for k, v := range m {
		out[v] = k
	}
	return out
}

func gpuToDomain(g *pb.GPU) workers.GPU {
	if g == nil {
		return workers.GPU{}
	}
	out := workers.GPU{
		DeviceIndex:          g.GetDeviceIndex(),
		UUID:                 g.GetUuid(),
		TotalMemoryBytes:     g.GetTotalMemoryBytes(),
		AllocatedMemoryBytes: g.GetAllocatedMemoryBytes(),
		UtilizationPercent:   g.GetUtilizationPercent(),
		MIGProfile:           g.GetMigProfile(),
	}
	if state, ok := pbHealthStateToDomain[g.GetHealthState()]; ok {
		out.HealthState = state
	} else {
		out.HealthState = workers.GPUHealthy // default: unspecified on the wire means healthy
	}
	if g.TemperatureCelsius != nil {
		t := *g.TemperatureCelsius
		out.TemperatureCelsius = &t
	}
	return out
}

func gpusToDomain(gs []*pb.GPU) []workers.GPU {
	if gs == nil {
		return nil
	}
	out := make([]workers.GPU, len(gs))
	for i, g := range gs {
		out[i] = gpuToDomain(g)
	}
	return out
}

func gpuToProto(g workers.GPU) *pb.GPU {
	out := &pb.GPU{
		Id:                   g.ID,
		DeviceIndex:          g.DeviceIndex,
		Uuid:                 g.UUID,
		TotalMemoryBytes:     g.TotalMemoryBytes,
		AllocatedMemoryBytes: g.AllocatedMemoryBytes,
		UtilizationPercent:   g.UtilizationPercent,
		MigProfile:           g.MIGProfile,
		HealthState:          domainHealthStateToPB[g.HealthState],
	}
	if g.TemperatureCelsius != nil {
		t := *g.TemperatureCelsius
		out.TemperatureCelsius = &t
	}
	return out
}

func workerToProto(w workers.Worker) *pb.Worker {
	out := &pb.Worker{
		Id:                  w.ID,
		Hostname:            w.Hostname,
		Status:              domainWorkerStatusToPB[w.Status],
		CpuCapacity:         w.CPUCapacity,
		MemoryCapacityBytes: w.MemoryCapacityBytes,
		SoftwareVersion:     w.SoftwareVersion,
		Labels:              w.Labels,
		RunningJobIds:       w.RunningJobIDs,
		RegisteredAt:        timestamppb.New(w.RegisteredAt),
		LastHeartbeatAt:     timestamppb.New(w.LastHeartbeatAt),
	}
	for _, g := range w.GPUs {
		out.Gpus = append(out.Gpus, gpuToProto(g))
	}
	return out
}
