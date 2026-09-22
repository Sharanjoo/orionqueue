package api

import (
	"context"
	"log/slog"
	"testing"

	"google.golang.org/grpc/codes"

	pb "github.com/Sharanjoo/orionqueue/internal/api/gen/orionqueue/v1"
	"github.com/Sharanjoo/orionqueue/internal/leases"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

func newTestWorkerServer() (*WorkerServer, *leases.FakeManager) {
	repo := workers.NewMemoryRepository()
	fakeLeases := leases.NewFakeManager()
	svc := workers.NewService(repo, fakeLeases)
	logger := slog.New(slog.NewJSONHandler(discardWriter{}, nil))
	return NewWorkerServer(svc, logger), fakeLeases
}

func validRegisterWorkerRequest() *pb.RegisterWorkerRequest {
	return &pb.RegisterWorkerRequest{
		Hostname:            "worker-1.local",
		CpuCapacity:         8,
		MemoryCapacityBytes: 32 << 30,
		Gpus: []*pb.GPU{
			{DeviceIndex: 0, Uuid: "GPU-fake-0", TotalMemoryBytes: 16 << 30, HealthState: pb.GPUHealthState_GPU_HEALTH_STATE_HEALTHY},
		},
		SoftwareVersion: "0.1.0",
		Labels:          map[string]string{"env": "test"},
	}
}

func TestRegisterWorkerAndHeartbeat(t *testing.T) {
	s, _ := newTestWorkerServer()
	ctx := context.Background()

	regResp, err := s.RegisterWorker(ctx, validRegisterWorkerRequest())
	if err != nil {
		t.Fatalf("RegisterWorker returned error: %v", err)
	}
	if regResp.GetWorker().GetStatus() != pb.WorkerStatus_WORKER_STATUS_ACTIVE {
		t.Errorf("Status = %v, want WORKER_STATUS_ACTIVE", regResp.GetWorker().GetStatus())
	}
	if regResp.GetHeartbeatIntervalSeconds() <= 0 {
		t.Errorf("HeartbeatIntervalSeconds = %d, want > 0", regResp.GetHeartbeatIntervalSeconds())
	}
	if len(regResp.GetWorker().GetGpus()) != 1 {
		t.Fatalf("expected 1 GPU, got %d", len(regResp.GetWorker().GetGpus()))
	}

	hbResp, err := s.WorkerHeartbeat(ctx, &pb.WorkerHeartbeatRequest{
		WorkerId: regResp.GetWorker().GetId(),
		Gpus: []*pb.GPU{
			{DeviceIndex: 0, Uuid: "GPU-fake-0", TotalMemoryBytes: 16 << 30, UtilizationPercent: 55},
		},
		RunningJobIds: []string{"job-1"},
	})
	if err != nil {
		t.Fatalf("WorkerHeartbeat returned error: %v", err)
	}
	if hbResp.GetWorker().GetGpus()[0].GetUtilizationPercent() != 55 {
		t.Errorf("utilization_percent = %v, want 55", hbResp.GetWorker().GetGpus()[0].GetUtilizationPercent())
	}
	if len(hbResp.GetWorker().GetRunningJobIds()) != 1 || hbResp.GetWorker().GetRunningJobIds()[0] != "job-1" {
		t.Errorf("running_job_ids = %v, want [job-1]", hbResp.GetWorker().GetRunningJobIds())
	}
}

func TestRegisterWorkerRejectsInvalidRequest(t *testing.T) {
	s, _ := newTestWorkerServer()
	_, err := s.RegisterWorker(context.Background(), &pb.RegisterWorkerRequest{})
	assertStatusCode(t, err, codes.InvalidArgument)
}

func TestWorkerHeartbeatEmptyIDReturnsInvalidArgument(t *testing.T) {
	s, _ := newTestWorkerServer()
	_, err := s.WorkerHeartbeat(context.Background(), &pb.WorkerHeartbeatRequest{})
	assertStatusCode(t, err, codes.InvalidArgument)
}

func TestWorkerHeartbeatMissingWorkerReturnsNotFound(t *testing.T) {
	s, _ := newTestWorkerServer()
	_, err := s.WorkerHeartbeat(context.Background(), &pb.WorkerHeartbeatRequest{WorkerId: "does-not-exist"})
	assertStatusCode(t, err, codes.NotFound)
}

func TestListWorkersFiltersByStatus(t *testing.T) {
	s, fakeLeases := newTestWorkerServer()
	ctx := context.Background()

	regResp, err := s.RegisterWorker(ctx, validRegisterWorkerRequest())
	if err != nil {
		t.Fatalf("RegisterWorker returned error: %v", err)
	}

	req2 := validRegisterWorkerRequest()
	req2.Hostname = "worker-2.local"
	if _, err := s.RegisterWorker(ctx, req2); err != nil {
		t.Fatalf("second RegisterWorker returned error: %v", err)
	}

	// Simulate the first worker's lease expiring without a heartbeat.
	w, err := s.svc.Get(ctx, regResp.GetWorker().GetId())
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	fakeLeases.Expire(*w.LeaseID)
	if _, err := s.svc.MarkLost(ctx, w.ID); err != nil {
		t.Fatalf("MarkLost returned error: %v", err)
	}

	lost, err := s.ListWorkers(ctx, &pb.ListWorkersRequest{StatusFilter: pb.WorkerStatus_WORKER_STATUS_LOST})
	if err != nil {
		t.Fatalf("ListWorkers returned error: %v", err)
	}
	if len(lost.GetWorkers()) != 1 || lost.GetWorkers()[0].GetHostname() != "worker-1.local" {
		t.Fatalf("expected only worker-1.local (LOST), got %+v", lost.GetWorkers())
	}

	active, err := s.ListWorkers(ctx, &pb.ListWorkersRequest{StatusFilter: pb.WorkerStatus_WORKER_STATUS_ACTIVE})
	if err != nil {
		t.Fatalf("ListWorkers returned error: %v", err)
	}
	if len(active.GetWorkers()) != 1 || active.GetWorkers()[0].GetHostname() != "worker-2.local" {
		t.Fatalf("expected only worker-2.local (ACTIVE), got %+v", active.GetWorkers())
	}
}
