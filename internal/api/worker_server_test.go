package api

import (
	"context"
	"log/slog"
	"testing"

	"google.golang.org/grpc/codes"

	pb "github.com/Sharanjoo/orionqueue/internal/api/gen/orionqueue/v1"
	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/leases"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

func newTestWorkerServer() (*WorkerServer, *leases.FakeManager, *jobs.Service) {
	repo := workers.NewMemoryRepository()
	fakeLeases := leases.NewFakeManager()
	workerSvc := workers.NewService(repo, fakeLeases)
	jobSvc := jobs.NewService(jobs.NewMemoryRepository())
	logger := slog.New(slog.NewJSONHandler(discardWriter{}, nil))
	return NewWorkerServer(workerSvc, jobSvc, logger), fakeLeases, jobSvc
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
	s, _, _ := newTestWorkerServer()
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
	s, _, _ := newTestWorkerServer()
	_, err := s.RegisterWorker(context.Background(), &pb.RegisterWorkerRequest{})
	assertStatusCode(t, err, codes.InvalidArgument)
}

func TestWorkerHeartbeatEmptyIDReturnsInvalidArgument(t *testing.T) {
	s, _, _ := newTestWorkerServer()
	_, err := s.WorkerHeartbeat(context.Background(), &pb.WorkerHeartbeatRequest{})
	assertStatusCode(t, err, codes.InvalidArgument)
}

func TestWorkerHeartbeatMissingWorkerReturnsNotFound(t *testing.T) {
	s, _, _ := newTestWorkerServer()
	_, err := s.WorkerHeartbeat(context.Background(), &pb.WorkerHeartbeatRequest{WorkerId: "does-not-exist"})
	assertStatusCode(t, err, codes.NotFound)
}

func TestListWorkersFiltersByStatus(t *testing.T) {
	s, fakeLeases, _ := newTestWorkerServer()
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

func TestWorkerHeartbeatReturnsAssignedJobsNotYetStarted(t *testing.T) {
	s, _, jobSvc := newTestWorkerServer()
	ctx := context.Background()

	regResp, err := s.RegisterWorker(ctx, validRegisterWorkerRequest())
	if err != nil {
		t.Fatalf("RegisterWorker returned error: %v", err)
	}
	workerID := regResp.GetWorker().GetId()

	job, err := jobSvc.Submit(ctx, jobs.SubmitInput{Name: "j1", Owner: "o", Image: "img"})
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := jobSvc.AssignToWorkers(ctx, job.ID, []string{workerID}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}

	hbResp, err := s.WorkerHeartbeat(ctx, &pb.WorkerHeartbeatRequest{WorkerId: workerID})
	if err != nil {
		t.Fatalf("WorkerHeartbeat returned error: %v", err)
	}
	if len(hbResp.GetAssignedJobs()) != 1 || hbResp.GetAssignedJobs()[0].GetId() != job.ID {
		t.Fatalf("expected assigned_jobs = [%s], got %+v", job.ID, hbResp.GetAssignedJobs())
	}

	// Once the worker reports it started the job, later heartbeats must
	// not offer it again.
	if _, err := jobSvc.Start(ctx, job.ID); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	hbResp2, err := s.WorkerHeartbeat(ctx, &pb.WorkerHeartbeatRequest{WorkerId: workerID})
	if err != nil {
		t.Fatalf("second WorkerHeartbeat returned error: %v", err)
	}
	if len(hbResp2.GetAssignedJobs()) != 0 {
		t.Errorf("expected no assigned_jobs after the job was started, got %+v", hbResp2.GetAssignedJobs())
	}
}

func TestWorkerHeartbeatReturnsStopJobIdsForCancelRequestedAndPreempted(t *testing.T) {
	s, _, jobSvc := newTestWorkerServer()
	ctx := context.Background()

	regResp, err := s.RegisterWorker(ctx, validRegisterWorkerRequest())
	if err != nil {
		t.Fatalf("RegisterWorker returned error: %v", err)
	}
	workerID := regResp.GetWorker().GetId()

	cancelling, err := jobSvc.Submit(ctx, jobs.SubmitInput{Name: "j1", Owner: "o", Image: "img"})
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := jobSvc.AssignToWorkers(ctx, cancelling.ID, []string{workerID}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}
	if _, err := jobSvc.Start(ctx, cancelling.ID); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if _, err := jobSvc.Cancel(ctx, cancelling.ID); err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}

	preempting, err := jobSvc.Submit(ctx, jobs.SubmitInput{Name: "j2", Owner: "o", Image: "img", Preemptible: true})
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if _, err := jobSvc.AssignToWorkers(ctx, preempting.ID, []string{workerID}); err != nil {
		t.Fatalf("AssignToWorkers returned error: %v", err)
	}
	if _, err := jobSvc.Start(ctx, preempting.ID); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if _, err := jobSvc.Preempt(ctx, preempting.ID, "testing"); err != nil {
		t.Fatalf("Preempt returned error: %v", err)
	}

	hbResp, err := s.WorkerHeartbeat(ctx, &pb.WorkerHeartbeatRequest{WorkerId: workerID})
	if err != nil {
		t.Fatalf("WorkerHeartbeat returned error: %v", err)
	}
	stopIDs := hbResp.GetStopJobIds()
	if len(stopIDs) != 2 {
		t.Fatalf("expected 2 stop_job_ids, got %v", stopIDs)
	}
	got := map[string]bool{stopIDs[0]: true, stopIDs[1]: true}
	if !got[cancelling.ID] || !got[preempting.ID] {
		t.Errorf("expected stop_job_ids to contain both %s and %s, got %v", cancelling.ID, preempting.ID, stopIDs)
	}
}
