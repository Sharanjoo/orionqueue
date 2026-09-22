package api

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Sharanjoo/orionqueue/internal/api/gen/orionqueue/v1"
	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

// WorkerServer implements pb.WorkerServiceServer on top of a
// workers.Service, the same adapter pattern JobServer established for
// jobs.Service in Phase 2. It also holds a jobs.Service, added in Phase 6
// solely to compose WorkerHeartbeat's response with the jobs a worker
// should pick up and start (see WorkerHeartbeat) — this is the one place
// internal/workers and internal/jobs are bridged; neither package imports
// the other directly.
type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer

	svc    *workers.Service
	jobs   *jobs.Service
	logger *slog.Logger
}

// NewWorkerServer returns a WorkerServer backed by svc and jobSvc.
func NewWorkerServer(svc *workers.Service, jobSvc *jobs.Service, logger *slog.Logger) *WorkerServer {
	return &WorkerServer{svc: svc, jobs: jobSvc, logger: logger}
}

func (s *WorkerServer) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	in := workers.RegisterInput{
		Hostname:            req.GetHostname(),
		CPUCapacity:         req.GetCpuCapacity(),
		MemoryCapacityBytes: req.GetMemoryCapacityBytes(),
		GPUs:                gpusToDomain(req.GetGpus()),
		SoftwareVersion:     req.GetSoftwareVersion(),
		Labels:              req.GetLabels(),
	}

	worker, err := s.svc.Register(ctx, in)
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.RegisterWorkerResponse{
		Worker:                   workerToProto(worker),
		HeartbeatIntervalSeconds: int64(s.svc.HeartbeatInterval().Seconds()),
	}, nil
}

func (s *WorkerServer) WorkerHeartbeat(ctx context.Context, req *pb.WorkerHeartbeatRequest) (*pb.WorkerHeartbeatResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id must not be empty")
	}

	worker, err := s.svc.Heartbeat(ctx, req.GetWorkerId(), gpusToDomain(req.GetGpus()), req.GetRunningJobIds())
	if err != nil {
		return nil, s.toStatus(err)
	}

	resp := &pb.WorkerHeartbeatResponse{Worker: workerToProto(worker)}
	assigned, err := s.jobs.ListAssignedToWorker(ctx, req.GetWorkerId())
	if err != nil {
		// The heartbeat itself (the part that keeps this worker's lease
		// alive) already succeeded and is recorded above — a failure
		// listing assigned jobs shouldn't make the whole heartbeat fail
		// and risk the lease expiring. Log it and return what we have;
		// the worker simply picks up any pending assignment on its next
		// heartbeat instead.
		s.logger.Error("failed to list jobs assigned to worker for heartbeat response",
			slog.String("worker_id", req.GetWorkerId()), slog.String("error", err.Error()))
		return resp, nil
	}
	for _, j := range assigned {
		resp.AssignedJobs = append(resp.AssignedJobs, jobToProto(j))
	}

	stoppable, err := s.jobs.ListStoppableForWorker(ctx, req.GetWorkerId())
	if err != nil {
		// Same reasoning as above: don't fail the heartbeat over this —
		// a missed stop signal is caught on the worker's next heartbeat.
		s.logger.Error("failed to list stoppable jobs for heartbeat response",
			slog.String("worker_id", req.GetWorkerId()), slog.String("error", err.Error()))
		return resp, nil
	}
	for _, j := range stoppable {
		resp.StopJobIds = append(resp.StopJobIds, j.ID)
	}
	return resp, nil
}

func (s *WorkerServer) ListWorkers(ctx context.Context, req *pb.ListWorkersRequest) (*pb.ListWorkersResponse, error) {
	opts := workers.ListOptions{
		PageSize:  req.GetPageSize(),
		PageToken: req.GetPageToken(),
	}
	if req.GetStatusFilter() != pb.WorkerStatus_WORKER_STATUS_UNSPECIFIED {
		opts.StatusFilter = pbWorkerStatusToDomain[req.GetStatusFilter()]
	}

	result, err := s.svc.List(ctx, opts)
	if err != nil {
		return nil, s.toStatus(err)
	}

	resp := &pb.ListWorkersResponse{NextPageToken: result.NextPageToken}
	for _, w := range result.Workers {
		resp.Workers = append(resp.Workers, workerToProto(w))
	}
	return resp, nil
}

// toStatus mirrors JobServer.toStatus's error-code mapping for the
// workers domain's error set.
func (s *WorkerServer) toStatus(err error) error {
	var verr *workers.ValidationError
	switch {
	case errors.As(err, &verr):
		return status.Error(codes.InvalidArgument, verr.Error())
	case errors.Is(err, workers.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, workers.ErrInvalidState):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		s.logger.Error("unhandled internal error", slog.String("error", err.Error()))
		return status.Error(codes.Internal, "internal error")
	}
}
