// Package api adapts internal/jobs' transport-agnostic business logic to
// the generated gRPC service interface (internal/api/gen/orionqueue/v1),
// and wires the REST gateway on top of it. internal/jobs never imports
// generated protobuf types; every conversion happens in this package.
package api

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Sharanjoo/orionqueue/internal/api/gen/orionqueue/v1"
	"github.com/Sharanjoo/orionqueue/internal/jobs"
)

// JobServer implements pb.JobServiceServer on top of a jobs.Service.
type JobServer struct {
	pb.UnimplementedJobServiceServer

	svc    *jobs.Service
	logger *slog.Logger
}

// NewJobServer returns a JobServer backed by svc. logger is used only to
// record unexpected (non-domain) errors server-side — client-facing error
// messages never include more than toStatus decides to expose.
func NewJobServer(svc *jobs.Service, logger *slog.Logger) *JobServer {
	return &JobServer{svc: svc, logger: logger}
}

func (s *JobServer) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	in := jobs.SubmitInput{
		Name:                      req.GetName(),
		Owner:                     req.GetOwner(),
		Image:                     req.GetImage(),
		Command:                   req.GetCommand(),
		Resources:                 resourcesToDomain(req.GetResources()),
		Priority:                  req.GetPriority(),
		Preemptible:               req.GetPreemptible(),
		RetryLimit:                req.GetRetryLimit(),
		TimeoutSeconds:            req.GetTimeoutSeconds(),
		CheckpointIntervalSeconds: req.GetCheckpointIntervalSeconds(),
		SubmissionID:              req.GetSubmissionId(),
	}

	job, err := s.svc.Submit(ctx, in)
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.SubmitJobResponse{Job: jobToProto(job)}, nil
}

func (s *JobServer) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id must not be empty")
	}
	job, err := s.svc.Get(ctx, req.GetId())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.GetJobResponse{Job: jobToProto(job)}, nil
}

func (s *JobServer) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	opts := jobs.ListOptions{
		PageSize:  req.GetPageSize(),
		PageToken: req.GetPageToken(),
	}
	if req.GetStateFilter() != pb.JobState_JOB_STATE_UNSPECIFIED {
		opts.StateFilter = pbStateToDomain[req.GetStateFilter()]
	}

	result, err := s.svc.List(ctx, opts)
	if err != nil {
		return nil, s.toStatus(err)
	}

	resp := &pb.ListJobsResponse{NextPageToken: result.NextPageToken}
	for _, j := range result.Jobs {
		resp.Jobs = append(resp.Jobs, jobToProto(j))
	}
	return resp, nil
}

func (s *JobServer) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id must not be empty")
	}
	job, err := s.svc.Cancel(ctx, req.GetId())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.CancelJobResponse{Job: jobToProto(job)}, nil
}

func (s *JobServer) RetryJob(ctx context.Context, req *pb.RetryJobRequest) (*pb.RetryJobResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id must not be empty")
	}
	job, err := s.svc.Retry(ctx, req.GetId())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.RetryJobResponse{Job: jobToProto(job)}, nil
}

func (s *JobServer) ReportJobStarted(ctx context.Context, req *pb.ReportJobStartedRequest) (*pb.ReportJobStartedResponse, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id must not be empty")
	}
	job, err := s.svc.Start(ctx, req.GetJobId())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.ReportJobStartedResponse{Job: jobToProto(job)}, nil
}

func (s *JobServer) ReportJobCompleted(ctx context.Context, req *pb.ReportJobCompletedRequest) (*pb.ReportJobCompletedResponse, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id must not be empty")
	}
	job, err := s.svc.Complete(ctx, req.GetJobId())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.ReportJobCompletedResponse{Job: jobToProto(job)}, nil
}

func (s *JobServer) ReportJobFailed(ctx context.Context, req *pb.ReportJobFailedRequest) (*pb.ReportJobFailedResponse, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id must not be empty")
	}
	job, err := s.svc.Fail(ctx, req.GetJobId(), req.GetFailureReason())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.ReportJobFailedResponse{Job: jobToProto(job)}, nil
}

// toStatus maps a jobs-package domain error to the gRPC status it should
// surface as. grpc-gateway maps these codes to HTTP statuses automatically
// (InvalidArgument->400, NotFound->404, FailedPrecondition->409,
// Internal->500), which is what gives REST clients "consistent error
// responses" without hand-written HTTP error mapping.
func (s *JobServer) toStatus(err error) error {
	var verr *jobs.ValidationError
	switch {
	case errors.As(err, &verr):
		return status.Error(codes.InvalidArgument, verr.Error())
	case errors.Is(err, jobs.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, jobs.ErrInvalidState):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, jobs.ErrRetryLimitExceeded):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		// An error we don't recognize is a bug, not a client mistake —
		// log the real cause server-side but don't leak internal details
		// to the caller.
		s.logger.Error("unhandled internal error", slog.String("error", err.Error()))
		return status.Error(codes.Internal, "internal error")
	}
}
