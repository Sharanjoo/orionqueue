package api

import (
	"context"
	"log/slog"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Sharanjoo/orionqueue/internal/api/gen/orionqueue/v1"
	"github.com/Sharanjoo/orionqueue/internal/jobs"
)

func newTestServer() *JobServer {
	repo := jobs.NewMemoryRepository()
	svc := jobs.NewService(repo)
	logger := slog.New(slog.NewJSONHandler(discardWriter{}, nil))
	return NewJobServer(svc, logger)
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func validSubmitRequest() *pb.SubmitJobRequest {
	return &pb.SubmitJobRequest{
		Name:  "train-resnet",
		Owner: "sharan",
		Image: "orionqueue/fake-gpu-job:latest",
		Resources: &pb.ResourceRequest{
			GpuCount:          1,
			MinGpuMemoryBytes: 8 << 30,
			CpuCores:          2,
			MemoryBytes:       4 << 30,
		},
		Priority:   50,
		RetryLimit: 3,
	}
}

func TestSubmitJobAndGetJobRoundTrip(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	submitResp, err := s.SubmitJob(ctx, validSubmitRequest())
	if err != nil {
		t.Fatalf("SubmitJob returned error: %v", err)
	}
	if submitResp.GetJob().GetState() != pb.JobState_JOB_STATE_QUEUED {
		t.Errorf("State = %v, want JOB_STATE_QUEUED", submitResp.GetJob().GetState())
	}
	if submitResp.GetJob().GetId() == "" {
		t.Fatal("expected a non-empty job ID")
	}

	getResp, err := s.GetJob(ctx, &pb.GetJobRequest{Id: submitResp.GetJob().GetId()})
	if err != nil {
		t.Fatalf("GetJob returned error: %v", err)
	}
	if getResp.GetJob().GetName() != "train-resnet" {
		t.Errorf("Name = %q, want %q", getResp.GetJob().GetName(), "train-resnet")
	}
	if getResp.GetJob().GetResources().GetGpuCount() != 1 {
		t.Errorf("GpuCount = %d, want 1", getResp.GetJob().GetResources().GetGpuCount())
	}
}

func TestSubmitJobRejectsInvalidRequestAsInvalidArgument(t *testing.T) {
	s := newTestServer()
	_, err := s.SubmitJob(context.Background(), &pb.SubmitJobRequest{})
	assertStatusCode(t, err, codes.InvalidArgument)
}

func TestSubmitJobIsIdempotentBySubmissionID(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()
	req := validSubmitRequest()
	req.SubmissionId = "req-42"

	first, err := s.SubmitJob(ctx, req)
	if err != nil {
		t.Fatalf("first SubmitJob returned error: %v", err)
	}
	second, err := s.SubmitJob(ctx, req)
	if err != nil {
		t.Fatalf("second SubmitJob returned error: %v", err)
	}
	if second.GetJob().GetId() != first.GetJob().GetId() {
		t.Errorf("expected the same job ID for a repeated submission_id, got %q then %q", first.GetJob().GetId(), second.GetJob().GetId())
	}

	list, err := s.ListJobs(ctx, &pb.ListJobsRequest{})
	if err != nil {
		t.Fatalf("ListJobs returned error: %v", err)
	}
	if len(list.GetJobs()) != 1 {
		t.Fatalf("expected exactly 1 job after 2 submits with the same submission_id, got %d", len(list.GetJobs()))
	}
}

func TestGetJobMissingReturnsNotFound(t *testing.T) {
	s := newTestServer()
	_, err := s.GetJob(context.Background(), &pb.GetJobRequest{Id: "does-not-exist"})
	assertStatusCode(t, err, codes.NotFound)
}

func TestGetJobEmptyIDReturnsInvalidArgument(t *testing.T) {
	s := newTestServer()
	_, err := s.GetJob(context.Background(), &pb.GetJobRequest{Id: ""})
	assertStatusCode(t, err, codes.InvalidArgument)
}

func TestListJobsFiltersByStateEnum(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	submitResp, err := s.SubmitJob(ctx, validSubmitRequest())
	if err != nil {
		t.Fatalf("SubmitJob returned error: %v", err)
	}
	if _, err := s.CancelJob(ctx, &pb.CancelJobRequest{Id: submitResp.GetJob().GetId()}); err != nil {
		t.Fatalf("CancelJob returned error: %v", err)
	}

	req2 := validSubmitRequest()
	req2.Name = "still-queued"
	if _, err := s.SubmitJob(ctx, req2); err != nil {
		t.Fatalf("second SubmitJob returned error: %v", err)
	}

	list, err := s.ListJobs(ctx, &pb.ListJobsRequest{StateFilter: pb.JobState_JOB_STATE_CANCELLED})
	if err != nil {
		t.Fatalf("ListJobs returned error: %v", err)
	}
	if len(list.GetJobs()) != 1 {
		t.Fatalf("expected exactly 1 CANCELLED job, got %d", len(list.GetJobs()))
	}
	if list.GetJobs()[0].GetState() != pb.JobState_JOB_STATE_CANCELLED {
		t.Errorf("State = %v, want JOB_STATE_CANCELLED", list.GetJobs()[0].GetState())
	}
}

func TestCancelJobQueuedSucceeds(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	submitResp, err := s.SubmitJob(ctx, validSubmitRequest())
	if err != nil {
		t.Fatalf("SubmitJob returned error: %v", err)
	}

	cancelResp, err := s.CancelJob(ctx, &pb.CancelJobRequest{Id: submitResp.GetJob().GetId()})
	if err != nil {
		t.Fatalf("CancelJob returned error: %v", err)
	}
	if cancelResp.GetJob().GetState() != pb.JobState_JOB_STATE_CANCELLED {
		t.Errorf("State = %v, want JOB_STATE_CANCELLED", cancelResp.GetJob().GetState())
	}
}

func TestCancelJobIsIdempotent(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	submitResp, err := s.SubmitJob(ctx, validSubmitRequest())
	if err != nil {
		t.Fatalf("SubmitJob returned error: %v", err)
	}
	id := submitResp.GetJob().GetId()

	if _, err := s.CancelJob(ctx, &pb.CancelJobRequest{Id: id}); err != nil {
		t.Fatalf("first CancelJob returned error: %v", err)
	}
	second, err := s.CancelJob(ctx, &pb.CancelJobRequest{Id: id})
	if err != nil {
		t.Fatalf("second CancelJob on an already-cancelled job returned an error, want idempotent success: %v", err)
	}
	if second.GetJob().GetState() != pb.JobState_JOB_STATE_CANCELLED {
		t.Errorf("State = %v, want JOB_STATE_CANCELLED", second.GetJob().GetState())
	}
}

func TestCancelJobMissingReturnsNotFound(t *testing.T) {
	s := newTestServer()
	_, err := s.CancelJob(context.Background(), &pb.CancelJobRequest{Id: "does-not-exist"})
	assertStatusCode(t, err, codes.NotFound)
}

func TestRetryJobOnNonFailedJobReturnsFailedPrecondition(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	submitResp, err := s.SubmitJob(ctx, validSubmitRequest())
	if err != nil {
		t.Fatalf("SubmitJob returned error: %v", err)
	}

	_, err = s.RetryJob(ctx, &pb.RetryJobRequest{Id: submitResp.GetJob().GetId()})
	assertStatusCode(t, err, codes.FailedPrecondition)
}

func TestRetryJobMissingReturnsNotFound(t *testing.T) {
	s := newTestServer()
	_, err := s.RetryJob(context.Background(), &pb.RetryJobRequest{Id: "does-not-exist"})
	assertStatusCode(t, err, codes.NotFound)
}

func TestReportJobLifecycleRunsFullPath(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	submitResp, err := s.SubmitJob(ctx, validSubmitRequest())
	if err != nil {
		t.Fatalf("SubmitJob returned error: %v", err)
	}
	jobID := submitResp.GetJob().GetId()

	if _, err := s.svc.AssignToWorkers(ctx, jobID, []string{"worker-1"}); err != nil {
		t.Fatalf("test setup AssignToWorkers returned error: %v", err)
	}

	startResp, err := s.ReportJobStarted(ctx, &pb.ReportJobStartedRequest{JobId: jobID, WorkerId: "worker-1"})
	if err != nil {
		t.Fatalf("ReportJobStarted returned error: %v", err)
	}
	if startResp.GetJob().GetState() != pb.JobState_JOB_STATE_RUNNING {
		t.Errorf("State = %v, want JOB_STATE_RUNNING", startResp.GetJob().GetState())
	}

	completeResp, err := s.ReportJobCompleted(ctx, &pb.ReportJobCompletedRequest{JobId: jobID, WorkerId: "worker-1", ExitCode: 0})
	if err != nil {
		t.Fatalf("ReportJobCompleted returned error: %v", err)
	}
	if completeResp.GetJob().GetState() != pb.JobState_JOB_STATE_SUCCEEDED {
		t.Errorf("State = %v, want JOB_STATE_SUCCEEDED", completeResp.GetJob().GetState())
	}
}

func TestReportJobFailedRequeuesWhenRetriesRemain(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	req := validSubmitRequest()
	req.RetryLimit = 3
	submitResp, err := s.SubmitJob(ctx, req)
	if err != nil {
		t.Fatalf("SubmitJob returned error: %v", err)
	}
	jobID := submitResp.GetJob().GetId()

	if _, err := s.svc.AssignToWorkers(ctx, jobID, []string{"worker-1"}); err != nil {
		t.Fatalf("test setup AssignToWorkers returned error: %v", err)
	}
	if _, err := s.ReportJobStarted(ctx, &pb.ReportJobStartedRequest{JobId: jobID, WorkerId: "worker-1"}); err != nil {
		t.Fatalf("ReportJobStarted returned error: %v", err)
	}

	failResp, err := s.ReportJobFailed(ctx, &pb.ReportJobFailedRequest{JobId: jobID, WorkerId: "worker-1", FailureReason: "simulated"})
	if err != nil {
		t.Fatalf("ReportJobFailed returned error: %v", err)
	}
	if failResp.GetJob().GetState() != pb.JobState_JOB_STATE_QUEUED {
		t.Errorf("State = %v, want JOB_STATE_QUEUED (retries remain)", failResp.GetJob().GetState())
	}
	if failResp.GetJob().GetFailureReason() != "simulated" {
		t.Errorf("FailureReason = %q, want %q", failResp.GetJob().GetFailureReason(), "simulated")
	}
}

func TestReportJobStartedRejectsNonScheduledJob(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()
	submitResp, err := s.SubmitJob(ctx, validSubmitRequest())
	if err != nil {
		t.Fatalf("SubmitJob returned error: %v", err)
	}
	_, err = s.ReportJobStarted(ctx, &pb.ReportJobStartedRequest{JobId: submitResp.GetJob().GetId(), WorkerId: "worker-1"})
	assertStatusCode(t, err, codes.FailedPrecondition)
}

func TestReportJobStartedEmptyIDReturnsInvalidArgument(t *testing.T) {
	s := newTestServer()
	_, err := s.ReportJobStarted(context.Background(), &pb.ReportJobStartedRequest{})
	assertStatusCode(t, err, codes.InvalidArgument)
}

func TestReportJobCompletedEmptyIDReturnsInvalidArgument(t *testing.T) {
	s := newTestServer()
	_, err := s.ReportJobCompleted(context.Background(), &pb.ReportJobCompletedRequest{})
	assertStatusCode(t, err, codes.InvalidArgument)
}

func TestReportJobFailedEmptyIDReturnsInvalidArgument(t *testing.T) {
	s := newTestServer()
	_, err := s.ReportJobFailed(context.Background(), &pb.ReportJobFailedRequest{})
	assertStatusCode(t, err, codes.InvalidArgument)
}

func TestReportJobCompletedMissingJobReturnsNotFound(t *testing.T) {
	s := newTestServer()
	_, err := s.ReportJobCompleted(context.Background(), &pb.ReportJobCompletedRequest{JobId: "does-not-exist"})
	assertStatusCode(t, err, codes.NotFound)
}

func assertStatusCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error with code %v, got nil", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != want {
		t.Fatalf("status code = %v, want %v (message: %s)", st.Code(), want, st.Message())
	}
}
