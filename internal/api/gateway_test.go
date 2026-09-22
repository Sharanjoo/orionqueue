package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/leases"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

// newTestGatewayServer builds a real httptest.Server serving the REST
// gateway on top of fresh in-memory job/worker stores, exercising the
// actual grpc-gateway wiring (proto <-> JSON, path parameter extraction,
// HTTP status mapping) end to end — not just direct Go method calls into
// JobServer/WorkerServer, which job_server_test.go and
// worker_server_test.go already cover. Also returns the workers.Service
// directly, since RegisterWorker is deliberately gRPC-only (see
// proto/orionqueue/v1/worker_service.proto) — a test that wants a worker
// to exist before hitting the REST-exposed ListWorkers has no REST path
// to create one and registers through the service instead.
func newTestGatewayServer(t *testing.T) (*httptest.Server, *workers.Service) {
	t.Helper()
	jobSvc := jobs.NewService(jobs.NewMemoryRepository())
	workerSvc := workers.NewService(workers.NewMemoryRepository(), leases.NewFakeManager())
	logger := slog.New(slog.NewJSONHandler(discardWriter{}, nil))
	jobServer := NewJobServer(jobSvc, logger)
	workerServer := NewWorkerServer(workerSvc, jobSvc, logger)

	mux, err := NewGatewayMux(context.Background(), jobServer, workerServer)
	if err != nil {
		t.Fatalf("NewGatewayMux returned error: %v", err)
	}

	srv := httptest.NewServer(WithRequestID(logger, mux))
	t.Cleanup(srv.Close)
	return srv, workerSvc
}

func TestRESTSubmitAndGetJobRoundTrip(t *testing.T) {
	srv, _ := newTestGatewayServer(t)

	submitBody := map[string]any{
		"name":  "train-resnet",
		"owner": "sharan",
		"image": "orionqueue/fake-gpu-job:latest",
		"resources": map[string]any{
			"gpu_count":            1,
			"min_gpu_memory_bytes": "8589934592",
			"cpu_cores":            2,
			"memory_bytes":         "4294967296",
		},
		"priority":    50,
		"retry_limit": 3,
	}
	b, err := json.Marshal(submitBody)
	if err != nil {
		t.Fatalf("failed to marshal request body: %v", err)
	}

	resp, err := http.Post(srv.URL+"/api/v1/jobs", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST /api/v1/jobs failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/v1/jobs status = %d, want 200", resp.StatusCode)
	}
	if resp.Header.Get(RequestIDHeader) == "" {
		t.Error("expected an X-Request-Id response header")
	}

	var submitted struct {
		Job struct {
			ID    string `json:"id"`
			State string `json:"state"`
			Name  string `json:"name"`
		} `json:"job"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&submitted); err != nil {
		t.Fatalf("failed to decode submit response: %v", err)
	}
	if submitted.Job.ID == "" {
		t.Fatal("expected a non-empty job id in the submit response")
	}
	if submitted.Job.State != "JOB_STATE_QUEUED" {
		t.Errorf("state = %q, want %q", submitted.Job.State, "JOB_STATE_QUEUED")
	}

	getResp, err := http.Get(srv.URL + "/api/v1/jobs/" + submitted.Job.ID)
	if err != nil {
		t.Fatalf("GET /api/v1/jobs/{id} failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/jobs/{id} status = %d, want 200", getResp.StatusCode)
	}

	var fetched struct {
		Job struct {
			Name string `json:"name"`
		} `json:"job"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&fetched); err != nil {
		t.Fatalf("failed to decode get response: %v", err)
	}
	if fetched.Job.Name != "train-resnet" {
		t.Errorf("Name = %q, want %q", fetched.Job.Name, "train-resnet")
	}
}

func TestRESTGetJobMissingReturns404(t *testing.T) {
	srv, _ := newTestGatewayServer(t)

	resp, err := http.Get(srv.URL + "/api/v1/jobs/does-not-exist")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestRESTSubmitInvalidJobReturns400(t *testing.T) {
	srv, _ := newTestGatewayServer(t)

	resp, err := http.Post(srv.URL+"/api/v1/jobs", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestRESTCancelAndListJobs(t *testing.T) {
	srv, _ := newTestGatewayServer(t)

	submitResp, err := http.Post(srv.URL+"/api/v1/jobs", "application/json", bytes.NewReader([]byte(
		`{"name":"j1","owner":"sharan","image":"img"}`,
	)))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	var submitted struct {
		Job struct {
			ID string `json:"id"`
		} `json:"job"`
	}
	if err := json.NewDecoder(submitResp.Body).Decode(&submitted); err != nil {
		t.Fatalf("failed to decode submit response: %v", err)
	}
	submitResp.Body.Close()

	cancelResp, err := http.Post(srv.URL+"/api/v1/jobs/"+submitted.Job.ID+"/cancel", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatalf("POST cancel failed: %v", err)
	}
	defer cancelResp.Body.Close()
	if cancelResp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200", cancelResp.StatusCode)
	}

	listResp, err := http.Get(srv.URL + "/api/v1/jobs")
	if err != nil {
		t.Fatalf("GET list failed: %v", err)
	}
	defer listResp.Body.Close()
	var list struct {
		Jobs []struct {
			ID    string `json:"id"`
			State string `json:"state"`
		} `json:"jobs"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&list); err != nil {
		t.Fatalf("failed to decode list response: %v", err)
	}
	if len(list.Jobs) != 1 {
		t.Fatalf("expected 1 job in the list, got %d", len(list.Jobs))
	}
	if list.Jobs[0].State != "JOB_STATE_CANCELLED" {
		t.Errorf("state = %q, want %q", list.Jobs[0].State, "JOB_STATE_CANCELLED")
	}
}

func TestRESTListWorkers(t *testing.T) {
	srv, workerSvc := newTestGatewayServer(t)

	if _, err := workerSvc.Register(context.Background(), workers.RegisterInput{
		Hostname:            "worker-1.local",
		CPUCapacity:         8,
		MemoryCapacityBytes: 32 << 30,
		GPUs:                []workers.GPU{{DeviceIndex: 0, UUID: "GPU-fake-0", TotalMemoryBytes: 16 << 30}},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	resp, err := http.Get(srv.URL + "/api/v1/workers")
	if err != nil {
		t.Fatalf("GET /api/v1/workers failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var list struct {
		Workers []struct {
			Hostname string `json:"hostname"`
			Status   string `json:"status"`
			Gpus     []struct {
				Uuid string `json:"uuid"`
			} `json:"gpus"`
		} `json:"workers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(list.Workers) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(list.Workers))
	}
	if list.Workers[0].Hostname != "worker-1.local" {
		t.Errorf("hostname = %q, want %q", list.Workers[0].Hostname, "worker-1.local")
	}
	if list.Workers[0].Status != "WORKER_STATUS_ACTIVE" {
		t.Errorf("status = %q, want %q", list.Workers[0].Status, "WORKER_STATUS_ACTIVE")
	}
	if len(list.Workers[0].Gpus) != 1 || list.Workers[0].Gpus[0].Uuid != "GPU-fake-0" {
		t.Errorf("gpus = %+v, want a single GPU-fake-0 entry (GPU inventory visible via REST)", list.Workers[0].Gpus)
	}
}
