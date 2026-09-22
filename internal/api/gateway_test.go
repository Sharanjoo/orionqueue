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
)

// newTestGatewayServer builds a real httptest.Server serving the REST
// gateway on top of a fresh in-memory job store, exercising the actual
// grpc-gateway wiring (proto <-> JSON, path parameter extraction, HTTP
// status mapping) end to end — not just direct Go method calls into
// JobServer, which job_server_test.go already covers.
func newTestGatewayServer(t *testing.T) *httptest.Server {
	t.Helper()
	repo := jobs.NewMemoryRepository()
	svc := jobs.NewService(repo)
	logger := slog.New(slog.NewJSONHandler(discardWriter{}, nil))
	jobServer := NewJobServer(svc, logger)

	mux, err := NewGatewayMux(context.Background(), jobServer)
	if err != nil {
		t.Fatalf("NewGatewayMux returned error: %v", err)
	}

	srv := httptest.NewServer(WithRequestID(logger, mux))
	t.Cleanup(srv.Close)
	return srv
}

func TestRESTSubmitAndGetJobRoundTrip(t *testing.T) {
	srv := newTestGatewayServer(t)

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
	srv := newTestGatewayServer(t)

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
	srv := newTestGatewayServer(t)

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
	srv := newTestGatewayServer(t)

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
