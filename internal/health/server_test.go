package health

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http/httptest"
	"testing"
)

// discardLogger and discardWriter are shared by server_test.go and
// run_test.go (same package) so tests don't spam test output with the
// service's own log lines.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestHealthzAlwaysReturnsOK(t *testing.T) {
	srv := NewServer(":0", discardLogger(), Checks{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/healthz", nil)

	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status field = %q, want %q", body["status"], "ok")
	}
}

func TestReadyzReturnsOKWhenReadyCheckPasses(t *testing.T) {
	srv := NewServer(":0", discardLogger(), Checks{
		Ready: func(context.Context) error { return nil },
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)

	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestReadyzReturns503WhenReadyCheckFails(t *testing.T) {
	srv := NewServer(":0", discardLogger(), Checks{
		Ready: func(context.Context) error { return errors.New("db unavailable") },
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)

	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != 503 {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body["reason"] != "db unavailable" {
		t.Errorf("reason field = %q, want %q", body["reason"], "db unavailable")
	}
}

func TestReadyzDefaultsToAlwaysReady(t *testing.T) {
	srv := NewServer(":0", discardLogger(), Checks{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)

	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200 when no Ready check is configured", rec.Code)
	}
}
