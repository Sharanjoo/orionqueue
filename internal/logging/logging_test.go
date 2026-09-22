package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNewEmitsJSONWithServiceAndEnvironment(t *testing.T) {
	var buf bytes.Buffer
	logger := New(&buf, "orionqueue-api", "test-env", "info")
	logger.Info("hello", "job_id", "job-1")

	line := strings.TrimSpace(buf.String())
	var payload map[string]any
	if err := json.Unmarshal([]byte(line), &payload); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, line)
	}

	if payload["msg"] != "hello" {
		t.Errorf("msg = %v, want %q", payload["msg"], "hello")
	}
	if payload["service"] != "orionqueue-api" {
		t.Errorf("service = %v, want %q", payload["service"], "orionqueue-api")
	}
	if payload["environment"] != "test-env" {
		t.Errorf("environment = %v, want %q", payload["environment"], "test-env")
	}
	if payload["job_id"] != "job-1" {
		t.Errorf("job_id = %v, want %q", payload["job_id"], "job-1")
	}
}

func TestNewFiltersBelowConfiguredLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := New(&buf, "svc", "env", "warn")

	logger.Info("should be filtered out")
	logger.Warn("should appear")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1: %v", len(lines), lines)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &payload); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if payload["msg"] != "should appear" {
		t.Errorf("msg = %v, want %q", payload["msg"], "should appear")
	}
}

func TestParseLevelHandlesAllKnownValues(t *testing.T) {
	cases := map[string]bool{
		"debug": true,
		"info":  false,
		"warn":  false,
		"error": false,
		"bogus": false, // unknown values fall back to info
	}
	for level, expectDebugVisible := range cases {
		var buf bytes.Buffer
		logger := New(&buf, "svc", "env", level)
		logger.Debug("debug line")
		gotOutput := strings.TrimSpace(buf.String()) != ""
		if gotOutput != expectDebugVisible {
			t.Errorf("level %q: debug line present = %v, want %v", level, gotOutput, expectDebugVisible)
		}
	}
}
