// Package logging configures structured JSON logging shared by OrionQueue's
// Go services, so every log line — from the API, the scheduler, and later
// binaries — carries the same service/environment fields and can be
// filtered consistently regardless of which process emitted it.
package logging

import (
	"io"
	"log/slog"
	"os"
)

// New returns a structured JSON slog.Logger writing to w, tagged with
// service and environment attributes on every record.
func New(w io.Writer, service, environment, level string) *slog.Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: parseLevel(level)})
	return slog.New(handler).With(
		slog.String("service", service),
		slog.String("environment", environment),
	)
}

// NewStdout is a convenience wrapper around New that writes to os.Stdout,
// the normal case for a running service (container log collectors read
// stdout, not a file).
func NewStdout(service, environment, level string) *slog.Logger {
	return New(os.Stdout, service, environment, level)
}

func parseLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
