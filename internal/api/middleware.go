package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"time"
)

// RequestIDHeader is the HTTP header carrying the correlation ID for a
// request, both inbound (a caller may supply one to correlate their own
// logs with ours) and outbound (always echoed back on the response).
const RequestIDHeader = "X-Request-Id"

type requestIDKey struct{}

// WithRequestID wraps next so every request has a request ID — reusing an
// inbound X-Request-Id header if the caller supplied one, generating a new
// one otherwise — echoes it on the response, and logs one structured line
// per request (method, path, status, duration, request ID). This is the
// project brief's "correlation/request IDs" requirement, implemented once
// here rather than per-handler.
func WithRequestID(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if id == "" {
			id = newRequestID()
		}
		w.Header().Set(RequestIDHeader, id)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r.WithContext(ctx))

		logger.Info("request",
			slog.String("request_id", id),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Duration("duration", time.Since(start)),
		)
	})
}

// RequestIDFromContext returns the request ID set by WithRequestID, or ""
// if none is present (e.g. a direct gRPC call that didn't go through the
// REST gateway's HTTP middleware).
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

func newRequestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// statusRecorder captures the status code a handler wrote, so
// WithRequestID can log it — http.ResponseWriter has no getter for this.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// WithAuthPlaceholder is a deliberate pass-through middleware marking
// where request authentication will be wired in (see README.md's
// Limitations section). Every request is currently treated as
// authenticated — there is no local auth yet. Routing every request
// through this no-op now means adding real authentication later is a
// change to this one function, not to every handler.
func WithAuthPlaceholder(next http.Handler) http.Handler {
	return next
}
