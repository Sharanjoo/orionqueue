package api

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithRequestIDGeneratesIDWhenAbsent(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(discardWriter{}, nil))
	handler := WithRequestID(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if RequestIDFromContext(r.Context()) == "" {
			t.Error("expected a non-empty request ID in the handler's context")
		}
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	handler.ServeHTTP(rec, req)

	if rec.Header().Get(RequestIDHeader) == "" {
		t.Error("expected a non-empty X-Request-Id response header")
	}
}

func TestWithRequestIDReusesInboundHeader(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(discardWriter{}, nil))
	var seen string
	handler := WithRequestID(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(RequestIDHeader, "caller-supplied-id")
	handler.ServeHTTP(rec, req)

	if seen != "caller-supplied-id" {
		t.Errorf("request ID in context = %q, want %q", seen, "caller-supplied-id")
	}
	if got := rec.Header().Get(RequestIDHeader); got != "caller-supplied-id" {
		t.Errorf("response X-Request-Id = %q, want %q", got, "caller-supplied-id")
	}
}

func TestWithRequestIDGeneratesDistinctIDsAcrossRequests(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(discardWriter{}, nil))
	handler := WithRequestID(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/x", nil))
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/x", nil))

	id1 := rec1.Header().Get(RequestIDHeader)
	id2 := rec2.Header().Get(RequestIDHeader)
	if id1 == "" || id2 == "" {
		t.Fatal("expected non-empty request IDs")
	}
	if id1 == id2 {
		t.Error("expected distinct request IDs for two separate requests")
	}
}

func TestRequestIDFromContextReturnsEmptyWhenUnset(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	if got := RequestIDFromContext(req.Context()); got != "" {
		t.Errorf("RequestIDFromContext on a plain context = %q, want empty", got)
	}
}

func TestWithAuthPlaceholderIsPassThrough(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusTeapot)
	})
	handler := WithAuthPlaceholder(inner)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))

	if !called {
		t.Fatal("expected the wrapped handler to be called")
	}
	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want %d (pass-through must not alter the response)", rec.Code, http.StatusTeapot)
	}
}
