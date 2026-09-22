// Package health provides the /healthz and /readyz HTTP endpoints, plus a
// shared graceful-shutdown process lifecycle, used by every OrionQueue Go
// binary (cmd/api, cmd/scheduler). Keeping this in one package means
// startup/shutdown handling — the part that's easy to get subtly wrong —
// is written and tested once instead of duplicated per binary.
package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// Checks lets a service plug real dependency checks into /readyz. Ready is
// called on every /readyz request; a non-nil error means "not ready yet"
// and is reported (but not logged as an error — an unready dependency
// during startup is expected, not exceptional). Phase 1 has no
// dependencies to check, so callers may leave Ready nil.
type Checks struct {
	Ready func(ctx context.Context) error
}

// RegisterRoutes adds /healthz (always 200 once the process is up) and
// /readyz (200 once checks.Ready succeeds, 503 otherwise) to mux. Exposed
// separately from NewServer so a binary that serves more than health
// checks on one port (cmd/api, which also serves the REST gateway) can
// build its own top-level mux, register health routes on it, and mount
// its other routes alongside them — rather than needing two separate
// listeners just to get /healthz.
func RegisterRoutes(mux *http.ServeMux, checks Checks) {
	if checks.Ready == nil {
		checks.Ready = func(context.Context) error { return nil }
	}

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := checks.Ready(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "not_ready",
				"reason": err.Error(),
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
}

// NewServer builds an *http.Server exposing only /healthz and /readyz for
// service — the simple case, used by binaries (cmd/scheduler) that don't
// serve anything else on their HTTP port. Binaries that need to add more
// routes use RegisterRoutes directly on their own mux instead.
func NewServer(addr string, logger *slog.Logger, checks Checks) *http.Server {
	mux := http.NewServeMux()
	RegisterRoutes(mux, checks)
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
