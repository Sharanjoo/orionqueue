package health

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Run starts srv, blocks until an OS interrupt/SIGTERM arrives or the
// server exits on its own, and performs a graceful shutdown with a bounded
// timeout either way. It returns a non-nil error only if the server failed
// to start/serve, or if shutdown itself failed — a normal signal-triggered
// shutdown returns nil.
//
// Note: reliable delivery of SIGTERM to a running process is a POSIX
// concept; on Windows (this project's primary local dev platform) only
// os.Interrupt is dependably deliverable in-process, so the SIGTERM path
// is exercised in Linux CI and containers, not covered by a Windows-only
// unit test. See internal/health/run_test.go for what is covered directly.
func Run(logger *slog.Logger, srv *http.Server, shutdownTimeout time.Duration) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	logger.Info("stopped")
	return nil
}
