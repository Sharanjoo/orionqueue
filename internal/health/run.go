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

// ShutdownContext returns a context that's canceled when an OS
// interrupt/SIGTERM arrives, and the stop function to release the signal
// notification (always defer it). Binaries that manage more than one
// server and need to shut them down together (cmd/api, with an HTTP
// server and a gRPC server) use this directly instead of Run, which only
// manages a single *http.Server.
//
// Note: reliable delivery of SIGTERM to a running process is a POSIX
// concept; on Windows (this project's primary local dev platform) only
// os.Interrupt is dependably deliverable in-process, so the SIGTERM path
// is exercised in Linux CI and containers, not covered by a Windows-only
// unit test.
func ShutdownContext() (context.Context, func()) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// Run starts srv, blocks until an OS interrupt/SIGTERM arrives or the
// server exits on its own, and performs a graceful shutdown with a bounded
// timeout either way. It returns a non-nil error only if the server failed
// to start/serve, or if shutdown itself failed — a normal signal-triggered
// shutdown returns nil. See internal/health/run_test.go for what is
// covered directly (the Windows SIGTERM caveat on ShutdownContext applies
// here too).
func Run(logger *slog.Logger, srv *http.Server, shutdownTimeout time.Duration) error {
	ctx, stop := ShutdownContext()
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
