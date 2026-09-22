// Command api is OrionQueue's API service: the gRPC server and REST
// gateway that external clients (the dashboard, CLI, or curl) submit jobs
// and read cluster state through.
//
// Phase 3 scope: job submission, lookup, listing, cancellation, and retry,
// backed by PostgreSQL — job state survives a restart. Migrations must
// already be applied (scripts/migrate.sh, or the `migrate` service in
// docker-compose.yml); this binary does not run them itself. Worker-facing
// RPCs (RegisterWorker, WorkerHeartbeat, AssignJob, ...) are added in
// Phase 4/5 once there's a scheduler/worker to call them.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	orionapi "github.com/Sharanjoo/orionqueue/internal/api"
	pb "github.com/Sharanjoo/orionqueue/internal/api/gen/orionqueue/v1"
	"github.com/Sharanjoo/orionqueue/internal/config"
	"github.com/Sharanjoo/orionqueue/internal/health"
	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/logging"
	"github.com/Sharanjoo/orionqueue/internal/persistence"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := config.Load("orionqueue-api")
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error(
			"invalid configuration", slog.String("error", err.Error()),
		)
		return 1
	}

	logger := logging.NewStdout(cfg.ServiceName, cfg.Environment, cfg.LogLevel)
	logger.Info("starting", slog.String("http_addr", cfg.HTTPAddr), slog.String("grpc_addr", cfg.GRPCAddr))

	// Connect with a bounded retry (see persistence.Connect) rather than
	// failing on the first attempt — Postgres inside Docker Compose can
	// still be starting even after api's container itself is running.
	// This is the one dependency this service has; if it's unreachable
	// after all retries, there's nothing useful this process can do, so
	// it exits rather than serving requests against a repository that
	// can't work (jobs.MemoryRepository, Phase 2's fallback, is no longer
	// wired in here — a service silently reverting to "state doesn't
	// survive a restart" on DB trouble would be a worse failure mode than
	// just not starting).
	dbPool, err := persistence.Connect(context.Background(), cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to connect to database", slog.String("error", err.Error()))
		return 1
	}
	defer dbPool.Close()

	repo := persistence.NewJobRepository(dbPool)
	svc := jobs.NewService(repo)
	jobServer := orionapi.NewJobServer(svc, logger)

	grpcServer := grpc.NewServer()
	pb.RegisterJobServiceServer(grpcServer, jobServer)
	// Server reflection lets ad-hoc tools (grpcurl, grpcui, Postman) call
	// the API without needing a local copy of the .proto files — useful
	// for a portfolio project people will want to poke at directly.
	// Real production deployments often disable this; it's on by default
	// here since there's no auth yet either (see README's Limitations).
	reflection.Register(grpcServer)

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("failed to listen for gRPC", slog.String("addr", cfg.GRPCAddr), slog.String("error", err.Error()))
		return 1
	}

	gwMux, err := orionapi.NewGatewayMux(context.Background(), jobServer)
	if err != nil {
		logger.Error("failed to build REST gateway", slog.String("error", err.Error()))
		return 1
	}

	mux := http.NewServeMux()
	health.RegisterRoutes(mux, health.Checks{
		Ready: func(ctx context.Context) error { return dbPool.Ping(ctx) },
	})
	mux.Handle("/", gwMux)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           orionapi.WithAuthPlaceholder(orionapi.WithRequestID(logger, mux)),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return serve(logger, grpcServer, grpcLis, httpSrv)
}

// serve starts the gRPC and HTTP servers together and shuts both down
// gracefully on the first OS interrupt/SIGTERM or the first one to fail.
// Split out from run so the orchestration itself — not just its
// components — could be exercised by a future test with fake servers if
// that turns out to be worth the complexity; documented as untested today
// alongside the rest of cmd/api's process-lifecycle wiring (see
// PROJECT_STATUS.md's coverage notes).
func serve(logger *slog.Logger, grpcServer *grpc.Server, grpcLis net.Listener, httpSrv *http.Server) int {
	ctx, stop := health.ShutdownContext()
	defer stop()

	errCh := make(chan error, 2)
	go func() {
		logger.Info("grpc listening", slog.String("addr", grpcLis.Addr().String()))
		if err := grpcServer.Serve(grpcLis); err != nil {
			errCh <- fmt.Errorf("grpc server: %w", err)
			return
		}
		errCh <- nil
	}()
	go func() {
		logger.Info("http listening", slog.String("addr", httpSrv.Addr))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		if err != nil {
			logger.Error("server error", slog.String("error", err.Error()))
			return 1
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http graceful shutdown failed", slog.String("error", err.Error()))
	}

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		logger.Warn("grpc graceful stop timed out, forcing stop")
		grpcServer.Stop()
	}

	logger.Info("stopped")
	return 0
}
