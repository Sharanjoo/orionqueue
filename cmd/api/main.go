// Command api is OrionQueue's API service: the gRPC server and REST
// gateway that external clients (the dashboard, CLI, or curl) submit jobs
// and read cluster state through.
//
// Phase 2 scope: job submission, lookup, listing, cancellation, and retry,
// backed by an in-memory job store. Job state does not survive a restart
// yet — PostgreSQL-backed persistence is Phase 3. Worker-facing RPCs
// (RegisterWorker, WorkerHeartbeat, AssignJob, ...) are added in Phase 4/5
// once there's a scheduler/worker to call them.
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

	// Phase 2 uses an in-memory repository: job state resets on restart.
	// Phase 3 swaps this for a PostgreSQL-backed jobs.Repository without
	// any other change in this file.
	repo := jobs.NewMemoryRepository()
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
	// Phase 2 has no external dependencies to check; readiness is always
	// true. Phase 3/4 will wire real PostgreSQL/etcd checks in here.
	health.RegisterRoutes(mux, health.Checks{})
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
