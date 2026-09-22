// Command api is OrionQueue's API service: the gRPC server and REST
// gateway that external clients (the dashboard, CLI, or curl) submit jobs
// and read cluster state through, and that worker agents register and
// heartbeat against.
//
// Phase 4 scope: adds worker registration, heartbeats, and automatic
// worker-loss detection (an etcd lease per worker; a background watcher
// marks a worker LOST the moment its lease expires without a renewing
// heartbeat) on top of Phase 2/3's job submission/lookup/listing/
// cancel/retry, backed by PostgreSQL. Migrations must already be applied
// (scripts/migrate.sh, or the `migrate` service in docker-compose.yml);
// this binary does not run them itself. Scheduler-facing pieces (AssignJob,
// leader election) are added in Phase 5.
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

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	orionapi "github.com/Sharanjoo/orionqueue/internal/api"
	pb "github.com/Sharanjoo/orionqueue/internal/api/gen/orionqueue/v1"
	"github.com/Sharanjoo/orionqueue/internal/config"
	"github.com/Sharanjoo/orionqueue/internal/health"
	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/leases"
	"github.com/Sharanjoo/orionqueue/internal/logging"
	"github.com/Sharanjoo/orionqueue/internal/persistence"
	"github.com/Sharanjoo/orionqueue/internal/workers"
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
	logger.Info("starting",
		slog.String("http_addr", cfg.HTTPAddr),
		slog.String("grpc_addr", cfg.GRPCAddr),
		slog.Any("etcd_endpoints", cfg.EtcdEndpoints),
	)

	// Connect with a bounded retry (see persistence.Connect) rather than
	// failing on the first attempt — Postgres inside Docker Compose can
	// still be starting even after api's container itself is running.
	dbPool, err := persistence.Connect(context.Background(), cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to connect to database", slog.String("error", err.Error()))
		return 1
	}
	defer dbPool.Close()

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		logger.Error("failed to create etcd client", slog.String("error", err.Error()))
		return 1
	}
	defer etcdClient.Close()
	if err := leases.WaitForEtcd(context.Background(), etcdClient); err != nil {
		logger.Error("failed to reach etcd", slog.String("error", err.Error()))
		return 1
	}

	jobSvc := jobs.NewService(persistence.NewJobRepository(dbPool))
	jobServer := orionapi.NewJobServer(jobSvc, logger)

	leaseManager := leases.NewEtcdManager(etcdClient)
	workerSvc := workers.NewService(persistence.NewWorkerRepository(dbPool), leaseManager)
	workerServer := orionapi.NewWorkerServer(workerSvc, jobSvc, logger)

	grpcServer := grpc.NewServer()
	pb.RegisterJobServiceServer(grpcServer, jobServer)
	pb.RegisterWorkerServiceServer(grpcServer, workerServer)
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

	gwMux, err := orionapi.NewGatewayMux(context.Background(), jobServer, workerServer)
	if err != nil {
		logger.Error("failed to build REST gateway", slog.String("error", err.Error()))
		return 1
	}

	mux := http.NewServeMux()
	health.RegisterRoutes(mux, health.Checks{
		Ready: func(ctx context.Context) error {
			if err := dbPool.Ping(ctx); err != nil {
				return fmt.Errorf("database: %w", err)
			}
			if _, err := etcdClient.Get(ctx, "orionqueue-readyz-probe"); err != nil {
				return fmt.Errorf("etcd: %w", err)
			}
			return nil
		},
	})
	mux.Handle("/", gwMux)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           orionapi.WithAuthPlaceholder(orionapi.WithRequestID(logger, mux)),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return serve(logger, grpcServer, grpcLis, httpSrv, workerSvc, jobSvc)
}

// serve starts the gRPC and HTTP servers together, runs the worker
// lease-expiration watcher for the process lifetime, and shuts everything
// down gracefully on the first OS interrupt/SIGTERM or the first
// component to fail. Split out from run so the orchestration itself —
// not just its components — could be exercised by a future test with
// fake servers if that turns out to be worth the complexity; documented
// as untested today alongside the rest of cmd/api's process-lifecycle
// wiring (see PROJECT_STATUS.md's coverage notes).
func serve(logger *slog.Logger, grpcServer *grpc.Server, grpcLis net.Listener, httpSrv *http.Server, workerSvc *workers.Service, jobSvc *jobs.Service) int {
	ctx, stop := health.ShutdownContext()
	defer stop()

	go workerSvc.WatchExpirations(ctx,
		func(workerID string) {
			logger.Info("worker marked LOST (lease expired without a renewing heartbeat)",
				slog.String("worker_id", workerID))
			// Recover any job that was SCHEDULED/RUNNING on this worker
			// (requeue if retries remain, else FAIL) — this is what
			// connects Phase 4's automatic worker-loss detection to job
			// execution, so a crashed worker's in-flight jobs don't sit
			// stuck forever.
			recovered, err := jobSvc.LoseWorker(context.Background(), workerID)
			if err != nil {
				logger.Error("failed to recover jobs from lost worker",
					slog.String("worker_id", workerID), slog.String("error", err.Error()))
				return
			}
			if recovered > 0 {
				logger.Info("recovered jobs from lost worker",
					slog.String("worker_id", workerID), slog.Int("recovered", recovered))
			}
		},
		func(workerID string, err error) {
			logger.Error("failed to mark worker lost after lease expiration",
				slog.String("worker_id", workerID), slog.String("error", err.Error()))
		},
	)

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
