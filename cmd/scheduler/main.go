// Command scheduler is OrionQueue's scheduling loop: it assigns QUEUED
// jobs to eligible ACTIVE workers (internal/scheduler.Plan) and runs
// leader election over etcd (internal/leases.RunElection) so exactly one
// instance is actively scheduling at a time when multiple replicas run —
// see docs/adr/0002-scheduler-design.md.
//
// Phase 5 scope: the scheduling algorithm itself, leader election, and
// scheduling-decision persistence. Jobs reach SCHEDULED with
// AssignedWorkerIDs set, but nothing yet delivers that assignment to the
// worker or executes it — that's Phase 6 ("Job execution").
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/Sharanjoo/orionqueue/internal/config"
	"github.com/Sharanjoo/orionqueue/internal/health"
	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/leases"
	"github.com/Sharanjoo/orionqueue/internal/logging"
	"github.com/Sharanjoo/orionqueue/internal/persistence"
	"github.com/Sharanjoo/orionqueue/internal/scheduler"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

const (
	electionKey        = "/orionqueue/scheduler/leader"
	electionSessionTTL = 15 * time.Second
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := config.Load("orionqueue-scheduler")
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error(
			"invalid configuration", slog.String("error", err.Error()),
		)
		return 1
	}

	logger := logging.NewStdout(cfg.ServiceName, cfg.Environment, cfg.LogLevel)
	logger.Info("starting",
		slog.String("http_addr", cfg.HTTPAddr),
		slog.Any("etcd_endpoints", cfg.EtcdEndpoints),
		slog.Int64("scheduling_interval_seconds", cfg.SchedulingIntervalSeconds),
		slog.Bool("preemption_enabled", cfg.PreemptionEnabled),
	)

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
	workerSvc := workers.NewService(persistence.NewWorkerRepository(dbPool), leases.NewEtcdManager(etcdClient))
	decisionRepo := persistence.NewSchedulingDecisionRepository(dbPool)
	schedulerSvc := scheduler.NewService(jobSvc, workerSvc, decisionRepo,
		scheduler.WithPreemptionEnabled(cfg.PreemptionEnabled))

	var isLeader atomic.Bool
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
	// /leader is scheduler-specific: readiness (above) reflects "can this
	// process reach its dependencies," which is true for every standby
	// replica too — this endpoint answers the operationally different
	// question "is this particular replica the one actually scheduling
	// right now."
	mux.HandleFunc("/leader", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"is_leader": isLeader.Load()})
	})

	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	return serve(logger, httpSrv, etcdClient, cfg, schedulerSvc, &isLeader)
}

// serve starts the health HTTP server and the etcd leader-election loop
// together, and shuts both down gracefully on the first OS
// interrupt/SIGTERM. Split out from run so process orchestration is
// separated from wiring, the same pattern cmd/api uses; documented as
// untested at this level alongside the rest of both binaries'
// process-lifecycle code (see PROJECT_STATUS.md's coverage notes) — the
// pieces it coordinates (scheduler.Service, leases.RunElection,
// internal/health) are each tested directly and, for leader election,
// against a real etcd (tests/integration/etcd_election_test.go).
func serve(
	logger *slog.Logger,
	httpSrv *http.Server,
	etcdClient *clientv3.Client,
	cfg config.Config,
	schedulerSvc *scheduler.Service,
	isLeader *atomic.Bool,
) int {
	ctx, stop := health.ShutdownContext()
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http listening", slog.String("addr", httpSrv.Addr))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
			return
		}
		errCh <- nil
	}()

	electionDone := make(chan struct{})
	go func() {
		defer close(electionDone)
		instanceID := fmt.Sprintf("scheduler-%d", os.Getpid())
		interval := time.Duration(cfg.SchedulingIntervalSeconds) * time.Second

		err := leases.RunElection(ctx, etcdClient, electionKey, instanceID, electionSessionTTL,
			func(leaderCtx context.Context) {
				isLeader.Store(true)
				logger.Info("became scheduler leader", slog.String("instance_id", instanceID))
				runSchedulingLoop(leaderCtx, logger, schedulerSvc, interval)
				isLeader.Store(false)
				logger.Info("stepped down as scheduler leader")
			},
			func(status string) {
				logger.Info("election status changed", slog.String("status", status))
			},
		)
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("leader election loop exited with error", slog.String("error", err.Error()))
		}
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

	select {
	case <-electionDone:
	case <-time.After(10 * time.Second):
		logger.Warn("leader election loop did not stop within timeout")
	}

	logger.Info("stopped")
	return 0
}

// runSchedulingLoop calls schedulerSvc.RunOnce once immediately, then on
// every tick of interval, for as long as leaderCtx is valid — i.e. for as
// long as this instance holds scheduler leadership.
func runSchedulingLoop(leaderCtx context.Context, logger *slog.Logger, schedulerSvc *scheduler.Service, interval time.Duration) {
	runPass := func() {
		result, err := schedulerSvc.RunOnce(leaderCtx)
		if err != nil {
			logger.Error("scheduling pass failed", slog.String("error", err.Error()))
			return
		}
		if result.Assigned > 0 || result.Conflicted > 0 || result.Preempted > 0 {
			logger.Info("scheduling pass complete",
				slog.Int("considered", result.Considered),
				slog.Int("assigned", result.Assigned),
				slog.Int("conflicted", result.Conflicted),
				slog.Int("preempted", result.Preempted),
			)
		}
	}

	runPass()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-leaderCtx.Done():
			return
		case <-ticker.C:
			runPass()
		}
	}
}
