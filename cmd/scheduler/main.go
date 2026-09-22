// Command scheduler is OrionQueue's scheduling loop: it assigns queued
// jobs to eligible workers and runs leader election over etcd so exactly
// one instance is active at a time when multiple replicas run.
//
// Phase 1 scope: process foundation only — config, structured logging, and
// health endpoints. Leader election and the scheduling loop itself are
// added starting Phase 5.
package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/Sharanjoo/orionqueue/internal/config"
	"github.com/Sharanjoo/orionqueue/internal/health"
	"github.com/Sharanjoo/orionqueue/internal/logging"
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
	logger.Info("starting", slog.String("http_addr", cfg.HTTPAddr))
	logger.Info("phase 1 foundation: no etcd leader election or scheduling loop yet (see Phase 5)")

	srv := health.NewServer(cfg.HTTPAddr, logger, health.Checks{})

	if err := health.Run(logger, srv, 10*time.Second); err != nil {
		logger.Error("server error", slog.String("error", err.Error()))
		return 1
	}
	return 0
}
