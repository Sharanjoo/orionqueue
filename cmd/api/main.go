// Command api is OrionQueue's API service: the gRPC server and REST
// gateway that external clients (the dashboard, CLI, or curl) submit jobs
// and read cluster state through.
//
// Phase 1 scope: process foundation only — config, structured logging, and
// health endpoints. Job submission, lookup, and the rest of the API
// surface are added starting Phase 2.
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
	cfg, err := config.Load("orionqueue-api")
	if err != nil {
		// No structured logger yet if config itself failed to load.
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error(
			"invalid configuration", slog.String("error", err.Error()),
		)
		return 1
	}

	logger := logging.NewStdout(cfg.ServiceName, cfg.Environment, cfg.LogLevel)
	logger.Info("starting", slog.String("http_addr", cfg.HTTPAddr))
	logger.Info("phase 1 foundation: no gRPC/REST business routes yet (see Phase 2)")

	// Phase 1 has no external dependencies to check; readiness is always
	// true. Phase 3/4 will wire real PostgreSQL/etcd checks into this.
	srv := health.NewServer(cfg.HTTPAddr, logger, health.Checks{})

	if err := health.Run(logger, srv, 10*time.Second); err != nil {
		logger.Error("server error", slog.String("error", err.Error()))
		return 1
	}
	return 0
}
