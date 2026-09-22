// Package config loads runtime configuration for OrionQueue's Go services
// from ORIONQUEUE_-prefixed environment variables, layered over defaults
// that are always valid so a service can start with zero configuration in
// local development.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds settings shared by OrionQueue's Go services. Phase 1 only
// needs process identity and HTTP listen settings; later phases extend
// this (database DSN, etcd endpoints, etc.) without changing how it's
// loaded or validated.
type Config struct {
	// ServiceName identifies which binary this process is; used in logs
	// and, from Phase 10 onward, metrics labels.
	ServiceName string
	// Environment is a free-form deployment environment label (e.g.
	// "local", "ci", "staging", "production").
	Environment string
	// HTTPAddr is the address the service's HTTP server (health checks,
	// and the REST gateway from Phase 2 onward) listens on.
	HTTPAddr string
	// GRPCAddr is the address the service's gRPC server listens on.
	// Empty for services that don't run a gRPC server (only cmd/api does,
	// as of Phase 2).
	GRPCAddr string
	// LogLevel controls the minimum level emitted by the service's
	// structured logger ("debug", "info", "warn", or "error").
	LogLevel string
}

// Defaults returns the baseline configuration for serviceName before
// environment overrides are applied, so callers always get a valid,
// runnable Config even with no environment variables set.
func Defaults(serviceName string) Config {
	return Config{
		ServiceName: serviceName,
		Environment: "local",
		HTTPAddr:    defaultHTTPAddr(serviceName),
		GRPCAddr:    defaultGRPCAddr(serviceName),
		LogLevel:    "info",
	}
}

// defaultHTTPAddr gives each known service its own default port so
// cmd/api and cmd/scheduler can both run directly on one developer machine
// (outside Docker, where each service gets its own network namespace)
// without a port collision. The 7080/7081 range was chosen after checking
// this project's dev machine directly: 8080 was already bound by an
// unrelated service (a locally running Airflow webserver), so the more
// commonly-claimed 80xx range is avoided.
func defaultHTTPAddr(serviceName string) string {
	switch serviceName {
	case "orionqueue-scheduler":
		return ":7081"
	default:
		return ":7080"
	}
}

// defaultGRPCAddr mirrors defaultHTTPAddr for the gRPC listener. Only
// cmd/api runs a gRPC server as of Phase 2; other services get a default
// too (so Config.Validate still passes with zero env vars set) even
// though nothing listens on it yet.
func defaultGRPCAddr(serviceName string) string {
	switch serviceName {
	case "orionqueue-scheduler":
		return ":9081"
	default:
		return ":9080"
	}
}

// Load builds a Config for serviceName, applying ORIONQUEUE_-prefixed
// environment variable overrides on top of Defaults. Missing variables are
// never an error (defaults are always valid); a malformed supplied value
// is, so misconfiguration fails fast at startup instead of surfacing later
// as a confusing runtime error.
func Load(serviceName string) (Config, error) {
	cfg := Defaults(serviceName)

	if v, ok := os.LookupEnv("ORIONQUEUE_ENVIRONMENT"); ok && v != "" {
		cfg.Environment = v
	}
	if v, ok := os.LookupEnv("ORIONQUEUE_HTTP_ADDR"); ok && v != "" {
		cfg.HTTPAddr = v
	}
	if v, ok := os.LookupEnv("ORIONQUEUE_GRPC_ADDR"); ok && v != "" {
		cfg.GRPCAddr = v
	}
	if v, ok := os.LookupEnv("ORIONQUEUE_LOG_LEVEL"); ok && v != "" {
		cfg.LogLevel = v
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate reports whether cfg is well-formed enough to start a service.
func (c Config) Validate() error {
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("invalid ORIONQUEUE_LOG_LEVEL %q: must be one of debug, info, warn, error", c.LogLevel)
	}
	if c.HTTPAddr == "" {
		return fmt.Errorf("ORIONQUEUE_HTTP_ADDR must not be empty")
	}
	if _, err := portOf(c.HTTPAddr); err != nil {
		return fmt.Errorf("invalid ORIONQUEUE_HTTP_ADDR %q: %w", c.HTTPAddr, err)
	}
	if c.GRPCAddr != "" {
		if _, err := portOf(c.GRPCAddr); err != nil {
			return fmt.Errorf("invalid ORIONQUEUE_GRPC_ADDR %q: %w", c.GRPCAddr, err)
		}
	}
	return nil
}

// portOf extracts and validates the trailing ":PORT" segment of a
// "[HOST]:PORT" address, so a malformed ORIONQUEUE_HTTP_ADDR is caught by
// Validate with a clear error instead of failing later inside
// net.Listen.
func portOf(addr string) (int, error) {
	i := len(addr) - 1
	for i >= 0 && addr[i] != ':' {
		i--
	}
	if i < 0 {
		return 0, fmt.Errorf("missing ':' separating host and port")
	}
	port, err := strconv.Atoi(addr[i+1:])
	if err != nil {
		return 0, fmt.Errorf("port must be numeric: %w", err)
	}
	if port < 0 || port > 65535 {
		return 0, fmt.Errorf("port %d out of range", port)
	}
	return port, nil
}
