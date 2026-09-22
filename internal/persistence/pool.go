// Package persistence provides OrionQueue's PostgreSQL-backed
// implementations of the repository interfaces defined in internal/jobs
// (and, from later phases, workers/GPUs/leases/checkpoints). PostgreSQL
// is the durable source of truth for this state — see
// docs/adr/0001-postgresql-vs-etcd.md for why, versus etcd.
package persistence

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// connectMaxAttempts and connectBaseDelay bound Connect's retry loop.
// Postgres inside Docker Compose can take a few seconds to start
// accepting connections even after its own healthcheck passes, since
// ours runs from a separate container on a separate clock — this ride's
// out that startup race instead of cmd/api failing on the first try.
const (
	connectMaxAttempts = 10
	connectBaseDelay   = 300 * time.Millisecond
)

// Connect opens a connection pool to databaseURL and verifies
// connectivity with a bounded, increasing-delay retry. It returns an
// error only once the database is still unreachable after every attempt
// — callers should treat that as fatal at startup, not something to
// silently retry forever.
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("persistence: parse/configure database URL: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= connectMaxAttempts; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = pool.Ping(pingCtx)
		cancel()
		if lastErr == nil {
			return pool, nil
		}
		if attempt == connectMaxAttempts {
			break
		}
		select {
		case <-time.After(connectBaseDelay * time.Duration(attempt)):
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		}
	}

	pool.Close()
	return nil, fmt.Errorf("persistence: database unreachable after %d attempts: %w", connectMaxAttempts, lastErr)
}
