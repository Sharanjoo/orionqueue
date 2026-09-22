// Package leases wraps etcd's lease primitive behind an interface,
// giving worker registration/heartbeat logic (internal/workers) a way to
// create a lease bound to a key, renew it once per heartbeat, and be
// notified when a lease expires without renewal — without internal/workers
// importing an etcd client directly. See ADR-0001 for why etcd (not
// PostgreSQL) owns this short-lived coordination state.
package leases

import (
	"context"
	"errors"
	"time"
)

// ErrLeaseNotFound is returned by Renew when the lease has already
// expired (its TTL elapsed before this renewal arrived). The caller
// should treat this as "the worker needs to register again," not retry
// the same lease ID.
var ErrLeaseNotFound = errors.New("leases: lease not found (already expired)")

// Manager grants, renews, revokes, and watches the expiration of
// etcd-backed leases.
type Manager interface {
	// Grant creates a new lease with the given TTL and binds key to it,
	// so etcd automatically deletes key the moment the lease expires
	// without a Renew call — that deletion is what WatchExpirations
	// reports.
	Grant(ctx context.Context, key string, ttl time.Duration) (id int64, err error)

	// Renew extends id's TTL to ttl-from-now. Returns ErrLeaseNotFound if
	// the lease already expired.
	Renew(ctx context.Context, id int64) error

	// Revoke ends id immediately (deleting its bound key right away)
	// rather than waiting for its TTL to elapse.
	Revoke(ctx context.Context, id int64) error

	// WatchExpirations returns a channel receiving the full key of every
	// lease-bound key deleted under keyPrefix — whether by TTL expiry or
	// an explicit Revoke — until ctx is canceled, at which point the
	// channel is closed. Each received value is a key, not a lease ID;
	// callers derive the worker ID from the key's shape (see
	// internal/workers' KeyForWorker/WorkerIDFromKey).
	WatchExpirations(ctx context.Context, keyPrefix string) <-chan string
}
