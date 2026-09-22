package leases

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// waitForEtcdMaxAttempts and waitForEtcdBaseDelay bound WaitForEtcd's
// retry loop the same way persistence.Connect bounds its Postgres
// equivalent.
const (
	waitForEtcdMaxAttempts = 10
	waitForEtcdBaseDelay   = 300 * time.Millisecond
)

// WaitForEtcd checks etcd connectivity with a bounded, increasing-delay
// retry — etcd inside Docker Compose can still be starting even after a
// dependent service's own container is already up. Returns an error only
// once etcd is still unreachable after every attempt; callers should
// treat that as fatal at startup. Shared by cmd/api (worker leases) and
// cmd/scheduler (leader election) so this startup-ordering accommodation
// is written once.
func WaitForEtcd(ctx context.Context, client *clientv3.Client) error {
	var lastErr error
	for attempt := 1; attempt <= waitForEtcdMaxAttempts; attempt++ {
		getCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, lastErr = client.Get(getCtx, "orionqueue-startup-probe")
		cancel()
		if lastErr == nil {
			return nil
		}
		if attempt == waitForEtcdMaxAttempts {
			break
		}
		select {
		case <-time.After(waitForEtcdBaseDelay * time.Duration(attempt)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("etcd unreachable after %d attempts: %w", waitForEtcdMaxAttempts, lastErr)
}
