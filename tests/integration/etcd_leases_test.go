//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	tcetcd "github.com/testcontainers/testcontainers-go/modules/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/Sharanjoo/orionqueue/internal/leases"
)

// newTestEtcdManager starts a real, disposable single-node etcd container
// (the same image docker-compose.yml uses) and returns a leases.EtcdManager
// connected to it.
func newTestEtcdManager(t *testing.T) *leases.EtcdManager {
	t.Helper()
	ctx := context.Background()

	container, err := tcetcd.Run(ctx, "gcr.io/etcd-development/etcd:v3.5.17")
	if err != nil {
		t.Fatalf("failed to start etcd container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate etcd container: %v", err)
		}
	})

	endpoint, err := container.ClientEndpoint(ctx)
	if err != nil {
		t.Fatalf("failed to get client endpoint: %v", err)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create etcd client: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	return leases.NewEtcdManager(client)
}

func TestEtcdManager_GrantAndRenew(t *testing.T) {
	m := newTestEtcdManager(t)
	ctx := context.Background()

	id, err := m.Grant(ctx, "/test/leases/w1", 10*time.Second)
	if err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}
	if err := m.Renew(ctx, id); err != nil {
		t.Fatalf("Renew returned error: %v", err)
	}
}

func TestEtcdManager_LeaseExpiresWithoutRenewal(t *testing.T) {
	m := newTestEtcdManager(t)
	ctx := context.Background()

	// Real, actual expiry — this test does not renew and waits past the
	// TTL, exercising etcd's own expiration mechanism rather than
	// anything simulated.
	id, err := m.Grant(ctx, "/test/leases/w2", 3*time.Second)
	if err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}

	time.Sleep(6 * time.Second)

	if err := m.Renew(ctx, id); !errors.Is(err, leases.ErrLeaseNotFound) {
		t.Fatalf("expected ErrLeaseNotFound after the lease's TTL elapsed, got %v", err)
	}
}

// TestEtcdManager_WatchExpirationsReceivesExpiredKey is the most direct
// possible proof that Phase 4's automatic worker-loss detection mechanism
// works against real etcd, not just leases.FakeManager (already covered
// by internal/workers' unit tests): grant a short-lived lease, never
// renew it, and confirm WatchExpirations reports its key once etcd
// actually expires it.
func TestEtcdManager_WatchExpirationsReceivesExpiredKey(t *testing.T) {
	m := newTestEtcdManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh := m.WatchExpirations(ctx, "/test/leases/")

	if _, err := m.Grant(ctx, "/test/leases/w3", 3*time.Second); err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}

	select {
	case key := <-watchCh:
		if key != "/test/leases/w3" {
			t.Errorf("received key = %q, want %q", key, "/test/leases/w3")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for a real etcd lease expiration notification")
	}
}

func TestEtcdManager_Revoke(t *testing.T) {
	m := newTestEtcdManager(t)
	ctx := context.Background()

	id, err := m.Grant(ctx, "/test/leases/w4", 30*time.Second)
	if err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}
	if err := m.Revoke(ctx, id); err != nil {
		t.Fatalf("Revoke returned error: %v", err)
	}
	if err := m.Renew(ctx, id); !errors.Is(err, leases.ErrLeaseNotFound) {
		t.Fatalf("expected ErrLeaseNotFound after Revoke, got %v", err)
	}
}

func TestEtcdManager_RevokeUnknownLeaseIsANoOp(t *testing.T) {
	m := newTestEtcdManager(t)
	if err := m.Revoke(context.Background(), 999999); err != nil {
		t.Fatalf("expected Revoke of an unknown lease to be a no-op, got error: %v", err)
	}
}
