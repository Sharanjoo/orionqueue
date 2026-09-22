package leases

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Compile-time check: EtcdManager must satisfy Manager.
var _ Manager = (*EtcdManager)(nil)
var _ Manager = (*FakeManager)(nil)

func TestFakeManagerGrantThenRenewSucceeds(t *testing.T) {
	m := NewFakeManager()
	id, err := m.Grant(context.Background(), "/workers/leases/w1", 20*time.Second)
	if err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}
	if err := m.Renew(context.Background(), id); err != nil {
		t.Fatalf("Renew returned error: %v", err)
	}
}

func TestFakeManagerRenewAfterExpireReturnsErrLeaseNotFound(t *testing.T) {
	m := NewFakeManager()
	id, err := m.Grant(context.Background(), "/workers/leases/w1", 20*time.Second)
	if err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}

	m.Expire(id)

	if err := m.Renew(context.Background(), id); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("expected ErrLeaseNotFound after Expire, got %v", err)
	}
}

func TestFakeManagerRenewUnknownLeaseReturnsErrLeaseNotFound(t *testing.T) {
	m := NewFakeManager()
	if err := m.Renew(context.Background(), 999); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("expected ErrLeaseNotFound for an unknown lease, got %v", err)
	}
}

func TestFakeManagerWatchExpirationsReceivesExpiredKey(t *testing.T) {
	m := NewFakeManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh := m.WatchExpirations(ctx, "/workers/leases/")

	id, err := m.Grant(ctx, "/workers/leases/w1", 20*time.Second)
	if err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}
	m.Expire(id)

	select {
	case key := <-watchCh:
		if key != "/workers/leases/w1" {
			t.Errorf("received key = %q, want %q", key, "/workers/leases/w1")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for expiration notification")
	}
}

func TestFakeManagerWatchExpirationsReceivesExplicitRevoke(t *testing.T) {
	m := NewFakeManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh := m.WatchExpirations(ctx, "/workers/leases/")

	id, err := m.Grant(ctx, "/workers/leases/w1", 20*time.Second)
	if err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}
	if err := m.Revoke(ctx, id); err != nil {
		t.Fatalf("Revoke returned error: %v", err)
	}

	select {
	case key := <-watchCh:
		if key != "/workers/leases/w1" {
			t.Errorf("received key = %q, want %q", key, "/workers/leases/w1")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for revoke notification")
	}
}

func TestFakeManagerWatchExpirationsClosesOnContextCancel(t *testing.T) {
	m := NewFakeManager()
	ctx, cancel := context.WithCancel(context.Background())

	watchCh := m.WatchExpirations(ctx, "/workers/leases/")
	cancel()

	select {
	case _, ok := <-watchCh:
		if ok {
			t.Error("expected the channel to be closed (no value), got a value instead")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the channel to close after context cancellation")
	}
}

func TestFakeManagerRevokeUnknownLeaseIsANoOp(t *testing.T) {
	m := NewFakeManager()
	if err := m.Revoke(context.Background(), 999); err != nil {
		t.Fatalf("expected Revoke of an unknown lease to be a no-op, got error: %v", err)
	}
}
