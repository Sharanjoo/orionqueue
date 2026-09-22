package leases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdManager is the real, etcd-backed Manager. internal/workers.Service
// depends on the Manager interface, not this type directly, so its tests
// can use a fake instead of a live etcd (see internal/leases/fake.go);
// EtcdManager itself is exercised by tests/integration against a real
// etcd container.
type EtcdManager struct {
	client *clientv3.Client
}

// NewEtcdManager returns an EtcdManager using client. The caller owns
// client's lifecycle (created via clientv3.New, closed by the caller).
func NewEtcdManager(client *clientv3.Client) *EtcdManager {
	return &EtcdManager{client: client}
}

func (m *EtcdManager) Grant(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	seconds := int64(ttl.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	lease, err := m.client.Grant(ctx, seconds)
	if err != nil {
		return 0, fmt.Errorf("leases: grant: %w", err)
	}
	if _, err := m.client.Put(ctx, key, "", clientv3.WithLease(lease.ID)); err != nil {
		return 0, fmt.Errorf("leases: bind key to lease: %w", err)
	}
	return int64(lease.ID), nil
}

func (m *EtcdManager) Renew(ctx context.Context, id int64) error {
	_, err := m.client.KeepAliveOnce(ctx, clientv3.LeaseID(id))
	if err != nil {
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return ErrLeaseNotFound
		}
		return fmt.Errorf("leases: renew: %w", err)
	}
	return nil
}

func (m *EtcdManager) Revoke(ctx context.Context, id int64) error {
	if _, err := m.client.Revoke(ctx, clientv3.LeaseID(id)); err != nil {
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return nil // already gone; Revoke is idempotent from the caller's point of view
		}
		return fmt.Errorf("leases: revoke: %w", err)
	}
	return nil
}

func (m *EtcdManager) WatchExpirations(ctx context.Context, keyPrefix string) <-chan string {
	out := make(chan string)
	watchCh := m.client.Watch(ctx, keyPrefix, clientv3.WithPrefix())

	go func() {
		defer close(out)
		for resp := range watchCh {
			for _, ev := range resp.Events {
				if ev.Type != clientv3.EventTypeDelete {
					continue
				}
				select {
				case out <- string(ev.Kv.Key):
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}
