package leases

import (
	"context"
	"sync"
	"time"
)

// FakeManager is an in-memory Manager for unit tests that need lease
// semantics without a real etcd — internal/workers' tests use this, the
// same way internal/jobs' tests use MemoryRepository instead of a real
// PostgreSQL. It never expires leases on a real clock; tests call Expire
// explicitly to simulate a lease's TTL elapsing.
type FakeManager struct {
	mu       sync.Mutex
	nextID   int64
	keyByID  map[int64]string
	watchers []chan string
}

// NewFakeManager returns an empty FakeManager.
func NewFakeManager() *FakeManager {
	return &FakeManager{keyByID: make(map[int64]string)}
}

func (m *FakeManager) Grant(_ context.Context, key string, _ time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	m.keyByID[m.nextID] = key
	return m.nextID, nil
}

func (m *FakeManager) Renew(_ context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.keyByID[id]; !ok {
		return ErrLeaseNotFound
	}
	return nil
}

func (m *FakeManager) Revoke(_ context.Context, id int64) error {
	m.mu.Lock()
	key, ok := m.keyByID[id]
	if ok {
		delete(m.keyByID, id)
	}
	watchers := append([]chan string(nil), m.watchers...)
	m.mu.Unlock()

	if ok {
		for _, w := range watchers {
			w <- key
		}
	}
	return nil
}

func (m *FakeManager) WatchExpirations(ctx context.Context, _ string) <-chan string {
	ch := make(chan string, 16)
	m.mu.Lock()
	m.watchers = append(m.watchers, ch)
	m.mu.Unlock()

	go func() {
		<-ctx.Done()
		m.mu.Lock()
		defer m.mu.Unlock()
		for i, w := range m.watchers {
			if w == ch {
				m.watchers = append(m.watchers[:i], m.watchers[i+1:]...)
				break
			}
		}
		close(ch)
	}()

	return ch
}

// WatcherCount reports how many WatchExpirations channels are currently
// registered — test-only introspection, used to wait for a watch to
// actually be established before triggering an event that a watch set up
// afterward would (correctly, matching real etcd's semantics) miss.
func (m *FakeManager) WatcherCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.watchers)
}

// Expire simulates lease id's TTL elapsing without a Renew call: it
// removes the lease and — exactly like a real etcd lease expiring —
// sends its bound key to every active WatchExpirations channel.
func (m *FakeManager) Expire(id int64) {
	m.mu.Lock()
	key, ok := m.keyByID[id]
	if ok {
		delete(m.keyByID, id)
	}
	watchers := append([]chan string(nil), m.watchers...)
	m.mu.Unlock()

	if ok {
		for _, w := range watchers {
			w <- key
		}
	}
}
