package workers

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryRepositoryCreateAndGet(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()

	created, err := repo.Create(ctx, Worker{ID: "worker-1", Hostname: "h1", Status: StatusActive, RegisteredAt: time.Now()})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if created.ID != "worker-1" {
		t.Errorf("ID = %q, want %q", created.ID, "worker-1")
	}

	got, err := repo.Get(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Hostname != "h1" {
		t.Errorf("Hostname = %q, want %q", got.Hostname, "h1")
	}
}

func TestMemoryRepositoryGetMissingReturnsErrNotFound(t *testing.T) {
	repo := NewMemoryRepository()
	_, err := repo.Get(context.Background(), "does-not-exist")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryRepositoryListOrdersAndFilters(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	_, _ = repo.Create(ctx, Worker{ID: "worker-b", Status: StatusActive, RegisteredAt: base.Add(time.Second)})
	_, _ = repo.Create(ctx, Worker{ID: "worker-a", Status: StatusLost, RegisteredAt: base})

	all, err := repo.List(ctx, ListOptions{})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(all.Workers) != 2 || all.Workers[0].ID != "worker-a" {
		t.Fatalf("expected [worker-a, worker-b] in registration order, got %+v", all.Workers)
	}

	active, err := repo.List(ctx, ListOptions{StatusFilter: StatusActive})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(active.Workers) != 1 || active.Workers[0].ID != "worker-b" {
		t.Fatalf("expected only worker-b (ACTIVE), got %+v", active.Workers)
	}
}

func TestMemoryRepositoryUpdateAppliesChangeUnderLock(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	_, _ = repo.Create(ctx, Worker{ID: "worker-1", Status: StatusActive})

	updated, err := repo.Update(ctx, "worker-1", func(w Worker) (Worker, error) {
		w.Status = StatusLost
		return w, nil
	})
	if err != nil {
		t.Fatalf("Update returned error: %v", err)
	}
	if updated.Status != StatusLost {
		t.Errorf("Status = %q, want %q", updated.Status, StatusLost)
	}

	got, err := repo.Get(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != StatusLost {
		t.Errorf("stored Status = %q, want %q (Update must persist)", got.Status, StatusLost)
	}
}

func TestMemoryRepositoryUpdateMissingReturnsErrNotFound(t *testing.T) {
	repo := NewMemoryRepository()
	_, err := repo.Update(context.Background(), "does-not-exist", func(w Worker) (Worker, error) { return w, nil })
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
