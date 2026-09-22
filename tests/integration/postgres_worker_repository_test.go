//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Sharanjoo/orionqueue/internal/persistence"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

func newTestWorkerRepository(t *testing.T, dsn string) *persistence.WorkerRepository {
	t.Helper()
	pool, err := persistence.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}
	t.Cleanup(pool.Close)
	return persistence.NewWorkerRepository(pool)
}

func validWorker(id string) workers.Worker {
	temp := 65.5
	leaseID := int64(42)
	now := time.Now().UTC().Truncate(time.Microsecond)
	return workers.Worker{
		ID:                  id,
		Hostname:            "worker-1.local",
		Status:              workers.StatusActive,
		LeaseID:             &leaseID,
		CPUCapacity:         8,
		MemoryCapacityBytes: 32 << 30,
		GPUs: []workers.GPU{
			{
				DeviceIndex:          0,
				UUID:                 "GPU-fake-0",
				TotalMemoryBytes:     16 << 30,
				AllocatedMemoryBytes: 0,
				UtilizationPercent:   12.5,
				TemperatureCelsius:   &temp,
				HealthState:          workers.GPUHealthy,
			},
			{
				DeviceIndex:      1,
				UUID:             "GPU-fake-1",
				TotalMemoryBytes: 16 << 30,
				HealthState:      workers.GPUHealthy,
			},
		},
		SoftwareVersion: "0.1.0",
		Labels:          map[string]string{"env": "test"},
		RegisteredAt:    now,
		LastHeartbeatAt: now,
	}
}

func TestPostgresWorkerRepository_CreateAndGet(t *testing.T) {
	dsn := newTestDatabase(t) // reuses Phase 3's Postgres+migrations helper from postgres_job_repository_test.go
	repo := newTestWorkerRepository(t, dsn)
	ctx := context.Background()

	created, err := repo.Create(ctx, validWorker("worker-pg-1"))
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if len(created.GPUs) != 2 {
		t.Fatalf("expected 2 GPUs on create, got %d", len(created.GPUs))
	}

	got, err := repo.Get(ctx, "worker-pg-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Hostname != "worker-1.local" {
		t.Errorf("Hostname = %q, want %q", got.Hostname, "worker-1.local")
	}
	if len(got.GPUs) != 2 {
		t.Fatalf("expected 2 GPUs on get, got %d", len(got.GPUs))
	}
	if got.GPUs[0].TemperatureCelsius == nil || *got.GPUs[0].TemperatureCelsius != 65.5 {
		t.Errorf("GPUs[0].TemperatureCelsius = %v, want 65.5", got.GPUs[0].TemperatureCelsius)
	}
	if got.GPUs[1].TemperatureCelsius != nil {
		t.Errorf("GPUs[1].TemperatureCelsius = %v, want nil (never set)", *got.GPUs[1].TemperatureCelsius)
	}
	if got.LeaseID == nil || *got.LeaseID != 42 {
		t.Errorf("LeaseID = %v, want 42", got.LeaseID)
	}
	if got.Labels["env"] != "test" {
		t.Errorf("Labels[env] = %q, want %q", got.Labels["env"], "test")
	}
}

func TestPostgresWorkerRepository_WorkerSurvivesReconnection(t *testing.T) {
	dsn := newTestDatabase(t)
	ctx := context.Background()

	first := newTestWorkerRepository(t, dsn)
	if _, err := first.Create(ctx, validWorker("worker-restart-1")); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	second := newTestWorkerRepository(t, dsn)
	got, err := second.Get(ctx, "worker-restart-1")
	if err != nil {
		t.Fatalf("worker did not survive reconnection: Get returned error: %v", err)
	}
	if got.ID != "worker-restart-1" {
		t.Errorf("ID = %q, want %q", got.ID, "worker-restart-1")
	}
}

func TestPostgresWorkerRepository_GetMissingReturnsErrNotFound(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestWorkerRepository(t, dsn)

	_, err := repo.Get(context.Background(), "does-not-exist")
	if !errors.Is(err, workers.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPostgresWorkerRepository_ListFiltersByStatus(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestWorkerRepository(t, dsn)
	ctx := context.Background()

	active := validWorker("worker-pg-active")
	lost := validWorker("worker-pg-lost")
	lost.Status = workers.StatusLost
	lost.LeaseID = nil
	lost.GPUs = nil

	if _, err := repo.Create(ctx, active); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, err := repo.Create(ctx, lost); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	result, err := repo.List(ctx, workers.ListOptions{StatusFilter: workers.StatusLost})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(result.Workers) != 1 || result.Workers[0].ID != "worker-pg-lost" {
		t.Fatalf("expected only worker-pg-lost, got %+v", result.Workers)
	}
	if result.Workers[0].LeaseID != nil {
		t.Errorf("expected nil LeaseID for a LOST worker, got %v", result.Workers[0].LeaseID)
	}
}

func TestPostgresWorkerRepository_UpdateReplacesGPUInventory(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestWorkerRepository(t, dsn)
	ctx := context.Background()

	if _, err := repo.Create(ctx, validWorker("worker-pg-update")); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	updated, err := repo.Update(ctx, "worker-pg-update", func(w workers.Worker) (workers.Worker, error) {
		w.Status = workers.StatusLost
		w.LeaseID = nil
		w.GPUs = nil // heartbeat/mark-lost with no GPU update means "clear inventory" here
		return w, nil
	})
	if err != nil {
		t.Fatalf("Update returned error: %v", err)
	}
	if updated.Status != workers.StatusLost {
		t.Errorf("Status = %q, want %q", updated.Status, workers.StatusLost)
	}

	got, err := repo.Get(ctx, "worker-pg-update")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if len(got.GPUs) != 0 {
		t.Errorf("expected GPU inventory to be replaced with none, got %d rows", len(got.GPUs))
	}
	if got.LeaseID != nil {
		t.Errorf("expected LeaseID to be nil after being cleared, got %v", got.LeaseID)
	}
}

func TestPostgresWorkerRepository_UpdateMissingReturnsErrNotFound(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestWorkerRepository(t, dsn)

	_, err := repo.Update(context.Background(), "does-not-exist", func(w workers.Worker) (workers.Worker, error) { return w, nil })
	if !errors.Is(err, workers.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
