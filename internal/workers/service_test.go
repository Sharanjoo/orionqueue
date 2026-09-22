package workers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Sharanjoo/orionqueue/internal/leases"
)

func newTestService() (*Service, Repository, *leases.FakeManager) {
	repo := NewMemoryRepository()
	fakeLeases := leases.NewFakeManager()
	fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	n := 0
	svc := NewService(repo, fakeLeases,
		WithClock(func() time.Time { return fixedNow }),
		WithIDGenerator(func() string {
			n++
			return "worker-test-" + string(rune('0'+n))
		}),
		WithLeaseTTL(20*time.Second),
	)
	return svc, repo, fakeLeases
}

func TestServiceRegisterCreatesActiveWorkerWithLease(t *testing.T) {
	svc, _, _ := newTestService()
	worker, err := svc.Register(context.Background(), validRegisterInput())
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if worker.Status != StatusActive {
		t.Errorf("Status = %q, want %q", worker.Status, StatusActive)
	}
	if worker.LeaseID == nil {
		t.Fatal("expected a non-nil LeaseID")
	}
	if len(worker.GPUs) != 2 {
		t.Errorf("len(GPUs) = %d, want 2", len(worker.GPUs))
	}
	if worker.ID == "" {
		t.Error("expected a non-empty worker ID")
	}
}

func TestServiceRegisterRejectsInvalidInput(t *testing.T) {
	svc, _, _ := newTestService()
	_, err := svc.Register(context.Background(), RegisterInput{})
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("expected *ValidationError, got %v (%T)", err, err)
	}
}

func TestServiceHeartbeatRenewsLease(t *testing.T) {
	svc, _, _ := newTestService()
	worker, err := svc.Register(context.Background(), validRegisterInput())
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	updated, err := svc.Heartbeat(context.Background(), worker.ID, nil, nil)
	if err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	if updated.LeaseID == nil || *updated.LeaseID != *worker.LeaseID {
		t.Error("expected the same lease ID after a successful renewal")
	}
	if updated.Status != StatusActive {
		t.Errorf("Status = %q, want %q", updated.Status, StatusActive)
	}
}

func TestServiceHeartbeatUpdatesGPUInventoryAndRunningJobs(t *testing.T) {
	svc, _, _ := newTestService()
	worker, err := svc.Register(context.Background(), validRegisterInput())
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	newGPUs := []GPU{{DeviceIndex: 0, UUID: "GPU-fake-0", TotalMemoryBytes: 16 << 30, UtilizationPercent: 42}}
	updated, err := svc.Heartbeat(context.Background(), worker.ID, newGPUs, []string{"job-1"})
	if err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	if len(updated.GPUs) != 1 || updated.GPUs[0].UtilizationPercent != 42 {
		t.Errorf("GPUs = %+v, want the updated single-GPU inventory", updated.GPUs)
	}
	if len(updated.RunningJobIDs) != 1 || updated.RunningJobIDs[0] != "job-1" {
		t.Errorf("RunningJobIDs = %v, want [job-1]", updated.RunningJobIDs)
	}
}

func TestServiceHeartbeatSelfHealsAfterLeaseExpiry(t *testing.T) {
	svc, _, fakeLeases := newTestService()
	worker, err := svc.Register(context.Background(), validRegisterInput())
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	fakeLeases.Expire(*worker.LeaseID)

	updated, err := svc.Heartbeat(context.Background(), worker.ID, nil, nil)
	if err != nil {
		t.Fatalf("expected Heartbeat to self-heal after lease expiry, got error: %v", err)
	}
	if updated.Status != StatusActive {
		t.Errorf("Status = %q, want %q after self-healing", updated.Status, StatusActive)
	}
	if updated.LeaseID == nil || *updated.LeaseID == *worker.LeaseID {
		t.Error("expected a new lease ID after self-healing, got the same (or nil) one")
	}
}

func TestServiceHeartbeatMissingWorkerReturnsErrNotFound(t *testing.T) {
	svc, _, _ := newTestService()
	_, err := svc.Heartbeat(context.Background(), "does-not-exist", nil, nil)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestServiceMarkLostIsIdempotent(t *testing.T) {
	svc, _, _ := newTestService()
	worker, err := svc.Register(context.Background(), validRegisterInput())
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	first, err := svc.MarkLost(context.Background(), worker.ID)
	if err != nil {
		t.Fatalf("first MarkLost returned error: %v", err)
	}
	if first.Status != StatusLost || first.LeaseID != nil {
		t.Errorf("expected Status=LOST and LeaseID=nil, got Status=%q LeaseID=%v", first.Status, first.LeaseID)
	}

	second, err := svc.MarkLost(context.Background(), worker.ID)
	if err != nil {
		t.Fatalf("second MarkLost returned error: %v", err)
	}
	if second.Status != StatusLost {
		t.Errorf("expected idempotent MarkLost to leave Status=LOST, got %q", second.Status)
	}
}

func TestServiceWatchExpirationsMarksWorkerLostAutomatically(t *testing.T) {
	svc, repo, fakeLeases := newTestService()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, err := svc.Register(ctx, validRegisterInput())
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		svc.WatchExpirations(ctx, nil, nil)
		close(done)
	}()
	waitForWatcher(t, fakeLeases)

	// Simulate the worker's lease TTL elapsing without a heartbeat —
	// exactly what happens in real etcd when a worker stops heartbeating.
	fakeLeases.Expire(*worker.LeaseID)

	deadline := time.After(2 * time.Second)
	for {
		got, err := repo.Get(ctx, worker.ID)
		if err != nil {
			t.Fatalf("Get returned error: %v", err)
		}
		if got.Status == StatusLost {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the worker to be marked LOST automatically")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WatchExpirations did not return after context cancellation")
	}
}

func TestServiceWatchExpirationsCallsOnLostWithTheWorkerID(t *testing.T) {
	svc, _, fakeLeases := newTestService()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, err := svc.Register(ctx, validRegisterInput())
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	lostCh := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		svc.WatchExpirations(ctx, func(workerID string) { lostCh <- workerID }, nil)
		close(done)
	}()
	waitForWatcher(t, fakeLeases)

	fakeLeases.Expire(*worker.LeaseID)

	select {
	case gotID := <-lostCh:
		if gotID != worker.ID {
			t.Errorf("onLost called with worker ID %q, want %q", gotID, worker.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for onLost to be called")
	}

	cancel()
	<-done
}

func TestServiceWatchExpirationsCallsOnErrorOnRepositoryFailure(t *testing.T) {
	failingRepo := &updateFailsRepository{Repository: NewMemoryRepository(), err: errors.New("boom")}
	fakeLeases := leases.NewFakeManager()
	svc := NewService(failingRepo, fakeLeases)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leaseID, err := fakeLeases.Grant(ctx, LeaseKey("worker-x"), 20*time.Second)
	if err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}
	if _, err := failingRepo.Repository.Create(ctx, Worker{ID: "worker-x", Status: StatusActive, LeaseID: &leaseID}); err != nil {
		t.Fatalf("test setup Create returned error: %v", err)
	}

	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		svc.WatchExpirations(ctx, nil, func(workerID string, err error) {
			errCh <- err
		})
		close(done)
	}()
	waitForWatcher(t, fakeLeases)

	fakeLeases.Expire(leaseID)

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected a non-nil error via onError")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for onError to be called")
	}

	cancel()
	<-done
}

// waitForWatcher blocks until fakeLeases has at least one registered
// WatchExpirations channel, or fails the test after a timeout. Needed
// because establishing a watch is inherently asynchronous relative to
// the goroutine that starts it — the same is true of a real etcd Watch
// call — so a test must not fire an event until the watch is actually in
// place, or it will (correctly) be missed.
func waitForWatcher(t *testing.T, fakeLeases *leases.FakeManager) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for fakeLeases.WatcherCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for a watcher to be registered")
		case <-time.After(time.Millisecond):
		}
	}
}

// updateFailsRepository wraps a Repository but makes every Update fail,
// used to test WatchExpirations' onError callback.
type updateFailsRepository struct {
	Repository
	err error
}

func (r *updateFailsRepository) Update(context.Context, string, func(Worker) (Worker, error)) (Worker, error) {
	return Worker{}, r.err
}
