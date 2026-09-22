package workers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Sharanjoo/orionqueue/internal/leases"
)

// ErrInvalidState is returned when an operation is requested against a
// worker whose current state doesn't allow it.
var ErrInvalidState = errors.New("worker is not in a valid state for this operation")

const (
	// DefaultLeaseTTL is how long a worker's etcd lease lives without a
	// renewing heartbeat before etcd expires it — the mechanism behind
	// automatic worker-loss detection.
	DefaultLeaseTTL = 20 * time.Second
	// heartbeatFractionOfTTL: worker agents are told to heartbeat at
	// TTL/heartbeatFractionOfTTL, giving several missed-heartbeat
	// chances before the lease actually expires, so a single slow
	// request doesn't flap a worker to LOST.
	heartbeatFractionOfTTL = 4

	leaseKeyPrefix = "/orionqueue/workers/leases/"
)

// LeaseKey returns the etcd key a worker's liveness lease is bound to.
func LeaseKey(workerID string) string {
	return leaseKeyPrefix + workerID
}

// WorkerIDFromLeaseKey extracts the worker ID from a key produced by
// LeaseKey, as delivered by leases.Manager.WatchExpirations. ok is false
// if key doesn't have the expected shape (e.g. a key from an unrelated
// prefix, which shouldn't happen given WatchExpirations is always called
// with leaseKeyPrefix, but is checked rather than assumed).
func WorkerIDFromLeaseKey(key string) (id string, ok bool) {
	if !strings.HasPrefix(key, leaseKeyPrefix) {
		return "", false
	}
	id = strings.TrimPrefix(key, leaseKeyPrefix)
	return id, id != ""
}

// Clock abstracts time.Now so tests can control timestamps deterministically.
type Clock func() time.Time

// IDGenerator abstracts worker ID generation so tests can supply
// deterministic IDs instead of random ones.
type IDGenerator func() string

// Service implements OrionQueue's worker business logic (registration,
// heartbeat, listing, automatic loss detection) on top of a Repository
// and a leases.Manager. It knows nothing about gRPC or protobuf —
// internal/api's server implementation is a thin adapter around this
// type, the same split jobs.Service established in Phase 2.
type Service struct {
	repo     Repository
	leases   leases.Manager
	now      Clock
	newID    IDGenerator
	leaseTTL time.Duration
}

// ServiceOption customizes a Service returned by NewService.
type ServiceOption func(*Service)

func WithClock(c Clock) ServiceOption { return func(s *Service) { s.now = c } }
func WithIDGenerator(g IDGenerator) ServiceOption {
	return func(s *Service) { s.newID = g }
}
func WithLeaseTTL(ttl time.Duration) ServiceOption {
	return func(s *Service) { s.leaseTTL = ttl }
}

// NewService returns a Service backed by repo and leaseManager.
func NewService(repo Repository, leaseManager leases.Manager, opts ...ServiceOption) *Service {
	s := &Service{
		repo:     repo,
		leases:   leaseManager,
		now:      time.Now,
		newID:    func() string { return newRandomID("worker") },
		leaseTTL: DefaultLeaseTTL,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// HeartbeatInterval is how often a registered worker should call
// Heartbeat to stay ahead of lease expiration — returned to the caller
// (internal/api's RegisterWorker response) so the worker agent doesn't
// have to know the TTL/fraction policy itself.
func (s *Service) HeartbeatInterval() time.Duration {
	interval := s.leaseTTL / heartbeatFractionOfTTL
	if interval < time.Second {
		interval = time.Second
	}
	return interval
}

// Register validates in, grants a new etcd lease for the worker, and
// creates its record in state ACTIVE.
func (s *Service) Register(ctx context.Context, in RegisterInput) (Worker, error) {
	if err := ValidateRegister(in); err != nil {
		return Worker{}, err
	}

	id := s.newID()
	leaseID, err := s.leases.Grant(ctx, LeaseKey(id), s.leaseTTL)
	if err != nil {
		return Worker{}, fmt.Errorf("workers: grant lease: %w", err)
	}

	now := s.now().UTC()
	worker := Worker{
		ID:                  id,
		Hostname:            in.Hostname,
		Status:              StatusActive,
		LeaseID:             &leaseID,
		CPUCapacity:         in.CPUCapacity,
		MemoryCapacityBytes: in.MemoryCapacityBytes,
		GPUs:                in.GPUs,
		SoftwareVersion:     in.SoftwareVersion,
		Labels:              in.Labels,
		RegisteredAt:        now,
		LastHeartbeatAt:     now,
	}

	return s.repo.Create(ctx, worker)
}

// Heartbeat renews the worker's lease and refreshes its record. If the
// lease already expired (the previous heartbeat was late enough that
// etcd gave up on it before this one arrived — e.g. after a long GC
// pause or a network partition longer than the lease TTL), Heartbeat
// self-heals by granting a fresh lease under the same worker ID and
// resuming ACTIVE, rather than forcing the worker agent to notice its
// lease died and call Register again itself.
func (s *Service) Heartbeat(ctx context.Context, workerID string, gpus []GPU, runningJobIDs []string) (Worker, error) {
	current, err := s.repo.Get(ctx, workerID)
	if err != nil {
		return Worker{}, err
	}

	var newLeaseID *int64
	if current.LeaseID == nil {
		// Already marked LOST by the expiration watcher; treat this
		// heartbeat as a fresh registration under the existing ID.
		id, err := s.leases.Grant(ctx, LeaseKey(workerID), s.leaseTTL)
		if err != nil {
			return Worker{}, fmt.Errorf("workers: grant lease for recovering worker: %w", err)
		}
		newLeaseID = &id
	} else if err := s.leases.Renew(ctx, *current.LeaseID); err != nil {
		if !errors.Is(err, leases.ErrLeaseNotFound) {
			return Worker{}, fmt.Errorf("workers: renew lease: %w", err)
		}
		id, grantErr := s.leases.Grant(ctx, LeaseKey(workerID), s.leaseTTL)
		if grantErr != nil {
			return Worker{}, fmt.Errorf("workers: re-grant lease after expiry: %w", grantErr)
		}
		newLeaseID = &id
	}

	return s.repo.Update(ctx, workerID, func(w Worker) (Worker, error) {
		w.Status = StatusActive
		w.LastHeartbeatAt = s.now().UTC()
		if newLeaseID != nil {
			w.LeaseID = newLeaseID
		}
		if gpus != nil {
			w.GPUs = gpus
		}
		if runningJobIDs != nil {
			w.RunningJobIDs = runningJobIDs
		}
		return w, nil
	})
}

// MarkLost transitions a worker to LOST. Idempotent: called with a worker
// already LOST is a no-op. Called by WatchExpirations when the worker's
// etcd lease expires — never called directly by a client-facing RPC.
func (s *Service) MarkLost(ctx context.Context, workerID string) (Worker, error) {
	return s.repo.Update(ctx, workerID, func(w Worker) (Worker, error) {
		if w.Status == StatusLost {
			return w, nil
		}
		w.Status = StatusLost
		w.LeaseID = nil
		return w, nil
	})
}

// Get returns the worker with the given ID, or ErrNotFound.
func (s *Service) Get(ctx context.Context, id string) (Worker, error) {
	return s.repo.Get(ctx, id)
}

// List returns a page of workers matching opts.
func (s *Service) List(ctx context.Context, opts ListOptions) (ListResult, error) {
	return s.repo.List(ctx, opts)
}

// ListActive returns every ACTIVE worker (with GPU inventory),
// unpaginated — see Repository.ListActive.
func (s *Service) ListActive(ctx context.Context) ([]Worker, error) {
	return s.repo.ListActive(ctx)
}

// WatchExpirations watches for worker lease expirations and marks the
// corresponding worker LOST — this is what makes worker-loss detection
// automatic rather than something a client has to poll for. Intended to
// run for the lifetime of the process (started as a goroutine in
// cmd/api's run()); returns once ctx is canceled or the underlying watch
// channel closes. This package doesn't import a logger itself, so the
// caller decides how to report outcomes: onLost, if non-nil, is called
// after a worker is successfully marked LOST; onError, if non-nil, is
// called for any failure doing so (e.g. a transient database error).
func (s *Service) WatchExpirations(ctx context.Context, onLost func(workerID string), onError func(workerID string, err error)) {
	ch := s.leases.WatchExpirations(ctx, leaseKeyPrefix)
	for key := range ch {
		workerID, ok := WorkerIDFromLeaseKey(key)
		if !ok {
			continue
		}
		_, err := s.MarkLost(ctx, workerID)
		switch {
		case err == nil:
			if onLost != nil {
				onLost(workerID)
			}
		case errors.Is(err, ErrNotFound):
			// Nothing to mark lost (e.g. a lease outliving a since-removed
			// worker record) — not an error worth reporting.
		default:
			if onError != nil {
				onError(workerID, err)
			}
		}
	}
}
