package workers

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// MemoryRepository is an in-memory Repository, safe for concurrent use —
// used by internal/workers' and internal/api's fast, Docker-free tests.
// cmd/api uses the PostgreSQL-backed implementation in
// internal/persistence instead, the same split Phase 2/3 established for
// jobs.
type MemoryRepository struct {
	mu          sync.Mutex
	workersByID map[string]Worker
}

// NewMemoryRepository returns an empty MemoryRepository.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{workersByID: make(map[string]Worker)}
}

func (r *MemoryRepository) Create(_ context.Context, worker Worker) (Worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workersByID[worker.ID] = worker
	return worker, nil
}

func (r *MemoryRepository) Get(_ context.Context, id string) (Worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workersByID[id]
	if !ok {
		return Worker{}, ErrNotFound
	}
	return w, nil
}

func (r *MemoryRepository) List(_ context.Context, opts ListOptions) (ListResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	all := make([]Worker, 0, len(r.workersByID))
	for _, w := range r.workersByID {
		if opts.StatusFilter != "" && w.Status != opts.StatusFilter {
			continue
		}
		all = append(all, w)
	}
	sort.Slice(all, func(i, k int) bool {
		if all[i].RegisteredAt.Equal(all[k].RegisteredAt) {
			return all[i].ID < all[k].ID
		}
		return all[i].RegisteredAt.Before(all[k].RegisteredAt)
	})

	offset := 0
	if opts.PageToken != "" {
		var err error
		offset, err = decodePageToken(opts.PageToken)
		if err != nil {
			return ListResult{}, fmt.Errorf("invalid page_token: %w", err)
		}
	}
	if offset < 0 || offset > len(all) {
		offset = len(all)
	}

	pageSize := int(opts.PageSize)
	if pageSize <= 0 || pageSize > maxPageSize {
		pageSize = defaultPageSize
	}

	end := offset + pageSize
	if end > len(all) {
		end = len(all)
	}
	page := append([]Worker(nil), all[offset:end]...)

	result := ListResult{Workers: page}
	if end < len(all) {
		result.NextPageToken = encodePageToken(end)
	}
	return result, nil
}

func (r *MemoryRepository) Update(_ context.Context, id string, fn func(Worker) (Worker, error)) (Worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.workersByID[id]
	if !ok {
		return Worker{}, ErrNotFound
	}
	updated, err := fn(w)
	if err != nil {
		return Worker{}, err
	}
	r.workersByID[id] = updated
	return updated, nil
}

func (r *MemoryRepository) ListActive(_ context.Context) ([]Worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	active := make([]Worker, 0, len(r.workersByID))
	for _, w := range r.workersByID {
		if w.Status == StatusActive {
			active = append(active, w)
		}
	}
	sort.Slice(active, func(i, k int) bool {
		if active[i].RegisteredAt.Equal(active[k].RegisteredAt) {
			return active[i].ID < active[k].ID
		}
		return active[i].RegisteredAt.Before(active[k].RegisteredAt)
	})
	return active, nil
}

func encodePageToken(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func decodePageToken(token string) (int, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(b))
}
