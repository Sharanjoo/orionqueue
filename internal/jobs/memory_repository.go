package jobs

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

// defaultPageSize and maxPageSize bound List's page size the same way
// Phase 3's PostgreSQL-backed repository will, so behavior doesn't change
// when persistence is swapped in.
const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// MemoryRepository is an in-memory Repository, safe for concurrent use.
// It's the only Repository implementation until Phase 3 adds a
// PostgreSQL-backed one — job state does not survive a process restart
// until then, which is documented in PROJECT_STATUS.md rather than hidden.
type MemoryRepository struct {
	mu             sync.Mutex
	jobsByID       map[string]Job
	idBySubmission map[string]string
}

// NewMemoryRepository returns an empty MemoryRepository.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		jobsByID:       make(map[string]Job),
		idBySubmission: make(map[string]string),
	}
}

func (r *MemoryRepository) Create(_ context.Context, job Job) (Job, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if job.SubmissionID != "" {
		if existingID, ok := r.idBySubmission[job.SubmissionID]; ok {
			return r.jobsByID[existingID], false, nil
		}
	}

	r.jobsByID[job.ID] = job
	if job.SubmissionID != "" {
		r.idBySubmission[job.SubmissionID] = job.ID
	}
	return job, true, nil
}

func (r *MemoryRepository) Get(_ context.Context, id string) (Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	job, ok := r.jobsByID[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return job, nil
}

func (r *MemoryRepository) List(_ context.Context, opts ListOptions) (ListResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	all := make([]Job, 0, len(r.jobsByID))
	for _, j := range r.jobsByID {
		if opts.StateFilter != "" && j.State != opts.StateFilter {
			continue
		}
		all = append(all, j)
	}
	sort.Slice(all, func(i, k int) bool {
		if all[i].CreatedAt.Equal(all[k].CreatedAt) {
			return all[i].ID < all[k].ID
		}
		return all[i].CreatedAt.Before(all[k].CreatedAt)
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
	page := append([]Job(nil), all[offset:end]...)

	result := ListResult{Jobs: page}
	if end < len(all) {
		result.NextPageToken = encodePageToken(end)
	}
	return result, nil
}

func (r *MemoryRepository) Update(_ context.Context, id string, fn func(Job) (Job, error)) (Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	job, ok := r.jobsByID[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	updated, err := fn(job)
	if err != nil {
		return Job{}, err
	}
	r.jobsByID[id] = updated
	return updated, nil
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
