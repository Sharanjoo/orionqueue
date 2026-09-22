package jobs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryRepositoryCreateAndGet(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()

	job := Job{ID: "job-1", Name: "first", State: StateQueued, CreatedAt: time.Now()}
	created, wasNew, err := repo.Create(ctx, job)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if !wasNew {
		t.Fatal("expected wasNew=true for a fresh job")
	}
	if created.ID != "job-1" {
		t.Errorf("created.ID = %q, want %q", created.ID, "job-1")
	}

	got, err := repo.Get(ctx, "job-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Name != "first" {
		t.Errorf("got.Name = %q, want %q", got.Name, "first")
	}
}

func TestMemoryRepositoryGetMissingReturnsErrNotFound(t *testing.T) {
	repo := NewMemoryRepository()
	_, err := repo.Get(context.Background(), "does-not-exist")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryRepositoryCreateIsIdempotentBySubmissionID(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()

	first, wasNew1, err := repo.Create(ctx, Job{ID: "job-1", SubmissionID: "sub-1", Name: "a", State: StateQueued})
	if err != nil {
		t.Fatalf("first Create returned error: %v", err)
	}
	if !wasNew1 {
		t.Fatal("expected wasNew=true for the first submission")
	}

	second, wasNew2, err := repo.Create(ctx, Job{ID: "job-2", SubmissionID: "sub-1", Name: "b", State: StateQueued})
	if err != nil {
		t.Fatalf("second Create returned error: %v", err)
	}
	if wasNew2 {
		t.Fatal("expected wasNew=false for a duplicate submission ID")
	}
	if second.ID != first.ID {
		t.Errorf("second.ID = %q, want the original job's ID %q", second.ID, first.ID)
	}
	if second.Name != "a" {
		t.Errorf("second.Name = %q, want the original job's name %q (duplicate must not overwrite)", second.Name, "a")
	}

	all, err := repo.List(ctx, ListOptions{})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(all.Jobs) != 1 {
		t.Fatalf("expected exactly 1 stored job after a duplicate submission, got %d", len(all.Jobs))
	}
}

func TestMemoryRepositoryCreateWithoutSubmissionIDNeverDeduplicates(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()

	if _, _, err := repo.Create(ctx, Job{ID: "job-1", Name: "a", State: StateQueued}); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, _, err := repo.Create(ctx, Job{ID: "job-2", Name: "b", State: StateQueued}); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	all, err := repo.List(ctx, ListOptions{})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(all.Jobs) != 2 {
		t.Fatalf("expected 2 distinct jobs with no submission ID, got %d", len(all.Jobs))
	}
}

func TestMemoryRepositoryListOrdersByCreatedAtThenID(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, _, _ = repo.Create(ctx, Job{ID: "job-c", State: StateQueued, CreatedAt: base.Add(2 * time.Second)})
	_, _, _ = repo.Create(ctx, Job{ID: "job-a", State: StateQueued, CreatedAt: base})
	_, _, _ = repo.Create(ctx, Job{ID: "job-b", State: StateQueued, CreatedAt: base.Add(1 * time.Second)})

	result, err := repo.List(ctx, ListOptions{})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	want := []string{"job-a", "job-b", "job-c"}
	if len(result.Jobs) != len(want) {
		t.Fatalf("got %d jobs, want %d", len(result.Jobs), len(want))
	}
	for i, id := range want {
		if result.Jobs[i].ID != id {
			t.Errorf("position %d: got %q, want %q", i, result.Jobs[i].ID, id)
		}
	}
}

func TestMemoryRepositoryListPaginates(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		id := string(rune('a' + i))
		_, _, _ = repo.Create(ctx, Job{ID: "job-" + id, State: StateQueued, CreatedAt: base.Add(time.Duration(i) * time.Second)})
	}

	page1, err := repo.List(ctx, ListOptions{PageSize: 2})
	if err != nil {
		t.Fatalf("List page 1 returned error: %v", err)
	}
	if len(page1.Jobs) != 2 {
		t.Fatalf("page 1: got %d jobs, want 2", len(page1.Jobs))
	}
	if page1.NextPageToken == "" {
		t.Fatal("page 1: expected a non-empty NextPageToken")
	}

	page2, err := repo.List(ctx, ListOptions{PageSize: 2, PageToken: page1.NextPageToken})
	if err != nil {
		t.Fatalf("List page 2 returned error: %v", err)
	}
	if len(page2.Jobs) != 2 {
		t.Fatalf("page 2: got %d jobs, want 2", len(page2.Jobs))
	}

	page3, err := repo.List(ctx, ListOptions{PageSize: 2, PageToken: page2.NextPageToken})
	if err != nil {
		t.Fatalf("List page 3 returned error: %v", err)
	}
	if len(page3.Jobs) != 1 {
		t.Fatalf("page 3: got %d jobs, want 1", len(page3.Jobs))
	}
	if page3.NextPageToken != "" {
		t.Errorf("page 3: expected empty NextPageToken (last page), got %q", page3.NextPageToken)
	}

	seen := map[string]bool{}
	for _, page := range [][]Job{page1.Jobs, page2.Jobs, page3.Jobs} {
		for _, j := range page {
			if seen[j.ID] {
				t.Errorf("job %q appeared in more than one page", j.ID)
			}
			seen[j.ID] = true
		}
	}
	if len(seen) != 5 {
		t.Errorf("expected 5 distinct jobs across all pages, saw %d", len(seen))
	}
}

func TestMemoryRepositoryListFiltersByState(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()

	_, _, _ = repo.Create(ctx, Job{ID: "job-1", State: StateQueued, CreatedAt: time.Now()})
	_, _, _ = repo.Create(ctx, Job{ID: "job-2", State: StateCancelled, CreatedAt: time.Now()})

	result, err := repo.List(ctx, ListOptions{StateFilter: StateCancelled})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(result.Jobs) != 1 || result.Jobs[0].ID != "job-2" {
		t.Fatalf("expected only job-2 (CANCELLED), got %+v", result.Jobs)
	}
}

func TestMemoryRepositoryUpdateAppliesFnUnderLock(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	_, _, _ = repo.Create(ctx, Job{ID: "job-1", State: StateQueued, CurrentAttempt: 1})

	updated, err := repo.Update(ctx, "job-1", func(j Job) (Job, error) {
		j.State = StateCancelled
		return j, nil
	})
	if err != nil {
		t.Fatalf("Update returned error: %v", err)
	}
	if updated.State != StateCancelled {
		t.Errorf("updated.State = %q, want %q", updated.State, StateCancelled)
	}

	got, err := repo.Get(ctx, "job-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != StateCancelled {
		t.Errorf("stored State = %q, want %q (Update must persist)", got.State, StateCancelled)
	}
}

func TestMemoryRepositoryUpdateMissingReturnsErrNotFound(t *testing.T) {
	repo := NewMemoryRepository()
	_, err := repo.Update(context.Background(), "does-not-exist", func(j Job) (Job, error) { return j, nil })
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryRepositoryUpdateFnErrorLeavesJobUnchanged(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	_, _, _ = repo.Create(ctx, Job{ID: "job-1", State: StateQueued})

	wantErr := errors.New("boom")
	_, err := repo.Update(ctx, "job-1", func(j Job) (Job, error) {
		return Job{}, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected the fn's own error to propagate, got %v", err)
	}

	got, err := repo.Get(ctx, "job-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != StateQueued {
		t.Errorf("job state changed despite fn returning an error: got %q", got.State)
	}
}
