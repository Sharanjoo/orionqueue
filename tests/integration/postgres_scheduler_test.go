//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/persistence"
	"github.com/Sharanjoo/orionqueue/internal/workers"
)

func TestPostgresJobRepository_ListActiveExcludesTerminalAndOrdersByPriority(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestRepository(t, dsn)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Microsecond)

	low := validJob("job-sched-low")
	low.Priority = 10
	low.CreatedAt = base
	high := validJob("job-sched-high")
	high.Priority = 90
	high.CreatedAt = base.Add(time.Second) // later submission, but higher priority
	succeeded := validJob("job-sched-done")
	succeeded.State = jobs.StateSucceeded
	succeeded.CreatedAt = base

	for _, j := range []jobs.Job{low, high, succeeded} {
		if _, _, err := repo.Create(ctx, j); err != nil {
			t.Fatalf("Create(%s) returned error: %v", j.ID, err)
		}
	}

	active, err := repo.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive returned error: %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("expected 2 active jobs (excluding SUCCEEDED), got %d: %+v", len(active), active)
	}
	if active[0].ID != "job-sched-high" || active[1].ID != "job-sched-low" {
		t.Fatalf("expected [job-sched-high, job-sched-low] in priority order, got [%s, %s]", active[0].ID, active[1].ID)
	}
}

func TestPostgresWorkerRepository_ListActiveExcludesLostWorkers(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestWorkerRepository(t, dsn)
	ctx := context.Background()

	active := validWorker("worker-sched-active")
	lost := validWorker("worker-sched-lost")
	lost.Status = workers.StatusLost
	lost.LeaseID = nil

	if _, err := repo.Create(ctx, active); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, err := repo.Create(ctx, lost); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	got, err := repo.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive returned error: %v", err)
	}
	if len(got) != 1 || got[0].ID != "worker-sched-active" {
		t.Fatalf("expected only worker-sched-active, got %+v", got)
	}
	if len(got[0].GPUs) != 2 {
		t.Fatalf("expected GPU inventory to be populated, got %d GPUs", len(got[0].GPUs))
	}
}

func TestSchedulingDecisionRepository_RecordPersists(t *testing.T) {
	dsn := newTestDatabase(t)
	jobRepo := newTestRepository(t, dsn)
	ctx := context.Background()

	if _, _, err := jobRepo.Create(ctx, validJob("job-decision-1")); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	pool, err := persistence.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("Connect returned error: %v", err)
	}
	defer pool.Close()

	decisions := persistence.NewSchedulingDecisionRepository(pool)
	if err := decisions.Record(ctx, "job-decision-1", "worker-1", "eligible worker found"); err != nil {
		t.Fatalf("Record returned error: %v", err)
	}

	var count int
	var reason string
	if err := pool.QueryRow(ctx,
		`SELECT count(*), max(reason) FROM scheduling_decisions WHERE job_id = $1 AND decision = 'ASSIGNED'`,
		"job-decision-1",
	).Scan(&count, &reason); err != nil {
		t.Fatalf("failed to query scheduling_decisions: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 recorded decision, got %d", count)
	}
	if reason != "eligible worker found" {
		t.Errorf("reason = %q, want %q", reason, "eligible worker found")
	}
}

func TestSchedulingDecisionRepository_RecordFailsForUnknownJob(t *testing.T) {
	dsn := newTestDatabase(t)
	ctx := context.Background()

	pool, err := persistence.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("Connect returned error: %v", err)
	}
	defer pool.Close()

	decisions := persistence.NewSchedulingDecisionRepository(pool)
	// job_id has a foreign key to jobs(id); recording a decision for a
	// job that doesn't exist must fail, not silently insert an orphan row.
	if err := decisions.Record(ctx, "does-not-exist", "worker-1", "test"); err == nil {
		t.Fatal("expected an error recording a decision for a non-existent job")
	}
}
