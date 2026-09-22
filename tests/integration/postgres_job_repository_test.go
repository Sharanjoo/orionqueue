//go:build integration

// Package integration holds tests that need real infrastructure (here, a
// real PostgreSQL instance via testcontainers-go), as opposed to the
// package-local unit tests under internal/*, which never touch the
// network or a database. See docs/architecture/system-overview.md's
// testing strategy.
//
// These tests start a disposable Postgres container per test run, so
// they require Docker to be running and (on the very first run) internet
// access to pull the postgres:16-alpine image. Run them with:
//
//	go test ./tests/integration/... -tags=integration
//
// They're gated behind the "integration" build tag so `go test ./...`
// (and CI's default job) doesn't require Docker.
package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/Sharanjoo/orionqueue/internal/jobs"
	"github.com/Sharanjoo/orionqueue/internal/persistence"
)

// newTestDatabase starts a real, disposable Postgres container, applies
// every migration to it, and returns a connection string. The container
// is torn down automatically at the end of the test.
func newTestDatabase(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("orionqueue_test"),
		tcpostgres.WithUsername("orionqueue"),
		tcpostgres.WithPassword("orionqueue"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	if err := persistence.RunMigrations(dsn); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}
	return dsn
}

func newTestRepository(t *testing.T, dsn string) *persistence.JobRepository {
	t.Helper()
	pool, err := persistence.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}
	t.Cleanup(pool.Close)
	return persistence.NewJobRepository(pool)
}

func validJob(id string) jobs.Job {
	return jobs.Job{
		ID:         id,
		Name:       "train-resnet",
		Owner:      "sharan",
		Image:      "orionqueue/fake-gpu-job:latest",
		Command:    []string{"python", "train.py"},
		Resources:  jobs.ResourceRequest{GPUCount: 2, MinGPUMemoryBytes: 16 << 30, CPUCores: 4, MemoryBytes: 8 << 30},
		Priority:   50,
		RetryLimit: 3,
		State:      jobs.StateQueued,
		CreatedAt:  time.Now().UTC().Truncate(time.Microsecond),
	}
}

func TestPostgresJobRepository_CreateAndGet(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestRepository(t, dsn)
	ctx := context.Background()

	created, wasNew, err := repo.Create(ctx, validJob("job-pg-1"))
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if !wasNew {
		t.Fatal("expected wasNew=true")
	}
	if created.Resources.GPUCount != 2 {
		t.Errorf("GPUCount = %d, want 2", created.Resources.GPUCount)
	}
	if len(created.Command) != 2 || created.Command[1] != "train.py" {
		t.Errorf("Command = %v, want [python train.py]", created.Command)
	}

	got, err := repo.Get(ctx, "job-pg-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Name != "train-resnet" {
		t.Errorf("Name = %q, want %q", got.Name, "train-resnet")
	}
	if got.CreatedAt.Location() != time.UTC {
		t.Errorf("CreatedAt location = %v, want UTC", got.CreatedAt.Location())
	}
}

// TestPostgresJobRepository_JobSurvivesReconnection is the most literal
// possible test of Phase 3's acceptance criterion ("job state survives an
// API restart"): it writes a job through one repository/pool instance,
// opens a completely independent second connection pool against the same
// database (standing in for a fresh cmd/api process), and confirms the
// job is still there.
func TestPostgresJobRepository_JobSurvivesReconnection(t *testing.T) {
	dsn := newTestDatabase(t)
	ctx := context.Background()

	firstRepo := newTestRepository(t, dsn)
	if _, _, err := firstRepo.Create(ctx, validJob("job-restart-1")); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	// A second, independent pool — nothing shared with firstRepo except
	// the database itself — simulates a restarted process reconnecting.
	secondRepo := newTestRepository(t, dsn)
	got, err := secondRepo.Get(ctx, "job-restart-1")
	if err != nil {
		t.Fatalf("job did not survive reconnection: Get returned error: %v", err)
	}
	if got.ID != "job-restart-1" {
		t.Errorf("ID = %q, want %q", got.ID, "job-restart-1")
	}
}

func TestPostgresJobRepository_GetMissingReturnsErrNotFound(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestRepository(t, dsn)

	_, err := repo.Get(context.Background(), "does-not-exist")
	if !errors.Is(err, jobs.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPostgresJobRepository_CreateIsIdempotentBySubmissionID(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestRepository(t, dsn)
	ctx := context.Background()

	j1 := validJob("job-pg-2")
	j1.SubmissionID = "sub-shared"
	first, wasNew1, err := repo.Create(ctx, j1)
	if err != nil {
		t.Fatalf("first Create returned error: %v", err)
	}
	if !wasNew1 {
		t.Fatal("expected wasNew=true for the first submission")
	}

	j2 := validJob("job-pg-3") // different ID, same submission_id
	j2.SubmissionID = "sub-shared"
	second, wasNew2, err := repo.Create(ctx, j2)
	if err != nil {
		t.Fatalf("second Create returned error: %v", err)
	}
	if wasNew2 {
		t.Fatal("expected wasNew=false for a duplicate submission_id")
	}
	if second.ID != first.ID {
		t.Errorf("second.ID = %q, want the original job's ID %q", second.ID, first.ID)
	}

	// Enforced by the database (a real improvement over the in-memory
	// repository's process-local mutex): a second, concurrent-looking
	// insert with the same submission_id must never produce two rows.
	list, err := repo.List(ctx, jobs.ListOptions{})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	count := 0
	for _, j := range list.Jobs {
		if j.SubmissionID == "sub-shared" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 stored job for submission_id=sub-shared, got %d", count)
	}
}

func TestPostgresJobRepository_ListOrdersAndPaginates(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestRepository(t, dsn)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Microsecond)
	ids := []string{"job-list-a", "job-list-b", "job-list-c", "job-list-d", "job-list-e"}
	for i, id := range ids {
		j := validJob(id)
		j.CreatedAt = base.Add(time.Duration(i) * time.Second)
		if _, _, err := repo.Create(ctx, j); err != nil {
			t.Fatalf("Create(%s) returned error: %v", id, err)
		}
	}

	var allSeen []string
	token := ""
	for {
		page, err := repo.List(ctx, jobs.ListOptions{PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatalf("List returned error: %v", err)
		}
		for _, j := range page.Jobs {
			allSeen = append(allSeen, j.ID)
		}
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
		if len(allSeen) > 20 {
			t.Fatal("pagination did not terminate")
		}
	}

	if len(allSeen) != len(ids) {
		t.Fatalf("got %d jobs across all pages, want %d: %v", len(allSeen), len(ids), allSeen)
	}
	for i, id := range ids {
		if allSeen[i] != id {
			t.Errorf("position %d: got %q, want %q (expected creation order)", i, allSeen[i], id)
		}
	}
}

func TestPostgresJobRepository_ListFiltersByState(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestRepository(t, dsn)
	ctx := context.Background()

	queued := validJob("job-filter-queued")
	cancelled := validJob("job-filter-cancelled")
	cancelled.State = jobs.StateCancelled
	if _, _, err := repo.Create(ctx, queued); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, _, err := repo.Create(ctx, cancelled); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	result, err := repo.List(ctx, jobs.ListOptions{StateFilter: jobs.StateCancelled})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(result.Jobs) != 1 || result.Jobs[0].ID != "job-filter-cancelled" {
		t.Fatalf("expected only job-filter-cancelled, got %+v", result.Jobs)
	}
}

func TestPostgresJobRepository_UpdateAppliesChangeAndRecordsEvent(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestRepository(t, dsn)
	ctx := context.Background()

	if _, _, err := repo.Create(ctx, validJob("job-pg-update")); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	updated, err := repo.Update(ctx, "job-pg-update", func(j jobs.Job) (jobs.Job, error) {
		j.State = jobs.StateCancelled
		return j, nil
	})
	if err != nil {
		t.Fatalf("Update returned error: %v", err)
	}
	if updated.State != jobs.StateCancelled {
		t.Errorf("State = %q, want %q", updated.State, jobs.StateCancelled)
	}

	got, err := repo.Get(ctx, "job-pg-update")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != jobs.StateCancelled {
		t.Errorf("stored State = %q, want %q (Update must persist)", got.State, jobs.StateCancelled)
	}

	pool, err := persistence.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to connect for event verification: %v", err)
	}
	defer pool.Close()
	var eventCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM job_events WHERE job_id = $1 AND event_type = 'STATE_CHANGED' AND from_state = 'QUEUED' AND to_state = 'CANCELLED'`,
		"job-pg-update",
	).Scan(&eventCount); err != nil {
		t.Fatalf("failed to query job_events: %v", err)
	}
	if eventCount != 1 {
		t.Errorf("expected exactly 1 matching job_events row, got %d", eventCount)
	}
}

func TestPostgresJobRepository_UpdateMissingReturnsErrNotFound(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestRepository(t, dsn)

	_, err := repo.Update(context.Background(), "does-not-exist", func(j jobs.Job) (jobs.Job, error) { return j, nil })
	if !errors.Is(err, jobs.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPostgresJobRepository_UpdateFnErrorLeavesJobUnchanged(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestRepository(t, dsn)
	ctx := context.Background()

	if _, _, err := repo.Create(ctx, validJob("job-pg-rollback")); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	wantErr := errors.New("boom")
	_, err := repo.Update(ctx, "job-pg-rollback", func(j jobs.Job) (jobs.Job, error) {
		return jobs.Job{}, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected the fn's own error to propagate, got %v", err)
	}

	got, err := repo.Get(ctx, "job-pg-rollback")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.State != jobs.StateQueued {
		t.Errorf("job state changed despite fn returning an error (transaction should have rolled back): got %q", got.State)
	}
}

func TestPostgresJobRepository_SubmissionEventRecorded(t *testing.T) {
	dsn := newTestDatabase(t)
	repo := newTestRepository(t, dsn)
	ctx := context.Background()

	if _, _, err := repo.Create(ctx, validJob("job-pg-submit-event")); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	pool, err := persistence.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to connect for event verification: %v", err)
	}
	defer pool.Close()

	var eventType string
	var fromState *string
	if err := pool.QueryRow(ctx,
		`SELECT event_type, from_state FROM job_events WHERE job_id = $1`, "job-pg-submit-event",
	).Scan(&eventType, &fromState); err != nil {
		t.Fatalf("failed to query job_events: %v", err)
	}
	if eventType != "SUBMITTED" {
		t.Errorf("event_type = %q, want %q", eventType, "SUBMITTED")
	}
	if fromState != nil {
		t.Errorf("from_state = %v, want nil for a SUBMITTED event", *fromState)
	}
}
