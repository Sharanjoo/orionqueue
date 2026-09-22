package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sharanjoo/orionqueue/internal/workers"
)

// workerColumnList matches workers.Worker's fields exactly (GPUs come
// from a separate query/table — see getGPUs); updated_at is a
// database-only bookkeeping column, deliberately not part of this list
// or the domain type.
const workerColumnList = `id, hostname, status, lease_id, cpu_capacity, memory_capacity_bytes,
	software_version, labels, running_job_ids, registered_at, last_heartbeat_at`

const gpuColumnList = `id, worker_id, device_index, uuid, total_memory_bytes, allocated_memory_bytes,
	utilization_percent, temperature_celsius, mig_profile, health_state`

var (
	insertWorkerSQL = fmt.Sprintf(`
INSERT INTO workers (
	id, hostname, status, lease_id, cpu_capacity, memory_capacity_bytes,
	software_version, labels, running_job_ids, registered_at, last_heartbeat_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, now())
RETURNING %s`, workerColumnList)

	selectWorkerByIDSQL = fmt.Sprintf(`SELECT %s FROM workers WHERE id = $1`, workerColumnList)

	selectWorkerByIDForUpdateSQL = fmt.Sprintf(`SELECT %s FROM workers WHERE id = $1 FOR UPDATE`, workerColumnList)

	updateWorkerSQL = fmt.Sprintf(`
UPDATE workers SET
	hostname=$2, status=$3, lease_id=$4, cpu_capacity=$5, memory_capacity_bytes=$6,
	software_version=$7, labels=$8, running_job_ids=$9, registered_at=$10, last_heartbeat_at=$11,
	updated_at=now()
WHERE id=$1
RETURNING %s`, workerColumnList)

	selectGPUsByWorkerIDSQL = fmt.Sprintf(`SELECT %s FROM gpus WHERE worker_id = $1 ORDER BY device_index`, gpuColumnList)

	insertGPUSQL = fmt.Sprintf(`
INSERT INTO gpus (%s) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, gpuColumnList)
)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, so GPU
// read/write helpers work the same whether called outside or inside a
// transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// WorkerRepository is a PostgreSQL-backed implementation of
// workers.Repository — the same two-implementation pattern (in-memory for
// fast tests, PostgreSQL for cmd/api) Phase 2/3 established for jobs.
type WorkerRepository struct {
	pool *pgxpool.Pool
}

// NewWorkerRepository returns a WorkerRepository backed by pool.
// Migrations must already be applied.
func NewWorkerRepository(pool *pgxpool.Pool) *WorkerRepository {
	return &WorkerRepository{pool: pool}
}

func (r *WorkerRepository) Create(ctx context.Context, w workers.Worker) (workers.Worker, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return workers.Worker{}, fmt.Errorf("persistence: begin create tx: %w", err)
	}
	defer tx.Rollback(ctx)

	created, err := scanWorker(tx.QueryRow(ctx, insertWorkerSQL, workerArgs(w)...))
	if err != nil {
		return workers.Worker{}, fmt.Errorf("persistence: insert worker: %w", err)
	}

	if err := replaceGPUs(ctx, tx, w.ID, w.GPUs); err != nil {
		return workers.Worker{}, err
	}
	created.GPUs = w.GPUs

	if err := tx.Commit(ctx); err != nil {
		return workers.Worker{}, fmt.Errorf("persistence: commit create: %w", err)
	}
	return created, nil
}

func (r *WorkerRepository) Get(ctx context.Context, id string) (workers.Worker, error) {
	w, err := scanWorker(r.pool.QueryRow(ctx, selectWorkerByIDSQL, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workers.Worker{}, workers.ErrNotFound
		}
		return workers.Worker{}, fmt.Errorf("persistence: get worker: %w", err)
	}
	gpus, err := getGPUs(ctx, r.pool, id)
	if err != nil {
		return workers.Worker{}, err
	}
	w.GPUs = gpus
	return w, nil
}

func (r *WorkerRepository) List(ctx context.Context, opts workers.ListOptions) (workers.ListResult, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 || pageSize > maxPageSize {
		pageSize = defaultPageSize
	}

	var (
		conditions []string
		args       = []any{pageSize + 1}
	)
	nextArg := func() string { return fmt.Sprintf("$%d", len(args)) }

	if opts.StatusFilter != "" {
		args = append(args, string(opts.StatusFilter))
		conditions = append(conditions, "status = "+nextArg())
	}
	if opts.PageToken != "" {
		registeredAt, id, err := decodeListToken(opts.PageToken)
		if err != nil {
			return workers.ListResult{}, fmt.Errorf("invalid page_token: %w", err)
		}
		args = append(args, registeredAt)
		registeredArg := nextArg()
		args = append(args, id)
		idArg := nextArg()
		conditions = append(conditions, fmt.Sprintf("(registered_at, id) > (%s, %s)", registeredArg, idArg))
	}

	query := fmt.Sprintf(`SELECT %s FROM workers`, workerColumnList)
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY registered_at, id LIMIT $1"

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return workers.ListResult{}, fmt.Errorf("persistence: list workers: %w", err)
	}
	defer rows.Close()

	var page []workers.Worker
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return workers.ListResult{}, fmt.Errorf("persistence: scan listed worker: %w", err)
		}
		page = append(page, w)
	}
	if err := rows.Err(); err != nil {
		return workers.ListResult{}, fmt.Errorf("persistence: iterate listed workers: %w", err)
	}

	result := workers.ListResult{}
	if len(page) > int(pageSize) {
		last := page[pageSize-1]
		result.NextPageToken = encodeListToken(last.RegisteredAt, last.ID)
		page = page[:pageSize]
	}

	// N+1: one GPU query per returned worker. Acceptable at the worker
	// counts a single-developer portfolio deployment targets; a JOIN
	// with array_agg would avoid this if listing needed to scale to many
	// more workers per page.
	for i := range page {
		gpus, err := getGPUs(ctx, r.pool, page[i].ID)
		if err != nil {
			return workers.ListResult{}, err
		}
		page[i].GPUs = gpus
	}
	result.Workers = page
	return result, nil
}

func (r *WorkerRepository) Update(ctx context.Context, id string, fn func(workers.Worker) (workers.Worker, error)) (workers.Worker, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return workers.Worker{}, fmt.Errorf("persistence: begin update tx: %w", err)
	}
	defer tx.Rollback(ctx)

	current, err := scanWorker(tx.QueryRow(ctx, selectWorkerByIDForUpdateSQL, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workers.Worker{}, workers.ErrNotFound
		}
		return workers.Worker{}, fmt.Errorf("persistence: select worker for update: %w", err)
	}
	currentGPUs, err := getGPUs(ctx, tx, id)
	if err != nil {
		return workers.Worker{}, err
	}
	current.GPUs = currentGPUs

	updated, err := fn(current)
	if err != nil {
		return workers.Worker{}, err
	}

	stored, err := scanWorker(tx.QueryRow(ctx, updateWorkerSQL, workerArgs(updated)...))
	if err != nil {
		return workers.Worker{}, fmt.Errorf("persistence: update worker: %w", err)
	}
	if err := replaceGPUs(ctx, tx, id, updated.GPUs); err != nil {
		return workers.Worker{}, err
	}
	stored.GPUs = updated.GPUs

	if err := tx.Commit(ctx); err != nil {
		return workers.Worker{}, fmt.Errorf("persistence: commit update: %w", err)
	}
	return stored, nil
}

func getGPUs(ctx context.Context, q querier, workerID string) ([]workers.GPU, error) {
	rows, err := q.Query(ctx, selectGPUsByWorkerIDSQL, workerID)
	if err != nil {
		return nil, fmt.Errorf("persistence: get gpus: %w", err)
	}
	defer rows.Close()

	var out []workers.GPU
	for rows.Next() {
		g, err := scanGPU(rows)
		if err != nil {
			return nil, fmt.Errorf("persistence: scan gpu: %w", err)
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func replaceGPUs(ctx context.Context, q querier, workerID string, gpus []workers.GPU) error {
	if _, err := q.Exec(ctx, `DELETE FROM gpus WHERE worker_id = $1`, workerID); err != nil {
		return fmt.Errorf("persistence: delete existing gpus: %w", err)
	}
	for _, g := range gpus {
		id := gpuRowID(workerID, g.DeviceIndex)
		_, err := q.Exec(ctx, insertGPUSQL,
			id, workerID, g.DeviceIndex, g.UUID, g.TotalMemoryBytes, g.AllocatedMemoryBytes,
			g.UtilizationPercent, g.TemperatureCelsius, g.MIGProfile, string(g.HealthState),
		)
		if err != nil {
			return fmt.Errorf("persistence: insert gpu: %w", err)
		}
	}
	return nil
}

func gpuRowID(workerID string, deviceIndex int32) string {
	return fmt.Sprintf("%s-gpu-%d", workerID, deviceIndex)
}

func scanWorker(row pgxRow) (workers.Worker, error) {
	var (
		w          workers.Worker
		status     string
		labelsJSON []byte
	)
	err := row.Scan(
		&w.ID, &w.Hostname, &status, &w.LeaseID, &w.CPUCapacity, &w.MemoryCapacityBytes,
		&w.SoftwareVersion, &labelsJSON, &w.RunningJobIDs,
		&w.RegisteredAt, &w.LastHeartbeatAt,
	)
	if err != nil {
		return workers.Worker{}, err
	}
	w.Status = workers.Status(status)
	if len(labelsJSON) > 0 {
		if err := json.Unmarshal(labelsJSON, &w.Labels); err != nil {
			return workers.Worker{}, fmt.Errorf("persistence: unmarshal labels: %w", err)
		}
	}
	w.RegisteredAt = w.RegisteredAt.UTC()
	w.LastHeartbeatAt = w.LastHeartbeatAt.UTC()
	return w, nil
}

func scanGPU(row pgxRow) (workers.GPU, error) {
	var (
		g           workers.GPU
		gpuID       string
		workerID    string
		healthState string
	)
	err := row.Scan(
		&gpuID, &workerID, &g.DeviceIndex, &g.UUID, &g.TotalMemoryBytes, &g.AllocatedMemoryBytes,
		&g.UtilizationPercent, &g.TemperatureCelsius, &g.MIGProfile, &healthState,
	)
	if err != nil {
		return workers.GPU{}, err
	}
	g.ID = gpuID
	g.HealthState = workers.GPUHealthState(healthState)
	return g, nil
}

func workerArgs(w workers.Worker) []any {
	labelsJSON, err := json.Marshal(w.Labels)
	if err != nil {
		// w.Labels is always a plain map[string]string; marshaling it can
		// only fail on an OOM-class condition, not on the data itself.
		labelsJSON = []byte("{}")
	}
	return []any{
		w.ID, w.Hostname, string(w.Status), w.LeaseID, w.CPUCapacity, w.MemoryCapacityBytes,
		w.SoftwareVersion, labelsJSON, nonNilStrings(w.RunningJobIDs),
		w.RegisteredAt, w.LastHeartbeatAt,
	}
}
