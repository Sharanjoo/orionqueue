package persistence

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver used below

	orionmigrations "github.com/Sharanjoo/orionqueue/migrations"
)

// RunMigrations applies every pending migration from the embedded
// migrations package (migrations/*.sql) to databaseURL.
//
// This is used by integration tests to prepare a fresh test database.
// cmd/api deliberately does NOT call this at startup — migrating is a
// separate, explicit step (scripts/migrate.sh, or the `migrate` service
// in docker-compose.yml), the same way a real deployment wouldn't want a
// service silently altering its own database schema on every boot.
func RunMigrations(databaseURL string) error {
	src, err := iofs.New(orionmigrations.FS, ".")
	if err != nil {
		return fmt.Errorf("persistence: load embedded migrations: %w", err)
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("persistence: open migration connection: %w", err)
	}
	defer db.Close()

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("persistence: create migration driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		return fmt.Errorf("persistence: create migrator: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("persistence: apply migrations: %w", err)
	}
	return nil
}
