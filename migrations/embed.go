// Package migrations embeds OrionQueue's SQL migration files so both
// human-run tooling (the golang-migrate CLI, via a plain file:// path —
// see scripts/migrate.sh) and Go code (via this embed, used by
// integration tests to migrate a fresh test database) read from the same
// source of truth, never two copies that could drift apart.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
