#!/usr/bin/env bash
# Applies pending database migrations to ORIONQUEUE_DATABASE_URL (or the
# same local default cmd/api uses). Installs the golang-migrate CLI on
# first run if it's not already on PATH. Inside `docker compose up`, this
# happens automatically via the `migrate` one-shot service instead — this
# script is for running against a manually-started Postgres, or against a
# non-Compose environment (e.g. a Kubernetes-forwarded database).
#
# Usage:
#   ./scripts/migrate.sh up          # apply all pending migrations (default)
#   ./scripts/migrate.sh down 1      # roll back the most recent migration
#   ./scripts/migrate.sh version     # show the current migration version
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

DATABASE_URL="${ORIONQUEUE_DATABASE_URL:-postgres://orionqueue:orionqueue@localhost:5432/orionqueue?sslmode=disable}"

export PATH="$PATH:$(go env GOPATH)/bin"
if ! command -v migrate >/dev/null 2>&1; then
  echo "==> installing golang-migrate CLI (first run only)"
  go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
fi

migrate -path "$REPO_ROOT/migrations" -database "$DATABASE_URL" "${@:-up}"
