#!/usr/bin/env bash
# Runs the integration test suite (tests/integration/), which needs Docker
# running — it starts a real, disposable PostgreSQL container per test via
# testcontainers-go. Separate from scripts/test.sh (unit tests only,
# no Docker required) so CI/local unit-test runs stay fast and
# dependency-free; this one is slower (~90s) and pulls postgres:16-alpine
# on first run.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

echo "==> go test ./tests/integration/... (tag: integration)"
go test ./tests/integration/... -tags=integration -v -cover

echo "All integration tests passed."
