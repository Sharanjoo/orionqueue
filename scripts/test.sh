#!/usr/bin/env bash
# Runs unit tests for every language in the repo. Phase 1 only has unit
# tests; scripts/test-integration.sh, test-e2e.sh, and test-load.sh are
# added in later phases as those suites exist (see PROJECT_STATUS.md).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

echo "==> go test"
# -race requires cgo + a C compiler, which this project's Windows dev
# machine doesn't have installed (confirmed: `gcc` not found). CI runs on
# Linux, where gcc is present by default, so -race is enabled there
# (.github/workflows/ci.yml) even though it's skipped here.
if go env CGO_ENABLED | grep -q 1 && command -v gcc >/dev/null 2>&1; then
  go test ./... -race -cover
else
  echo "(cgo/gcc unavailable locally, running without -race)"
  go test ./... -cover
fi

echo "==> pytest (worker/)"
activate_worker_venv
(cd worker && pytest)

echo "==> vitest (frontend/)"
(cd frontend && npm run test)

echo "All unit tests passed."
