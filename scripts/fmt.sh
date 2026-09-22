#!/usr/bin/env bash
# Applies formatting across every language in the repo. Run this before
# scripts/lint.sh (or `make fmt` before `make lint`) — lint checks formatting
# but doesn't fix it.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

echo "==> gofmt"
gofmt -w .

echo "==> black (worker/)"
activate_worker_venv
(cd worker && black .)

echo "==> prettier (frontend/)"
(cd frontend && npx prettier --write .)

echo "All formatting applied."
