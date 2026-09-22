#!/usr/bin/env bash
# Runs every formatting/lint check without modifying files. Fails (exit
# non-zero) on the first language that has a problem, after printing what
# it found, so CI output shows exactly what to fix.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

export PATH="$PATH:$(go env GOPATH)/bin"
if command -v buf >/dev/null 2>&1; then
  echo "==> buf lint"
  buf lint
else
  echo "==> buf not installed locally, skipping (run scripts/proto-gen.sh once to install it; CI always runs buf lint)"
fi

echo "==> gofmt -l (fails if any .go file is unformatted)"
unformatted="$(gofmt -l .)"
if [ -n "$unformatted" ]; then
  echo "The following files are not gofmt-formatted:"
  echo "$unformatted"
  exit 1
fi

echo "==> go vet"
go vet ./...

if command -v golangci-lint >/dev/null 2>&1; then
  echo "==> golangci-lint"
  golangci-lint run ./...
else
  echo "==> golangci-lint not installed locally, skipping (gofmt+vet already ran; CI installs it via the official action — see docs/adr/0000-local-tooling-adaptations.md)"
fi

echo "==> ruff (worker/)"
activate_worker_venv
(cd worker && ruff check .)

echo "==> black --check (worker/)"
(cd worker && black --check .)

echo "==> oxlint (frontend/)"
(cd frontend && npm run lint)

echo "==> prettier --check (frontend/)"
(cd frontend && npm run format:check)

echo "All lint checks passed."
