#!/usr/bin/env bash
# Builds every buildable artifact: the Go binaries and the frontend's
# production bundle. Does not build container images — see
# scripts/docker-build.sh, added in Phase 12.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

echo "==> go build"
mkdir -p bin
go build -o bin/ ./...

echo "==> frontend build"
(cd frontend && npm run build)

echo "Build artifacts: $REPO_ROOT/bin/*, $REPO_ROOT/frontend/dist/"
