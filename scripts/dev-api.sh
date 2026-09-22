#!/usr/bin/env bash
# Runs the API service directly (no Docker) for local development.
# Health endpoints: http://localhost:7080/healthz and /readyz once running
# (or ORIONQUEUE_HTTP_ADDR if overridden).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"
go run ./cmd/api
