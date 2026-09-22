#!/usr/bin/env bash
# Runs the Python worker agent directly (no Docker) for local development.
# Creates/activates worker/.venv automatically if needed.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"
activate_worker_venv
python -m agent.main
