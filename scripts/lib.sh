#!/usr/bin/env bash
# Shared helpers for scripts/*.sh. Not run directly — sourced by the other
# scripts in this directory. See docs/adr/0000-local-tooling-adaptations.md
# for why scripts/ (not a Makefile) is the primary task runner.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# activate_worker_venv sources the worker package's virtualenv, creating it
# first if it doesn't exist yet. Handles both the Windows (Scripts/) and
# POSIX (bin/) venv layouts so the same script works in Git Bash on
# Windows and in Linux CI.
activate_worker_venv() {
  local venv="$REPO_ROOT/worker/.venv"
  if [ ! -d "$venv" ]; then
    echo "==> creating worker virtualenv at $venv"
    python -m venv "$venv"
    if [ -f "$venv/Scripts/activate" ]; then
      # shellcheck disable=SC1091
      source "$venv/Scripts/activate"
    else
      # shellcheck disable=SC1091
      source "$venv/bin/activate"
    fi
    pip install -q -e "$REPO_ROOT/worker[dev]"
    return
  fi

  if [ -f "$venv/Scripts/activate" ]; then
    # shellcheck disable=SC1091
    source "$venv/Scripts/activate"
  else
    # shellcheck disable=SC1091
    source "$venv/bin/activate"
  fi
}
