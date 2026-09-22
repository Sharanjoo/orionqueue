#!/usr/bin/env bash
# Runs the React dashboard's Vite dev server. Installs node_modules first
# if they're missing.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT/frontend"
if [ ! -d node_modules ]; then
  npm install
fi
npm run dev
