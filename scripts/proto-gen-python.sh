#!/usr/bin/env bash
# Regenerates the worker agent's Python gRPC client stubs from
# proto/*.proto into worker/gen/. Uses grpcio-tools (already a dev
# dependency of the worker package) rather than buf's Go-oriented plugin
# mechanism, since Python codegen via `python -m grpc_tools.protoc` is the
# standard tool for this ecosystem.
#
# proto/orionqueue/v1/worker_service.proto imports google/api/annotations.proto
# for ListWorkers' REST binding — Python codegen needs that file resolvable
# to parse the .proto even though it doesn't use grpc-gateway itself. It's
# exported fresh into a temp dir each run via `buf export` (not committed —
# it's large, and buf.lock already pins the exact version), reusing the
# same googleapis dependency proto/'s buf.yaml already declares.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

export PATH="$PATH:$(go env GOPATH)/bin"

THIRD_PARTY_DIR="$(mktemp -d)"
trap 'rm -rf "$THIRD_PARTY_DIR"' EXIT
echo "==> exporting googleapis annotations (needed to parse worker_service.proto)"
buf export buf.build/googleapis/googleapis -o "$THIRD_PARTY_DIR"

activate_worker_venv
pip install -q grpcio-tools

rm -rf worker/gen
mkdir -p worker/gen
python -m grpc_tools.protoc \
  -I proto -I "$THIRD_PARTY_DIR" \
  --python_out=worker/gen \
  --grpc_python_out=worker/gen \
  --pyi_out=worker/gen \
  proto/orionqueue/v1/job.proto \
  proto/orionqueue/v1/job_service.proto \
  proto/orionqueue/v1/worker.proto \
  proto/orionqueue/v1/worker_service.proto

# grpc_tools.protoc doesn't emit __init__.py files; add them so
# worker/gen/orionqueue/v1 is an importable package.
touch worker/gen/__init__.py
touch worker/gen/orionqueue/__init__.py
touch worker/gen/orionqueue/v1/__init__.py

echo "Generated worker/gen/orionqueue/v1/*_pb2.py from proto/*.proto"
