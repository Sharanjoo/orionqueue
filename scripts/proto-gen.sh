#!/usr/bin/env bash
# Regenerates Go/grpc-gateway/OpenAPI code from proto/*.proto. Run this
# after any change under proto/, then commit the regenerated files under
# internal/api/gen/ and docs/api/ — CI fails the build if they drift from
# what buf generate would produce (see .github/workflows/ci.yml).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

export PATH="$PATH:$(go env GOPATH)/bin"

if ! command -v buf >/dev/null 2>&1; then
  echo "==> installing buf and codegen plugins (first run only)"
  go install github.com/bufbuild/buf/cmd/buf@latest
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
  go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest
fi

buf dep update
buf lint
buf generate
echo "Generated internal/api/gen/ and docs/api/ from proto/."
