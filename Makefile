# Thin wrapper around scripts/*.sh for CI and Linux/macOS convenience.
# scripts/*.sh are the primary, cross-platform entry point (they also run
# directly in Git Bash on Windows, this project's dev environment) — see
# docs/adr/0000-local-tooling-adaptations.md. Every target here just calls
# the matching script; add new functionality to the script, not here.
.PHONY: fmt lint test test-integration build migrate proto-gen dev-api dev-scheduler dev-worker dev-frontend

fmt:
	./scripts/fmt.sh

lint:
	./scripts/lint.sh

test:
	./scripts/test.sh

test-integration:
	./scripts/test-integration.sh

build:
	./scripts/build.sh

migrate:
	./scripts/migrate.sh up

proto-gen:
	./scripts/proto-gen.sh

dev-api:
	./scripts/dev-api.sh

dev-scheduler:
	./scripts/dev-scheduler.sh

dev-worker:
	./scripts/dev-worker.sh

dev-frontend:
	./scripts/dev-frontend.sh
