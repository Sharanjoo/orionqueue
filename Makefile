# Thin wrapper around scripts/*.sh for CI and Linux/macOS convenience.
# scripts/*.sh are the primary, cross-platform entry point (they also run
# directly in Git Bash on Windows, this project's dev environment) — see
# docs/adr/0000-local-tooling-adaptations.md. Every target here just calls
# the matching script; add new functionality to the script, not here.
.PHONY: fmt lint test build dev-api dev-scheduler dev-worker dev-frontend

fmt:
	./scripts/fmt.sh

lint:
	./scripts/lint.sh

test:
	./scripts/test.sh

build:
	./scripts/build.sh

dev-api:
	./scripts/dev-api.sh

dev-scheduler:
	./scripts/dev-scheduler.sh

dev-worker:
	./scripts/dev-worker.sh

dev-frontend:
	./scripts/dev-frontend.sh
