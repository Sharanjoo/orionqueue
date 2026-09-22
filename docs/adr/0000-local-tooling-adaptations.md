# ADR-0000: Local tooling adaptations

Status: Accepted (Phase 0)

## Context

The brief's recommended architecture assumes `make`, `protoc`, a native
`etcd` binary, and a native `psql` client are available on the development
machine. A direct check on this machine (Windows 11, Git Bash / MINGW64)
found:

- `make` — not installed.
- `protoc` / `buf` — not installed.
- `etcd` — no native binary.
- `psql` — no native client.
- `golangci-lint` — not installed.
- `nvidia-smi` / NVIDIA GPU — not present.

Go 1.27, Python 3.12.10, Node 24.14, Docker 29.3.1 (daemon running) with
Compose v5.1.1, and Terraform 1.15.8 are all present.

## Decision

Route around every missing host tool instead of requiring the developer to
install system packages, so `git clone` + Go + Python + Node + Docker is
sufficient to work on the project:

- **Task runner:** `scripts/*.sh` (bash) is the primary, cross-platform
  entry point, runnable directly in Git Bash on Windows or any POSIX shell.
  A `Makefile` at the repo root wraps the same scripts with `make` targets
  for convenience in CI and on Linux/macOS, but is never the only way to run
  a command.
- **Protobuf codegen:** `buf` (installed via
  `go install github.com/bufbuild/buf/cmd/buf@latest`), which bundles its own
  compiler and doesn't need a separately managed `protoc` binary.
- **Database migrations:** `golang-migrate`
  (`go install github.com/golang-migrate/migrate/v4/cmd/migrate@latest`), a
  static Go binary, instead of shelling out to `psql` for schema changes.
  Ad hoc manual inspection during development uses
  `docker exec -it orionqueue-postgres psql ...`.
- **etcd, PostgreSQL, MinIO, Prometheus, Grafana:** Docker Compose services
  only. Never a required host install.
- **Linting:** `golangci-lint` is installed via `go install` (or pulled as a
  pinned binary in CI via its official GitHub Action) rather than assumed
  present; `gofmt`/`go vet` are used as an always-available baseline.
- **GPU:** no real GPU is available on this machine, so the fake-GPU
  simulator (ADR-0003) is the only mode exercised locally and in CI.

## Consequences

- Slightly more custom scripting up front (bash scripts + thin Makefile
  wrappers) instead of a single canonical Makefile.
- Codegen and lint tool versions are pinned via `go install ...@<version>` (or
  a `tools.go` + `go.sum`-tracked approach) rather than relying on whatever
  the OS package manager provides, which is more reproducible across
  machines anyway.
- Real-GPU and real-NCCL code paths (Phase 9) can be written and unit-tested
  for their control flow, but cannot be validated end-to-end until run on
  hardware with an actual NVIDIA GPU.
