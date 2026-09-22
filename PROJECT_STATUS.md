# OrionQueue — Project Status

Last updated: 2026-09-21 (Phase 0)

## Current phase

**Phase 0 — Repository inspection and technical design.** Complete.

No application code has been written yet. This phase produced the
architecture proposal, ADRs, repository layout, and the phase plan that all
later phases execute against. See
[docs/architecture/system-overview.md](docs/architecture/system-overview.md)
and [docs/adr/](docs/adr/).

## Phase tracker

| Phase | Name | Status |
|---|---|---|
| 0 | Repository inspection & technical design | Done |
| 1 | Project foundation | Not started |
| 2 | Protobuf and API layer | Not started |
| 3 | Persistence (PostgreSQL) | Not started |
| 4 | Worker registration & heartbeats | Not started |
| 5 | Scheduler | Not started |
| 6 | Job execution | Not started |
| 7 | Cancellation and preemption | Not started |
| 8 | Checkpointing and recovery | Not started |
| 9 | Real GPU and NCCL execution | Not started |
| 10 | Observability | Not started |
| 11 | Dashboard | Not started |
| 12 | Docker Compose and Kubernetes | Not started |
| 13 | AWS and Terraform | Not started |
| 14 | Benchmarks and final hardening | Not started |

## Implemented vs. simulated vs. not yet validated

Nothing is implemented yet. This table will be filled in starting Phase 1 and
is the authoritative place to check "is X real or simulated" at any point in
the project. The rule going forward:

- **Implemented** — code exists, is tested, and runs in this repo's CI or
  local Docker Compose stack.
- **Simulated** — behavior is provided by the deterministic fake-GPU/fake-worker
  path, clearly labeled as simulated in logs, metrics, and docs.
- **Not yet validated** — code may exist (e.g. the real-GPU/NCCL path) but has
  not been run against real hardware or a real cloud account, so no results
  are reported for it.

## Known issues

- None yet — no code exists.

## Measured results

- None yet. No benchmark numbers, GPU results, or cloud deployment results
  will be reported until they come from an actual run in this repo, per the
  project's execution rules. See `docs/benchmarks/` (created, currently empty
  pending Phase 14).

## Assumptions and environment notes (from Phase 0 inspection)

- Dev machine: Windows 11 with Git Bash (MINGW64), Docker Desktop (Linux
  containers) running, no NVIDIA GPU detected (`nvidia-smi` not found). All
  GPU behavior will be developed and tested through the fake-GPU simulator;
  the real-GPU/NCCL path (Phase 9) will be written but marked unvalidated
  until run on real hardware.
- Available toolchain confirmed locally: Go 1.27, Python 3.12.10, Node 24.14 /
  npm 11.9, Docker 29.3.1 + Compose v5.1.1, Terraform 1.15.8, kubectl 1.34.1
  (client only), gh CLI 2.98.0.
- Not present locally: `make`, `protoc`/`buf`, `etcd`, `psql`, `golangci-lint`,
  `nvidia-smi`. The tooling plan (see
  [ADR-0000](docs/adr/0000-local-tooling-adaptations.md)) routes around these
  with `go install`-able Go binaries and Docker-only services instead of
  requiring host installs.
- This machine already has an unrelated `kind` cluster (`kind-aegisops-dev`)
  active from another project. OrionQueue's Kubernetes phase will create its
  own `kind` cluster (e.g. `orionqueue-dev`) rather than reuse that one.
- Per explicit user instruction, Claude does not run `git commit` or
  `git push` in this repo. Every phase's report includes the exact commands
  for the user to run instead.
- This is a 14-phase, multi-service system (Go control plane, Python worker
  agent, React dashboard, Postgres, etcd, Prometheus/Grafana, Kubernetes,
  Terraform). It will be built incrementally across many sessions, each phase
  gated on the previous one's tests passing, not delivered in a single pass.

## Next phase

**Phase 1 — Project foundation**: Go module, Python worker package skeleton,
React frontend scaffold, configuration system, structured logging, health
endpoints, Dockerfiles, CI workflow, and the first real unit tests.
