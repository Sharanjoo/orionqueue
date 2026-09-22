# ADR-0006: Kubernetes deployment strategy

Status: Accepted (Phase 0)

## Context

The brief asks for Kubernetes deployment artifacts covering API, scheduler,
worker, dependencies, RBAC, probes, resource limits, and an optional
GPU-worker profile — deployable in a fake-worker mode without requiring GPUs,
and validated locally where possible.

## Decision

- Start with **plain Kubernetes manifests** organized per-component under
  `deploy/kubernetes/` (Kustomize-friendly base + overlays for
  fake-worker vs. GPU-worker profiles), not a Helm chart, for Phase 12. A
  single-developer portfolio project doesn't yet have the multiple
  environments/consumers that justify Helm's templating overhead; plain
  manifests are also easier for a reader/reviewer to read top to bottom.
  Revisit as a Helm chart later only if packaging/distribution actually needs
  it — documented here so it isn't mistaken for an oversight.
- **Fake-worker deployment is the default and the one actually validated
  locally**, against a dedicated `kind` cluster (`orionqueue-dev`, created
  fresh rather than reusing the unrelated `kind-aegisops-dev` cluster already
  on this machine).
- **GPU-worker deployment is written but explicitly hardware-dependent**: it
  assumes the NVIDIA device plugin is installed on the target cluster,
  requests `nvidia.com/gpu` resources, uses a CUDA-enabled worker image, and
  documents the node labels/tolerations it expects. It is not applied or
  validated locally (no GPU nodes available) — the manifest is marked
  "demo-grade, unvalidated" until run against a real GPU node pool.
- Dependencies (PostgreSQL, etcd): manifests support both an in-cluster
  Compose-equivalent deployment (for the fake-worker demo profile) and
  pointing at externally managed instances via config (for a more realistic
  production posture) — documented per-component rather than hard-coding one
  assumption.
- Baseline hardening included from the start: non-root containers,
  read-only root filesystem where practical, resource requests/limits,
  liveness/readiness probes, a dedicated ServiceAccount with least-privilege
  RBAC, and a PodDisruptionBudget for the API/scheduler where replica counts
  make it meaningful.

## Consequences

- Manifests are more verbose than a Helm chart would be, but every value is
  visible without running a template engine, which fits a portfolio project
  meant to be read.
- Only the fake-worker profile gets an actual "it deployed and I watched it
  come up" validation in this project's environment; the GPU profile and any
  managed-database/etcd wiring for a real cluster stay documented-but-untested
  until run against infrastructure this project has access to, consistent
  with the project's no-fabricated-results rule.
