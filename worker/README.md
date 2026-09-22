# OrionQueue worker agent

The Python worker agent for OrionQueue. See the [repo-level
README](../README.md) and
[docs/architecture/system-overview.md](../docs/architecture/system-overview.md)
for the full system design — this file only covers running this package on
its own.

## Setup

```bash
cd worker
python -m venv .venv
source .venv/Scripts/activate   # Windows Git Bash; use .venv/bin/activate on Linux/macOS
pip install -e ".[dev]"
```

## Run

```bash
python -m agent.main
# or, once installed:
orionqueue-worker
```

Phase 1 scope: the agent starts, logs structured JSON to stdout, and runs
until interrupted (Ctrl+C). Registration with the control plane and job
execution are not implemented yet — see `../PROJECT_STATUS.md`.

## Test / lint / format

```bash
pytest
ruff check .
black --check .
```
