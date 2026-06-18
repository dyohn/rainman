# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Rainman is an experimental distributed key-value storage engine implemented in Python, built as an independent study project alongside *Designing Data-Intensive Applications* (Kleppmann). The system runs as a three-node Docker cluster with single-leader replication, and includes two AI agents: an **Adversarial Fault Injector** (introduces failures autonomously) and a **Consistency Observer** (read-only log monitoring and anomaly reporting).

See [project-description.md](project-description.md) for the full spec, including evaluation criteria, dataset details, and the DDIA chapter mapping.

## Development Setup

```bash
# Initial setup — creates venv and installs all dev dependencies (editable install)
./devSetup.sh

# Activate venv for a dev session
source venv/bin/activate
```

## Common Commands

```bash
# Run tests
./runTests.sh
# or directly:
pytest tests

# Run a single test file
pytest tests/path/to/test_file.py

# Run a single test by name
pytest tests/ -k "test_name"

# Lint
ruff check src/

# Format
ruff format src/

# Build wheel
./build.sh

# Clean build artifacts
./clean.sh
```

## Architecture

### Core System

The KV store exposes `get`, `set`, and `delete` operations. All writes go through an elected leader, which replicates asynchronously to followers. Every node maintains a replication log used for recovery and agent observation.

**Failure scenarios the system must handle:**
- Leader failure → election → follower resynchronization
- Network partition (split-brain) → node isolation → rejoin with resync
- Recovery invariant: all live nodes agree on state, no acknowledged writes lost

### AI Agents

Both agents share a single configurable model endpoint, allowing a one-line switch between Phase 1 (Ollama, local models) and Phase 2 (Claude API).

| Agent | Role | Access |
|---|---|---|
| Fault Injector | Autonomously pauses nodes, drops traffic, kills leaders based on observed state | Read + write (Docker/network controls) |
| Consistency Observer | Reads replication logs and status endpoints; flags lag, divergence, split-brain | Read-only |

**Phase 1 (development):** Ollama — `llama3.1:8b` or `mistral:7b`; larger models (`llama3.1:70b`, `qwen2.5:32b`) preferred if hardware allows. The Observer works well with small models; the Injector may show inconsistency and should be monitored.

**Phase 2 (integration/demo):** Claude API — `claude-sonnet-4-6`. Triggered once the three-node cluster has confirmed replication working.

### Build System

The `src/` layout uses `hatchling` as the build backend. Source under `src/` maps to the package root at install time (see `[tool.hatch.build.targets.wheel.sources]`).

## Tooling Notes

- **Python version:** 3.13 required
- **Linter/formatter:** `ruff` (line length 79, target py313)
- **Test runner:** `pytest` — report output goes to `unitTestsReport.xml`
- **Infrastructure:** Docker + Docker Compose for the three-node cluster
