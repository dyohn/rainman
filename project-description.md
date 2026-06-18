# Distributed Key-Value Storage Engine with Agentic Fault Injection and Consistency Monitoring

**Course:** Independent Study
**Timeline:** Four Weeks (Part-Time)
**Reference Text:** *Designing Data-Intensive Applications* — Martin Kleppmann (Release 17, November 2021)

---

## Overview

This project implements a miniature distributed key-value storage engine designed to demonstrate core principles from *Designing Data-Intensive Applications*: replication, fault tolerance, consistency, and recovery. The system is augmented with two lightweight AI agents — an Adversarial Fault Injector and a Consistency Observer — that automate failure scenarios and monitor cluster health in real time. Together, the system and its agents provide an end-to-end demonstration of how distributed storage systems behave under failure conditions and how those failures can be observed and reasoned about.

---

## Core System

### Architecture

- **Three-node cluster** deployed via Docker containers, each simulating an independent host
- **Leader-based replication** — one elected leader accepts writes and propagates them to follower nodes
- **Key-value interface** — simple `get`, `set`, and `delete` operations
- **Replication log** — all writes are logged per node to support recovery and agent observation

### Replication Model

The system uses a single-leader replication model consistent with Chapter 5 of DDIA. The leader is the authoritative source of truth for writes. Followers replicate asynchronously and serve reads. Leader election is handled via a lightweight consensus mechanism (e.g., Raft or a simplified equivalent).

### Failure and Recovery

The system must demonstrate:

- **Leader failure** — detection, election of a new leader, and follower resynchronization
- **Network partition** — isolation of one or more nodes into disconnected groups (split-brain scenario)
- **Node rejoin** — a previously partitioned or failed node rejoins the cluster and resynchronizes without data loss

Recovery is defined as: *all live nodes agree on the same data state, the elected leader is unambiguous, and no acknowledged writes have been lost.*

---

## AI Agent Components

### Agent 1: Adversarial Fault Injector

**Role:** Autonomously introduces failures into the cluster based on observed system state.

**Behavior:**
- Monitors node health and replication status via status endpoints or log polling
- Selects from a defined action set: pause a node, drop traffic between two nodes, kill the leader
- Makes decisions based on current cluster state rather than a fixed script
- Logs each decision and its rationale for review

**Scope Constraint:** The agent's decision space is intentionally limited to a small, well-defined action set. The goal is motivated, observable decision-making — not a complex autonomous planner.

### Agent 2: Consistency Observer

**Role:** Monitors replication logs and node state, flagging divergence and anomalies in real time.

**Behavior:**
- Reads structured log output and node status endpoints continuously
- Detects and reports: replication lag, node divergence, split-brain indicators, delayed acknowledgments
- Produces human-readable observations (e.g., *"Node 3's last confirmed write is 14 seconds behind Node 1 — possible replication lag or partition"*)
- Operates read-only; cannot affect cluster state

**Scope Constraint:** The observer reasons over structured log data rather than implementing custom anomaly detection algorithms. Observations are LLM-generated interpretations of system state.

### Agent Interaction

The two agents are complementary but loosely coupled:

- The Fault Injector creates failure conditions
- The Consistency Observer watches whether the system detects and surfaces those failures
- Together they demonstrate the full failure lifecycle: *inject → observe → recover*

Agents do not engage in elaborate inter-agent communication. Simplicity and correctness are prioritized over architectural complexity.

---

## Development Plan

### Recommended Build Order

1. **Core KV store** — single-node implementation with `get`, `set`, `delete`
2. **Replication layer** — leader election, follower sync, replication log
3. **Docker networking** — three-node cluster, network partition simulation
4. **Consistency Observer** — read-only log monitoring and anomaly flagging
5. **Fault Injector** — adversarial agent with defined action set and state-based decisions
6. **Integration and demonstration** — end-to-end failure/recovery scenarios with both agents active

### Tooling

- **Infrastructure:** Docker, Docker Compose
- **Implementation Language:** Python
- **AI Agents:** Phased model strategy — see below
- **Development Assistant:** Claude Code

### AI Model Strategy

Agent development follows a two-phase model strategy to minimize API costs during early development while ensuring reliability during integration and demonstration.

**Phase 1 — Initial Development (Ollama, local open source models)**

During scaffolding and early agent development, agents will use locally-hosted open source models via Ollama. Recommended starting models:

- `llama3.1:8b` or `mistral:7b` — fast, low memory footprint, suitable for structured log interpretation tasks
- `llama3.1:70b` or `qwen2.5:32b` — preferred if hardware supports it, offering better instruction-following for the Fault Injector's decision logic

The Consistency Observer is well-suited to local models throughout Phase 1, as its task (reading structured log output and producing plain-English observations) is straightforward and does not require complex reasoning. The Fault Injector may exhibit inconsistent decision quality with smaller models; this is acceptable during scaffolding but should be monitored as cluster testing begins.

**Phase 2 — Integration and Demonstration (Claude API)**

Once the core system reaches initial stability — defined as a functioning three-node cluster with confirmed replication — agents will be migrated to the Claude API (`claude-sonnet-4-6`) for integration testing and final demonstration. This ensures reliable instruction-following during documented failure/recovery scenarios where agent decision quality directly affects project outcomes.

**Implementation note:** The model endpoint will be defined as a configuration parameter from the start, so the transition from Ollama to the Claude API requires a single config change rather than a code refactor.

### Stretch Goal

If the core project is completed ahead of schedule, a **leaderless replication mode** may be added. This would allow direct comparison of leader-based and leaderless behavior under identical failure scenarios — a meaningful extension that maps directly to Chapter 5 of DDIA.

---

## Evaluation Criteria

The project is considered complete when the following can be demonstrated:

- Three nodes running with one elected leader
- Writes replicated to followers and confirmed
- Leader can be killed and a new leader elected without data loss
- A partitioned node can rejoin and resynchronize
- The Consistency Observer correctly identifies a divergence or lag condition
- The Fault Injector makes at least one autonomous, state-driven failure decision
- All events are observable via logs or a status endpoint

---

## Dataset

### Requirements

The dataset must satisfy the following criteria:

- **Format:** JSON objects with string keys and richly structured JSON values
- **Size:** ~20 MB per replica (~60 MB total across three nodes)
- **Scale:** ~10,000 key-value pairs
- **Value structure:** Heterogeneous, nested JSON — varying field counts and value sizes — to stress replication and recovery realistically rather than with uniform synthetic records
- **Key type:** Natural string identifiers present in the source data (no artificial key generation required)

### Primary Option — Yelp Academic Dataset (Business Subset)

The preferred dataset is a 10,000-record subset of the Yelp Academic Dataset's `yelp_academic_dataset_business.json` file. Each record represents a business listing and contains rich, nested JSON: name, address, city, state, coordinates, star rating, review count, hours of operation, and a nested attributes object covering parking, ambience, noise level, and similar properties. This structure produces heterogeneous value sizes well-suited to testing a distributed KV store under realistic load.

**Key field:** `business_id` (a unique string per record, e.g., `"Pns2l4eNsfO8kk83dixA6A"`)

**Value:** The remainder of the business record serialized as a JSON string or bytes

**Access:** Available free for academic and educational use from [https://www.yelp.com/dataset](https://www.yelp.com/dataset). Requires agreement to Yelp's dataset terms before download. A 10,000-record slice of the full file (~113 MB uncompressed) is extracted at load time via a preparation script.

**License:** Yelp Dataset License — academic and non-commercial use permitted.

### Secondary Option — Synthetic Data Generation

If the Yelp dataset is unavailable or unsuitable, a synthetic dataset will be generated using a Claude Code-assisted Python script. The generator will produce 10,000 key-value pairs matching the size and structural profile of the primary dataset:

- **Keys:** UUID-based or slug-style strings (e.g., `"biz-00042"`)
- **Values:** Nested JSON objects with a mix of string, integer, float, boolean, array, and sub-object fields
- **Size distribution:** Randomized value sizes averaging ~2 KB, with a realistic long-tail distribution (some small records, occasional larger ones)
- **Access patterns:** A configurable hot-key ratio (e.g., 20% of keys receiving 80% of traffic) to simulate realistic workload skew during fault injection testing

The synthetic generator will be version-controlled alongside the project so that test conditions are fully reproducible.

### Data Loading

Regardless of which option is used, a data loader script will be provided that:

1. Reads the source file or generator output
2. Extracts or constructs the string key and JSON value for each record
3. Bulk-loads all records into the KV store via the `set` interface before fault injection begins
4. Verifies that all three nodes have received and confirmed the full dataset prior to any test scenario

---

## Connection to DDIA

| Project Component | DDIA Chapter |
|---|---|
| Single-leader replication | Chapter 5 — Replication |
| Leader election and failover | Chapter 5 — Replication |
| Network partitions and split-brain | Chapter 8 — The Trouble with Distributed Systems |
| Recovery and consistency after failure | Chapter 9 — Consistency and Consensus |
| Replication logs | Chapter 5 — Replication, Chapter 11 — Stream Processing |
| Observability and anomaly detection | Chapter 1 — Reliable, Scalable, Maintainable Applications |

---

## Future Work

- **Isolation anomaly reproduction** — dirty reads, non-repeatable reads, phantom reads, write skew at varying isolation levels (deferred from current scope)
- **Leaderless replication mode** (elevated from stretch goal if not completed)
- **Quantitative benchmarking** — replication lag under load, time-to-election, recovery time

---

*Document version: preliminary draft.*
