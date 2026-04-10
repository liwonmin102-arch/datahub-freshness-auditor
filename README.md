# Data Freshness Auditor — DataHub x Nebius Hackathon

**Problem 3: Data Freshness Auditor**

A 3-level system that audits data pipeline freshness using **DataHub** as the metadata/context layer and **Nebius** (DeepSeek-R1) as the reasoning engine. It detects stale data, diagnoses root causes, and takes corrective actions automatically.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     NYC Taxi Data Pipeline                          │
│                                                                     │
│   raw_trips (250k) ──→ staging_trips (208k) ──→ mart_daily_summary │
│   through 2016-03-10    through 2016-03-01       through 2016-03-01│
│        ✅ OK              ❌ 9 DAYS STALE          ❌ STALE          │
└─────────────────────────────────────────────────────────────────────┘

Level 1: SQLite metadata ──→ Nebius LLM ──→ Freshness verification plan
Level 2: SQLite metadata ──→ DataHub SDK ──→ Register datasets + tags + lineage
Level 3: DataHub GraphQL + SQLite queries ──→ Nebius LLM diagnosis ──→ Update DataHub + alert owners
```

## Tech Stack

- **Python** — all scripts
- **DataHub SDK** (`acryl-datahub`) — metadata emission via `DatahubRestEmitter`, verification via GraphQL
- **Nebius Token Factory** — DeepSeek-R1-0528 via OpenAI-compatible API
- **SQLite** — pipeline database (`nyc_taxi_pipeline.db`)

## Dataset

**NYC Taxi Pipeline** with planted staleness issues:

| Table | Rows | Latest Data | Status |
|-------|------|-------------|--------|
| `raw_trips` | 250,000 | 2016-03-10 | Fresh |
| `staging_trips` | 208,675 | 2016-03-01 | **9 days stale** |
| `mart_daily_summary` | 41 | 2016-03-01 | **Stale (inherited)** |

Additional anomalies: near-empty loads (2 rows on 2016-02-25), 335-day data gap, 16.5% row drop at staging.

## What It Finds

- `staging_trips` is **9 days behind** `raw_trips` — the ETL pipeline has stalled
- `mart_daily_summary` inherits this staleness from its upstream
- Multiple dates with near-zero row counts (empty/failed loads)
- Large gaps in daily data coverage
- 16.5% row filtering from raw to staging

## Setup

```bash
pip install -r requirements.txt
```

### Prerequisites

- DataHub running locally (GMS at `localhost:8080`, UI at `localhost:9002`)
- Auth token in `~/.datahubenv`
- `NEBIUS_API_KEY` env var (optional — Level 3 falls back to rule-based diagnosis)

## How to Run

Run the levels in order — Level 2 registers datasets that Level 3 reads back.

### Level 1: Freshness Verification Plan

Reads SQLite metadata, sends to Nebius LLM, generates a prioritized verification plan.

```bash
export NEBIUS_API_KEY="your-key"
python level1_freshness_plan.py
```

Output: console + `reports/freshness_plan_<timestamp>.md`

### Level 2: Write Back to DataHub

Registers all 3 tables in DataHub with schemas, lineage, freshness tags, and descriptions.

```bash
python level2_write_back.py
```

After running: search `nyc_taxi_pipeline` in DataHub UI at `localhost:9002`.

### Level 3: Freshness Audit Agent (Main Demo)

The full agent loop — reads from DataHub, queries real data, gets LLM diagnosis, updates DataHub, alerts owners.

```bash
# With LLM:
export NEBIUS_API_KEY="your-key"
python level3_freshness_agent.py

# Without LLM (rule-based fallback):
python level3_freshness_agent.py
```

Output: colored terminal with 5 phases, Slack-style alerts, `reports/freshness_audit_<timestamp>.md`

### Level Comparison

| | Level 1 | Level 2 | Level 3 |
|---|---------|---------|---------|
| **Reads from** | SQLite | SQLite | DataHub + SQLite |
| **LLM role** | Generate plan | — | Diagnose + severity |
| **Writes to** | Console/file | DataHub | DataHub (updates) |
| **Actions** | None | Register datasets | Update tags, alert owners |

## Reliability

Level 3 is hardened for live demos:
- **Retries** LLM calls up to 3 times with progressively stricter prompts
- **Strips** DeepSeek-R1 `<think>` tags and markdown fences before JSON parsing
- **Falls back** to rule-based diagnosis if the LLM is unavailable or returns unparseable output
- **Never crashes** — always produces a clean formatted report
