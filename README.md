# Data Freshness Auditor

**DataHub x Nebius Hackathon — Problem 3**

Automated detection and diagnosis of stale data in multi-stage pipelines.

## Level 1: Freshness Verification Plan

Reads pipeline metadata from a SQLite database, builds a structured context, and uses a Nebius-hosted LLM (DeepSeek-R1) to generate a prioritized **Freshness Verification Plan**.

### Pipeline Under Test

```
raw_trips (250k rows) → staging_trips (208k rows) → mart_daily_summary (41 rows)
```

The pipeline has **planted staleness**: `raw_trips` has data through 2016-03-10, but downstream tables are stuck at 2016-03-01 — a ~9 day gap.

### What It Does

1. **Introspects** the SQLite database — extracts table schemas, row counts, timestamp ranges
2. **Builds context** — structures all metadata into a prompt-ready format with SLA definitions and the pipeline dependency graph
3. **Calls Nebius LLM** — sends the context to DeepSeek-R1-0528 via the OpenAI-compatible API
4. **Generates a plan** — the LLM returns a prioritized verification plan: which tables to check, what queries to run, what anomalies to look for
5. **Saves a report** — outputs to console and writes a timestamped Markdown file in `reports/`

### Setup

```bash
pip install -r requirements.txt
export NEBIUS_API_KEY="your-key-here"
```

### Run

```bash
python level1_freshness_plan.py
```

### Output

The script prints the verification plan to the console and saves it as a Markdown report in `reports/freshness_plan_<timestamp>.md`.

## Level 2: Write Freshness Status to DataHub

Registers all pipeline tables as DataHub datasets with full metadata — schemas, lineage, freshness tags, and staleness descriptions — then verifies everything via GraphQL.

### What It Does

1. **Extracts metadata** from SQLite (reuses Level 1 introspection logic)
2. **Computes staleness** by comparing each table's max timestamp against its upstream
3. **Emits to DataHub** via the Python SDK (`DatahubRestEmitter`):
   - **Dataset registration** — each table as a `sqlite` / `nyc_taxi_pipeline` dataset
   - **Schema metadata** — column names and types from SQLite `PRAGMA table_info`
   - **Lineage** — `raw_trips → staging_trips → mart_daily_summary`
   - **Freshness tags** — `freshness:ok` or `freshness:critical_stale` per table
   - **Descriptions** — human-readable staleness summaries (e.g. "STALE: 9 days behind upstream")
   - **Custom properties** — row counts, SLA hours, staleness gap, pipeline stage
4. **Verifies via GraphQL** — queries each dataset back and prints a summary

### Prerequisites

- DataHub running locally (GMS at `localhost:8080`, UI at `localhost:9002`)
- Auth token in `~/.datahubenv`

### Run

```bash
python level2_write_back.py
```

### After Running

Search for `nyc_taxi_pipeline` or `freshness` in the DataHub UI at `localhost:9002` to see the registered datasets, lineage graph, and freshness tags.

## Level 3: Freshness Audit Agent

The main demo script. A human-triggered agent that reads from DataHub, queries real data for evidence, gets an LLM diagnosis, then **acts** — updating DataHub and alerting owners.

### What It Does

1. **Reads FROM DataHub** (GraphQL) — fetches all 3 datasets with their tags, descriptions, lineage, and custom properties
2. **Queries real data** (SQLite) — MAX timestamps, daily row counts, date gap detection, empty load detection, cross-stage row count comparison
3. **LLM diagnosis** (Nebius DeepSeek-R1) — sends DataHub metadata + real evidence, gets back structured JSON with severity ratings and recommended actions. Falls back to rule-based diagnosis if `NEBIUS_API_KEY` is not set.
4. **Acts on findings:**
   - Updates DataHub tags based on diagnosis severity
   - Writes incident reports as dataset descriptions in DataHub
   - Prints Slack-style alerts addressed to `@data_platform_team`
5. **Saves audit report** — full Markdown report with issue table, evidence, and actions taken

### Key Difference from Level 1-2

| | Level 1 | Level 2 | Level 3 |
|---|---------|---------|---------|
| **Reads from** | SQLite only | SQLite only | DataHub + SQLite |
| **LLM role** | Generate plan | (none) | Diagnose issues |
| **Writes to** | Console/file | DataHub | DataHub (updates) |
| **Actions** | None | Register datasets | Update tags, alert owners |

### Run

```bash
# With LLM diagnosis:
export NEBIUS_API_KEY="your-key"
python level3_freshness_agent.py

# Without LLM (rule-based fallback):
python level3_freshness_agent.py
```

### Sample Output

The agent produces colored terminal output with 5 clear phases, Slack-style alert blocks, and saves a detailed Markdown report to `reports/freshness_audit_<timestamp>.md`.
