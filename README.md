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
