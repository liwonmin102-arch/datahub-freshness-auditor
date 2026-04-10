#!/usr/bin/env python3
"""
Level 1 — Data Freshness Auditor
DataHub x Nebius Hackathon (Problem 3)

Reads freshness metadata from a SQLite pipeline database, builds a structured
context, and asks a Nebius-hosted LLM to produce a prioritized Freshness
Verification Plan.
"""

import json
import os
import sqlite3
import sys
import textwrap
from datetime import datetime
from pathlib import Path

from openai import OpenAI

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = Path.home() / "static-assets" / "datasets" / "nyc-taxi" / "nyc_taxi_pipeline.db"
NEBIUS_BASE_URL = "https://api.studio.nebius.com/v1/"
NEBIUS_MODEL = "deepseek-ai/DeepSeek-R1-0528"
OUTPUT_DIR = Path(__file__).resolve().parent / "reports"

# Pipeline dependency graph (upstream → downstream)
PIPELINE_GRAPH = {
    "raw_trips": {"downstream": ["staging_trips"], "sla_hours": 24},
    "staging_trips": {"downstream": ["mart_daily_summary"], "sla_hours": 24},
    "mart_daily_summary": {"downstream": [], "sla_hours": 24},
}

# Columns that represent the "freshness timestamp" for each table
TIMESTAMP_COLUMNS = {
    "raw_trips": "tpep_pickup_datetime",
    "staging_trips": "tpep_pickup_datetime",
    "mart_daily_summary": "trip_date",
}


# ---------------------------------------------------------------------------
# Database introspection
# ---------------------------------------------------------------------------
def get_table_metadata(conn: sqlite3.Connection) -> list[dict]:
    """Return schema info, row counts, and freshness timestamps for every table."""
    tables = []
    cursor = conn.cursor()

    for table_name, ts_col in TIMESTAMP_COLUMNS.items():
        # Column info
        cols = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
        columns = [{"name": c[1], "type": c[2]} for c in cols]

        # Row count
        row_count = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        # Freshness
        max_ts = cursor.execute(f"SELECT MAX({ts_col}) FROM {table_name}").fetchone()[0]
        min_ts = cursor.execute(f"SELECT MIN({ts_col}) FROM {table_name}").fetchone()[0]

        # Sample of recent dates for mart table
        recent_sample = None
        if table_name == "mart_daily_summary":
            rows = cursor.execute(
                "SELECT trip_date, trip_count FROM mart_daily_summary ORDER BY trip_date DESC LIMIT 10"
            ).fetchall()
            recent_sample = [{"date": r[0], "trip_count": r[1]} for r in rows]

        tables.append({
            "table": table_name,
            "columns": columns,
            "row_count": row_count,
            "timestamp_column": ts_col,
            "max_timestamp": max_ts,
            "min_timestamp": min_ts,
            "recent_sample": recent_sample,
            "pipeline": PIPELINE_GRAPH[table_name],
        })

    return tables


def build_context(metadata: list[dict]) -> str:
    """Build a structured context string for the LLM."""
    lines = [
        "# NYC Taxi Data Pipeline — Freshness Metadata",
        "",
        "## Pipeline Flow",
        "raw_trips → staging_trips → mart_daily_summary",
        "",
        "## SLA Requirement",
        "Each stage must be refreshed within 24 hours of its upstream source.",
        "",
        "## Table Metadata",
        "",
    ]

    for t in metadata:
        lines.append(f"### {t['table']}")
        lines.append(f"- **Row count**: {t['row_count']:,}")
        lines.append(f"- **Timestamp column**: `{t['timestamp_column']}`")
        lines.append(f"- **Data range**: {t['min_timestamp']} → {t['max_timestamp']}")
        lines.append(f"- **Downstream tables**: {t['pipeline']['downstream'] or 'none (terminal)'}")
        lines.append(f"- **SLA**: {t['pipeline']['sla_hours']}h")
        col_names = ", ".join(f"`{c['name']}` ({c['type']})" for c in t["columns"])
        lines.append(f"- **Columns**: {col_names}")
        if t["recent_sample"]:
            lines.append("- **Recent daily summary rows (newest first):**")
            for row in t["recent_sample"]:
                lines.append(f"  - {row['date']}: {row['trip_count']} trips")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = textwrap.dedent("""\
    You are a senior data reliability engineer specializing in data pipeline
    freshness monitoring. Given metadata about a data pipeline, produce a
    structured **Freshness Verification Plan**.

    The plan must include:
    1. An executive summary of the pipeline health.
    2. A prioritized list of tables to investigate, ranked by severity.
       For each table state:
       - Priority (P0 / P1 / P2)
       - The staleness gap (how far behind the table is)
       - Root-cause hypothesis
       - Specific SQL queries to run for verification
       - Recommended remediation steps
    3. A dependency-aware investigation order (check upstream before downstream).
    4. Any anomalies you notice in the sample data.

    Be specific, actionable, and concise. Format the output as clean Markdown.
""")


def call_nebius(context: str) -> str:
    """Send context to the Nebius LLM and return the freshness plan."""
    api_key = os.environ.get("NEBIUS_API_KEY")
    if not api_key:
        print("ERROR: NEBIUS_API_KEY environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    client = OpenAI(base_url=NEBIUS_BASE_URL, api_key=api_key)

    print("🔄 Sending metadata to Nebius LLM (DeepSeek-R1-0528)…")
    response = client.chat.completions.create(
        model=NEBIUS_MODEL,
        temperature=0.6,
        max_tokens=4096,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Here is the freshness metadata for our NYC Taxi data pipeline. "
                    "Please produce a Freshness Verification Plan.\n\n"
                    + context
                ),
            },
        ],
    )

    return response.choices[0].message.content


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
def save_report(plan: str, context: str) -> Path:
    """Save the verification plan as a timestamped Markdown report."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUTPUT_DIR / f"freshness_plan_{ts}.md"
    report = "\n".join([
        "# Data Freshness Verification Plan",
        f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
        "",
        "---",
        "",
        plan,
        "",
        "---",
        "",
        "<details><summary>Raw pipeline metadata (input context)</summary>",
        "",
        context,
        "",
        "</details>",
    ])
    path.write_text(report)
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("  Level 1 — Data Freshness Auditor")
    print("  DataHub x Nebius Hackathon")
    print("=" * 60)
    print()

    # Step 1: Connect to database
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(str(DB_PATH))
    print(f"📂 Connected to {DB_PATH}")

    # Step 2: Extract metadata
    metadata = get_table_metadata(conn)
    conn.close()
    print(f"📊 Extracted metadata for {len(metadata)} tables")

    for t in metadata:
        print(f"   • {t['table']}: {t['row_count']:,} rows, latest = {t['max_timestamp']}")
    print()

    # Step 3: Build context
    context = build_context(metadata)

    # Step 4: Call Nebius LLM
    plan = call_nebius(context)
    print()

    # Step 5: Display plan
    print("=" * 60)
    print("  FRESHNESS VERIFICATION PLAN")
    print("=" * 60)
    print()
    print(plan)
    print()

    # Step 6: Save report
    report_path = save_report(plan, context)
    print(f"💾 Report saved to {report_path}")


if __name__ == "__main__":
    main()
