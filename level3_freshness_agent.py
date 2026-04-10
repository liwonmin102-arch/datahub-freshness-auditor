#!/usr/bin/env python3
"""
Level 3 — Freshness Audit Agent
DataHub x Nebius Hackathon (Problem 3)

Human-triggered agent that:
  1. Reads pipeline metadata FROM DataHub (GraphQL)
  2. Queries real data in SQLite for evidence
  3. Sends findings to Nebius LLM for diagnosis
  4. ACTS: updates DataHub tags/descriptions, alerts owners
  5. Saves a full audit report
"""

import json
import os
import re
import sqlite3
import sys
import textwrap
import time
from datetime import datetime
from pathlib import Path

import requests
import yaml

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    GlobalTagsClass,
    TagAssociationClass,
)
from openai import OpenAI

# ---------------------------------------------------------------------------
# Terminal colors
# ---------------------------------------------------------------------------
class C:
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    RESET = "\033[0m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"


def header(text, color=C.CYAN):
    width = 64
    print()
    print(f"{color}{C.BOLD}{'━' * width}")
    print(f"  {text}")
    print(f"{'━' * width}{C.RESET}")


def subheader(text, color=C.BLUE):
    print(f"\n{color}{C.BOLD}▸ {text}{C.RESET}")


def ok(text):
    print(f"  {C.GREEN}✓{C.RESET} {text}")


def warn(text):
    print(f"  {C.YELLOW}⚠{C.RESET} {text}")


def fail(text):
    print(f"  {C.RED}✗{C.RESET} {text}")


def info(text):
    print(f"  {C.DIM}│{C.RESET} {text}")


def badge(label, color):
    return f"{color}{C.BOLD}[{label}]{C.RESET}"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = (
    Path.home()
    / "static-assets"
    / "datasets"
    / "nyc-taxi"
    / "nyc_taxi_pipeline.db"
)
DATAHUB_ENV_PATH = Path.home() / ".datahubenv"
PLATFORM = "sqlite"
PLATFORM_INSTANCE = "nyc_taxi_pipeline"
ENV = "PROD"
NEBIUS_BASE_URL = "https://api.studio.nebius.com/v1/"
NEBIUS_MODEL = "deepseek-ai/DeepSeek-R1-0528"
OUTPUT_DIR = Path(__file__).resolve().parent / "reports"

TABLE_NAMES = ["raw_trips", "staging_trips", "mart_daily_summary"]

TIMESTAMP_COLUMNS = {
    "raw_trips": "tpep_pickup_datetime",
    "staging_trips": "tpep_pickup_datetime",
    "mart_daily_summary": "trip_date",
}

ALERT_OWNERS = "data_platform_team"


def make_urn(table_name: str) -> str:
    return make_dataset_urn_with_platform_instance(
        platform=PLATFORM,
        name=table_name,
        platform_instance=PLATFORM_INSTANCE,
        env=ENV,
    )


# ---------------------------------------------------------------------------
# 1. Read FROM DataHub
# ---------------------------------------------------------------------------
DATASET_QUERY = """
query getDataset($urn: String!) {
  dataset(urn: $urn) {
    urn
    name
    properties {
      name
      description
      customProperties { key value }
    }
    tags {
      tags {
        tag {
          urn
          properties { name description }
        }
      }
    }
    schemaMetadata {
      fields { fieldPath nativeDataType }
    }
    upstream: lineage(input: {direction: UPSTREAM, start: 0, count: 10}) {
      relationships { entity { urn type } }
    }
    downstream: lineage(input: {direction: DOWNSTREAM, start: 0, count: 10}) {
      relationships { entity { urn type } }
    }
  }
}
"""


def load_datahub_config():
    if not DATAHUB_ENV_PATH.exists():
        fail(f"{DATAHUB_ENV_PATH} not found")
        sys.exit(1)
    config = yaml.safe_load(DATAHUB_ENV_PATH.read_text())
    return config["gms"]["server"], config["gms"]["token"]


def fetch_datahub_metadata(server, token):
    """Fetch all 3 datasets from DataHub via GraphQL."""
    graphql_url = f"{server}/api/graphql"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    datasets = {}
    for name in TABLE_NAMES:
        urn = make_urn(name)
        resp = requests.post(
            graphql_url,
            headers=headers,
            json={"query": DATASET_QUERY, "variables": {"urn": urn}},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        ds = data.get("data", {}).get("dataset")
        if not ds:
            warn(f"{name}: not found in DataHub — will create during writeback")
            datasets[name] = None
            continue

        props = ds.get("properties") or {}
        custom = {
            p["key"]: p["value"]
            for p in (props.get("customProperties") or [])
        }
        tags = [
            t["tag"]["properties"]["name"]
            for t in (ds.get("tags", {}).get("tags") or [])
            if t.get("tag", {}).get("properties")
        ]
        upstream_urns = [
            r["entity"]["urn"]
            for r in (ds.get("upstream", {}).get("relationships") or [])
        ]
        downstream_urns = [
            r["entity"]["urn"]
            for r in (ds.get("downstream", {}).get("relationships") or [])
        ]
        fields = [
            {"name": f["fieldPath"], "type": f["nativeDataType"]}
            for f in ((ds.get("schemaMetadata") or {}).get("fields") or [])
        ]

        datasets[name] = {
            "urn": urn,
            "name": name,
            "description": props.get("description", ""),
            "custom_properties": custom,
            "tags": tags,
            "upstream_urns": upstream_urns,
            "downstream_urns": downstream_urns,
            "fields": fields,
            "datahub_freshness_status": custom.get("freshness_status", "UNKNOWN"),
            "datahub_staleness_days": custom.get("staleness_days", "?"),
            "datahub_max_timestamp": custom.get("max_timestamp", "?"),
            "datahub_row_count": custom.get("row_count", "?"),
        }

        status = custom.get("freshness_status", "UNKNOWN")
        tag_str = ", ".join(tags) if tags else "(none)"
        if status == "STALE":
            warn(f"{name}: {badge('STALE', C.RED)}  tag={tag_str}")
        else:
            ok(f"{name}: {badge(status, C.GREEN)}  tag={tag_str}")

    return datasets


# ---------------------------------------------------------------------------
# 2. Query real data for evidence
# ---------------------------------------------------------------------------
def query_real_data(conn):
    """Run verification queries against the actual SQLite database."""
    cursor = conn.cursor()
    evidence = {}

    for name, ts_col in TIMESTAMP_COLUMNS.items():
        subheader(f"Querying {name}")

        # MAX / MIN timestamps
        max_ts = cursor.execute(
            f"SELECT MAX({ts_col}) FROM {name}"
        ).fetchone()[0]
        min_ts = cursor.execute(
            f"SELECT MIN({ts_col}) FROM {name}"
        ).fetchone()[0]
        total_rows = cursor.execute(
            f"SELECT COUNT(*) FROM {name}"
        ).fetchone()[0]

        info(f"Rows: {total_rows:,}  Range: {min_ts} → {max_ts}")

        # Daily row counts (all days)
        if name == "mart_daily_summary":
            daily = cursor.execute(
                "SELECT trip_date, trip_count FROM mart_daily_summary "
                "ORDER BY trip_date"
            ).fetchall()
            daily_counts = [
                {"date": r[0], "count": int(r[1])} for r in daily
            ]
        else:
            daily = cursor.execute(
                f"SELECT DATE({ts_col}) as d, COUNT(*) as c "
                f"FROM {name} GROUP BY d ORDER BY d"
            ).fetchall()
            daily_counts = [
                {"date": r[0], "count": r[1]} for r in daily
            ]

        # Detect anomalies
        anomalies = []

        # Empty or near-empty loads (< 10 rows for a day)
        empty_days = [d for d in daily_counts if d["count"] < 10]
        if empty_days:
            for d in empty_days:
                anomalies.append({
                    "type": "empty_load",
                    "date": d["date"],
                    "count": d["count"],
                    "message": (
                        f"Near-empty load: {d['count']} rows on {d['date']}"
                    ),
                })
                warn(f"Empty/near-empty load: {d['date']} → "
                     f"{d['count']} rows")

        # Date gaps — find consecutive dates with missing days
        if len(daily_counts) >= 2:
            dates = sorted(daily_counts, key=lambda x: x["date"])
            for i in range(1, len(dates)):
                d1 = datetime.strptime(dates[i - 1]["date"], "%Y-%m-%d")
                d2 = datetime.strptime(dates[i]["date"], "%Y-%m-%d")
                gap = (d2 - d1).days
                if gap > 1:
                    anomalies.append({
                        "type": "date_gap",
                        "from_date": dates[i - 1]["date"],
                        "to_date": dates[i]["date"],
                        "gap_days": gap,
                        "message": (
                            f"{gap}-day gap: {dates[i-1]['date']} → "
                            f"{dates[i]['date']}"
                        ),
                    })
                    if gap > 5:
                        fail(f"Large gap: {dates[i-1]['date']} → "
                             f"{dates[i]['date']} ({gap} days)")
                    else:
                        warn(f"Gap: {dates[i-1]['date']} → "
                             f"{dates[i]['date']} ({gap} days)")

        evidence[name] = {
            "max_timestamp": max_ts,
            "min_timestamp": min_ts,
            "total_rows": total_rows,
            "daily_counts": daily_counts,
            "anomalies": anomalies,
            "num_days_with_data": len(daily_counts),
        }

    # Cross-stage comparison
    subheader("Cross-stage comparison")
    raw = evidence["raw_trips"]
    stg = evidence["staging_trips"]
    mart = evidence["mart_daily_summary"]

    row_drop = raw["total_rows"] - stg["total_rows"]
    drop_pct = (row_drop / raw["total_rows"]) * 100
    info(f"raw → staging: {raw['total_rows']:,} → {stg['total_rows']:,} "
         f"({row_drop:,} rows filtered, {drop_pct:.1f}%)")
    info(f"staging days: {stg['num_days_with_data']} → "
         f"mart days: {mart['num_days_with_data']}")

    # Check date coverage mismatch
    stg_dates = {d["date"] for d in stg["daily_counts"]}
    mart_dates = {d["date"] for d in mart["daily_counts"]}
    missing_in_mart = stg_dates - mart_dates
    if missing_in_mart:
        for d in sorted(missing_in_mart):
            warn(f"Date {d} in staging but missing from mart")

    evidence["_cross_stage"] = {
        "raw_to_staging_drop": row_drop,
        "raw_to_staging_drop_pct": round(drop_pct, 1),
        "staging_days": stg["num_days_with_data"],
        "mart_days": mart["num_days_with_data"],
        "dates_missing_in_mart": sorted(missing_in_mart),
    }

    return evidence


# ---------------------------------------------------------------------------
# 3. LLM Diagnosis
# ---------------------------------------------------------------------------
DIAGNOSIS_SYSTEM_PROMPT = textwrap.dedent("""\
    You are a senior data reliability engineer running a freshness audit.

    You will receive:
    A) Metadata currently stored in DataHub (the catalog's view of freshness)
    B) Real query results from the actual database (ground truth)

    Your job:
    1. Compare DataHub's recorded status against real query evidence.
    2. Identify ALL issues — staleness, empty loads, data gaps, row count
       drops, mismatches between pipeline stages.
    3. For each issue produce:
       - severity: CRITICAL / WARNING / INFO
       - affected_table: table name
       - issue_type: one of [staleness, empty_load, data_gap,
         row_count_anomaly, stage_mismatch]
       - description: 1-2 sentence human-readable description
       - evidence: the specific numbers/dates that prove this
       - recommended_action: what to do about it
    4. Produce an overall pipeline health score: HEALTHY / DEGRADED / CRITICAL.

    Respond with ONLY a valid JSON object (no markdown, no code fences):
    {
      "pipeline_health": "CRITICAL",
      "summary": "one paragraph overall summary",
      "issues": [
        {
          "severity": "CRITICAL",
          "affected_table": "staging_trips",
          "issue_type": "staleness",
          "description": "...",
          "evidence": "...",
          "recommended_action": "..."
        }
      ]
    }
""")


def build_llm_context(datahub_meta, evidence):
    """Build a combined context string with DataHub metadata + real evidence."""
    sections = []

    sections.append("# SECTION A: DataHub Catalog Metadata")
    sections.append("(This is what DataHub currently believes about the pipeline)\n")
    for name in TABLE_NAMES:
        meta = datahub_meta.get(name)
        if not meta:
            sections.append(f"## {name}\nNot registered in DataHub.\n")
            continue
        sections.append(f"## {name}")
        sections.append(f"- DataHub status: {meta['datahub_freshness_status']}")
        sections.append(f"- DataHub staleness: {meta['datahub_staleness_days']} days")
        sections.append(f"- DataHub max_timestamp: {meta['datahub_max_timestamp']}")
        sections.append(f"- DataHub row_count: {meta['datahub_row_count']}")
        sections.append(f"- Tags: {meta['tags']}")
        sections.append(f"- Description: {meta['description'][:200]}")
        sections.append(f"- Upstream: {meta['upstream_urns']}")
        sections.append(f"- Downstream: {meta['downstream_urns']}")
        sections.append("")

    sections.append("\n# SECTION B: Real Database Query Results")
    sections.append("(Ground truth from querying the actual SQLite database)\n")

    for name in TABLE_NAMES:
        ev = evidence[name]
        sections.append(f"## {name}")
        sections.append(f"- Total rows: {ev['total_rows']:,}")
        sections.append(f"- Data range: {ev['min_timestamp']} → {ev['max_timestamp']}")
        sections.append(f"- Days with data: {ev['num_days_with_data']}")

        # Show last 10 daily counts
        recent = sorted(ev["daily_counts"], key=lambda x: x["date"], reverse=True)[:10]
        sections.append("- Recent daily counts (newest first):")
        for d in recent:
            flag = " ← ANOMALY" if d["count"] < 10 else ""
            sections.append(f"    {d['date']}: {d['count']:,} rows{flag}")

        if ev["anomalies"]:
            sections.append(f"- Detected anomalies ({len(ev['anomalies'])}):")
            for a in ev["anomalies"]:
                sections.append(f"    [{a['type']}] {a['message']}")
        sections.append("")

    cross = evidence["_cross_stage"]
    sections.append("## Cross-stage comparison")
    sections.append(f"- raw → staging row drop: {cross['raw_to_staging_drop']:,} "
                    f"({cross['raw_to_staging_drop_pct']}%)")
    sections.append(f"- staging days: {cross['staging_days']}  "
                    f"mart days: {cross['mart_days']}")
    if cross["dates_missing_in_mart"]:
        sections.append(f"- Dates in staging but missing from mart: "
                        f"{cross['dates_missing_in_mart']}")

    return "\n".join(sections)


def build_fallback_diagnosis(evidence):
    """Build a rule-based diagnosis when the LLM is unavailable."""
    issues = []

    # Check staleness: staging is behind raw
    raw_max = evidence["raw_trips"]["max_timestamp"]
    stg_max = evidence["staging_trips"]["max_timestamp"]
    mart_max = evidence["mart_daily_summary"]["max_timestamp"]

    fmt = lambda ts: "%Y-%m-%d %H:%M:%S" if " " in ts else "%Y-%m-%d"
    raw_dt = datetime.strptime(raw_max, fmt(raw_max))
    stg_dt = datetime.strptime(stg_max, fmt(stg_max))
    mart_dt = datetime.strptime(mart_max, fmt(mart_max))

    stg_gap = (raw_dt - stg_dt).days
    mart_gap = (raw_dt - mart_dt).days

    if stg_gap > 1:
        issues.append({
            "severity": "CRITICAL",
            "affected_table": "staging_trips",
            "issue_type": "staleness",
            "description": (
                f"staging_trips is {stg_gap} days behind raw_trips. "
                f"The staging pipeline has stopped processing new data."
            ),
            "evidence": (
                f"raw_trips max={raw_max}, staging_trips max={stg_max}, "
                f"gap={stg_gap} days"
            ),
            "recommended_action": (
                "Investigate the raw_trips → staging_trips ETL job. "
                "Check for failed runs, schema changes, or resource issues."
            ),
        })

    if mart_gap > 1:
        issues.append({
            "severity": "CRITICAL",
            "affected_table": "mart_daily_summary",
            "issue_type": "staleness",
            "description": (
                f"mart_daily_summary is {mart_gap} days behind source. "
                f"Downstream consumers are reading stale aggregations."
            ),
            "evidence": (
                f"raw_trips max={raw_max}, mart max={mart_max}, "
                f"gap={mart_gap} days"
            ),
            "recommended_action": (
                "This is caused by upstream staging staleness. Fix "
                "staging_trips first, then backfill mart_daily_summary."
            ),
        })

    # Check for empty/near-empty loads
    for name in TABLE_NAMES:
        for a in evidence[name]["anomalies"]:
            if a["type"] == "empty_load":
                issues.append({
                    "severity": "WARNING",
                    "affected_table": name,
                    "issue_type": "empty_load",
                    "description": (
                        f"Near-empty load detected: only {a['count']} "
                        f"rows on {a['date']}."
                    ),
                    "evidence": f"{a['date']}: {a['count']} rows",
                    "recommended_action": (
                        "Verify whether this date had genuinely low "
                        "volume or if the load was truncated/failed."
                    ),
                })

    # Check for large date gaps
    for name in TABLE_NAMES:
        large_gaps = [
            a for a in evidence[name]["anomalies"]
            if a["type"] == "date_gap" and a["gap_days"] > 30
        ]
        if large_gaps:
            gap = large_gaps[0]
            issues.append({
                "severity": "WARNING",
                "affected_table": name,
                "issue_type": "data_gap",
                "description": (
                    f"Large {gap['gap_days']}-day gap in data from "
                    f"{gap['from_date']} to {gap['to_date']}."
                ),
                "evidence": (
                    f"{gap['gap_days']} consecutive days with no data"
                ),
                "recommended_action": (
                    "Investigate whether source data exists for this "
                    "period and was not ingested."
                ),
            })

    # Cross-stage row drop
    cross = evidence["_cross_stage"]
    if cross["raw_to_staging_drop_pct"] > 10:
        issues.append({
            "severity": "INFO",
            "affected_table": "staging_trips",
            "issue_type": "stage_mismatch",
            "description": (
                f"{cross['raw_to_staging_drop_pct']}% row drop from "
                f"raw to staging ({cross['raw_to_staging_drop']:,} rows "
                f"filtered)."
            ),
            "evidence": (
                f"raw={evidence['raw_trips']['total_rows']:,}, "
                f"staging={evidence['staging_trips']['total_rows']:,}"
            ),
            "recommended_action": (
                "Review staging filter criteria. A 16% drop may be "
                "expected (bad data filtering) but should be monitored."
            ),
        })

    health = "CRITICAL" if any(
        i["severity"] == "CRITICAL" for i in issues
    ) else "DEGRADED" if any(
        i["severity"] == "WARNING" for i in issues
    ) else "HEALTHY"

    return {
        "pipeline_health": health,
        "summary": (
            f"Pipeline audit found {len(issues)} issues. "
            f"staging_trips and mart_daily_summary are {stg_gap} days "
            f"behind raw_trips, indicating a stalled ETL pipeline. "
            f"Multiple date gaps and near-empty loads detected across "
            f"all stages."
        ),
        "issues": issues,
    }


MAX_LLM_RETRIES = 3

RETRY_SUFFIX = (
    "\n\nIMPORTANT: You MUST respond with valid JSON only. "
    "No markdown, no backticks, no explanation outside the JSON. "
    "Do not wrap the JSON in ```json``` fences. "
    "Start your response with { and end with }."
)


def _extract_json(raw_text):
    """Extract a JSON object from raw LLM text.

    Handles: <think> tags, markdown fences, leading/trailing prose.
    Returns the parsed dict or raises ValueError.
    """
    # Strip <think>...</think> blocks (DeepSeek-R1 reasoning traces)
    cleaned = re.sub(r"<think>.*?</think>", "", raw_text, flags=re.DOTALL)
    cleaned = cleaned.strip()

    # Strip markdown code fences (```json ... ``` or ``` ... ```)
    fence_match = re.search(
        r"```(?:json)?\s*\n?(.*?)\n?\s*```", cleaned, flags=re.DOTALL
    )
    if fence_match:
        cleaned = fence_match.group(1).strip()

    # Try direct parse first
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Find the outermost { ... } by matching braces
    start = cleaned.find("{")
    if start == -1:
        raise ValueError("No JSON object found in response")

    depth = 0
    end = -1
    for i in range(start, len(cleaned)):
        if cleaned[i] == "{":
            depth += 1
        elif cleaned[i] == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end == -1:
        raise ValueError("Unbalanced braces in response")

    return json.loads(cleaned[start:end])


def call_nebius_diagnosis(context, evidence):
    """Send findings to Nebius LLM for structured diagnosis.

    Retry up to 3 times with increasingly strict prompts.
    Falls back to rule-based diagnosis if the LLM is unavailable or
    all retries fail.
    """
    api_key = os.environ.get("NEBIUS_API_KEY")
    if not api_key:
        warn("NEBIUS_API_KEY not set — using rule-based diagnosis")
        return build_fallback_diagnosis(evidence)

    client = OpenAI(base_url=NEBIUS_BASE_URL, api_key=api_key)

    user_message = (
        "Run a freshness audit on this pipeline. Compare the "
        "DataHub catalog metadata against the real query results "
        "and produce a structured diagnosis.\n\n" + context
    )

    for attempt in range(1, MAX_LLM_RETRIES + 1):
        try:
            prompt = DIAGNOSIS_SYSTEM_PROMPT
            if attempt > 1:
                prompt += RETRY_SUFFIX
                warn(f"Retry {attempt}/{MAX_LLM_RETRIES} with stricter prompt")

            info(f"Sending findings to Nebius LLM (attempt {attempt}/{MAX_LLM_RETRIES})…")
            response = client.chat.completions.create(
                model=NEBIUS_MODEL,
                temperature=max(0.1, 0.3 - (attempt - 1) * 0.1),
                max_tokens=4096,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user_message},
                ],
            )

            raw_text = response.choices[0].message.content or ""
            result = _extract_json(raw_text)

            # Validate required keys
            if "pipeline_health" not in result or "issues" not in result:
                raise ValueError("Missing required keys: pipeline_health, issues")
            if not isinstance(result["issues"], list):
                raise ValueError("'issues' must be a list")

            ok(f"LLM response parsed successfully (attempt {attempt})")
            return result

        except (json.JSONDecodeError, ValueError) as exc:
            warn(f"Attempt {attempt} failed: {exc}")
            if attempt < MAX_LLM_RETRIES:
                info("Retrying with stricter prompt…")
            continue
        except Exception as exc:
            fail(f"LLM API error: {exc}")
            break

    warn("All LLM attempts failed — falling back to rule-based diagnosis")
    return build_fallback_diagnosis(evidence)


# ---------------------------------------------------------------------------
# 4. Actions — Update DataHub + Alert
# ---------------------------------------------------------------------------
def update_datahub_from_diagnosis(emitter, diagnosis, evidence):
    """Update DataHub tags and descriptions based on LLM diagnosis."""
    issues_by_table = {}
    for issue in diagnosis.get("issues", []):
        table = issue.get("affected_table", "unknown")
        issues_by_table.setdefault(table, []).append(issue)

    updated = []
    for name in TABLE_NAMES:
        table_issues = issues_by_table.get(name, [])
        ev = evidence[name]

        # Determine the most severe issue for this table
        severities = [i["severity"] for i in table_issues]
        if "CRITICAL" in severities:
            new_tag = "freshness:critical_stale"
            new_status = "CRITICAL"
        elif "WARNING" in severities:
            new_tag = "freshness:critical_stale"
            new_status = "WARNING"
        else:
            new_tag = "freshness:ok"
            new_status = "OK"

        # Build incident description
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_issues:
            issue_lines = []
            for i, issue in enumerate(table_issues, 1):
                issue_lines.append(
                    f"{i}. **[{issue['severity']}]** {issue['description']}"
                )
            incident_text = "\n".join(issue_lines)
            description = (
                f"**Freshness Audit ({now})**\n\n"
                f"Pipeline health: **{diagnosis['pipeline_health']}**\n\n"
                f"Issues found:\n{incident_text}\n\n"
                f"Last data: {ev['max_timestamp']} | "
                f"Rows: {ev['total_rows']:,}"
            )
        else:
            description = (
                f"**Freshness Audit ({now})**\n\n"
                f"No issues found. Data is current through "
                f"{ev['max_timestamp']}. Rows: {ev['total_rows']:,}"
            )

        # Emit updated tag
        mcp_tag = MetadataChangeProposalWrapper(
            entityUrn=make_urn(name),
            aspect=GlobalTagsClass(
                tags=[TagAssociationClass(tag=make_tag_urn(new_tag))],
            ),
        )
        emitter.emit(mcp_tag)

        # Emit updated properties with incident report
        custom_props = {
            "row_count": str(ev["total_rows"]),
            "timestamp_column": TIMESTAMP_COLUMNS[name],
            "max_timestamp": str(ev["max_timestamp"]),
            "min_timestamp": str(ev["min_timestamp"]),
            "freshness_status": new_status,
            "last_audit": now,
            "anomaly_count": str(len(ev["anomalies"])),
            "pipeline_health": diagnosis["pipeline_health"],
        }
        mcp_props = MetadataChangeProposalWrapper(
            entityUrn=make_urn(name),
            aspect=DatasetPropertiesClass(
                name=name,
                description=description,
                customProperties=custom_props,
            ),
        )
        emitter.emit(mcp_props)

        updated.append((name, new_tag, new_status, len(table_issues)))

    return updated


def print_alerts(diagnosis, evidence):
    """Print Slack-style alert messages for each issue."""
    health = diagnosis["pipeline_health"]
    if health == "CRITICAL":
        health_color = C.BG_RED
    elif health == "DEGRADED":
        health_color = C.BG_YELLOW
    else:
        health_color = C.BG_GREEN

    header("ALERTS → #data_platform_team", C.RED)

    # Overall alert
    print(f"""
  {health_color}{C.WHITE}{C.BOLD} {health} {C.RESET}  NYC Taxi Pipeline Freshness Alert
  {C.DIM}Channel: #data_platform_team  |  Owner: @{ALERT_OWNERS}{C.RESET}

  {C.BOLD}Summary:{C.RESET} {diagnosis['summary']}
""")

    issues = diagnosis.get("issues", [])
    if not issues:
        ok("No issues to alert on.")
        return

    # Group by severity
    for sev, color in [("CRITICAL", C.RED), ("WARNING", C.YELLOW), ("INFO", C.CYAN)]:
        sev_issues = [i for i in issues if i["severity"] == sev]
        if not sev_issues:
            continue

        print(f"  {color}{C.BOLD}{'─' * 56}{C.RESET}")
        print(f"  {color}{C.BOLD}{sev} ({len(sev_issues)} issue{'s' if len(sev_issues) != 1 else ''}){C.RESET}")
        print(f"  {color}{C.BOLD}{'─' * 56}{C.RESET}")

        for issue in sev_issues:
            print(f"""
  {color}●{C.RESET} {C.BOLD}{issue['issue_type']}{C.RESET} on {C.BOLD}{issue['affected_table']}{C.RESET}
    {issue['description']}
    {C.DIM}Evidence:{C.RESET} {issue['evidence']}
    {C.DIM}Action:{C.RESET}   {issue['recommended_action']}""")

    print()


# ---------------------------------------------------------------------------
# 5. Report
# ---------------------------------------------------------------------------
def save_audit_report(datahub_meta, evidence, diagnosis, actions):
    """Save a full markdown audit report."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUTPUT_DIR / f"freshness_audit_{ts}.md"

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    health = diagnosis["pipeline_health"]

    lines = [
        "# Freshness Audit Report",
        f"*Generated: {now_str}*",
        "",
        f"**Pipeline Health: {health}**",
        "",
        "---",
        "",
        "## Executive Summary",
        "",
        diagnosis["summary"],
        "",
        "---",
        "",
        "## Issues Found",
        "",
    ]

    issues = diagnosis.get("issues", [])
    if not issues:
        lines.append("No issues detected.")
    else:
        lines.append(f"| # | Severity | Table | Type | Description |")
        lines.append("|---|----------|-------|------|-------------|")
        for i, issue in enumerate(issues, 1):
            lines.append(
                f"| {i} | {issue['severity']} | "
                f"`{issue['affected_table']}` | "
                f"{issue['issue_type']} | "
                f"{issue['description']} |"
            )

    lines += [
        "",
        "---",
        "",
        "## Issue Details",
        "",
    ]
    for i, issue in enumerate(issues, 1):
        lines += [
            f"### {i}. [{issue['severity']}] {issue['issue_type']} — "
            f"`{issue['affected_table']}`",
            "",
            issue["description"],
            "",
            f"**Evidence:** {issue['evidence']}",
            "",
            f"**Recommended action:** {issue['recommended_action']}",
            "",
        ]

    lines += [
        "---",
        "",
        "## DataHub Actions Taken",
        "",
        "| Table | New Tag | Status | Issues |",
        "|-------|---------|--------|--------|",
    ]
    for name, tag, status, count in actions:
        lines.append(f"| `{name}` | `{tag}` | {status} | {count} |")

    lines += [
        "",
        "---",
        "",
        "## Real Data Evidence",
        "",
    ]
    for name in TABLE_NAMES:
        ev = evidence[name]
        lines += [
            f"### {name}",
            "",
            f"- **Rows:** {ev['total_rows']:,}",
            f"- **Range:** {ev['min_timestamp']} → {ev['max_timestamp']}",
            f"- **Days with data:** {ev['num_days_with_data']}",
            f"- **Anomalies:** {len(ev['anomalies'])}",
            "",
        ]
        if ev["anomalies"]:
            for a in ev["anomalies"]:
                lines.append(f"  - [{a['type']}] {a['message']}")
            lines.append("")

    cross = evidence["_cross_stage"]
    lines += [
        "### Cross-stage",
        "",
        f"- raw → staging drop: {cross['raw_to_staging_drop']:,} rows "
        f"({cross['raw_to_staging_drop_pct']}%)",
        f"- Staging days: {cross['staging_days']} | "
        f"Mart days: {cross['mart_days']}",
    ]
    if cross["dates_missing_in_mart"]:
        lines.append(f"- Missing from mart: {cross['dates_missing_in_mart']}")

    lines += [
        "",
        "---",
        "",
        "<details><summary>DataHub catalog metadata at time of audit</summary>",
        "",
        "```json",
        json.dumps(
            {k: v for k, v in datahub_meta.items() if v},
            indent=2,
            default=str,
        ),
        "```",
        "",
        "</details>",
    ]

    path.write_text("\n".join(lines))
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    start_time = time.time()

    header("LEVEL 3 — FRESHNESS AUDIT AGENT", C.MAGENTA)
    print(f"  {C.DIM}DataHub x Nebius Hackathon • Problem 3{C.RESET}")
    print(f"  {C.DIM}Agent run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{C.RESET}")

    # ── Step 1: Read from DataHub ──────────────────────────────────────
    header("STEP 1 — Read Pipeline Metadata from DataHub")
    server, token = load_datahub_config()
    ok(f"DataHub GMS: {server}")

    datahub_meta = fetch_datahub_metadata(server, token)
    found = sum(1 for v in datahub_meta.values() if v)
    ok(f"Loaded {found}/{len(TABLE_NAMES)} datasets from DataHub catalog")

    # ── Step 2: Query real data ────────────────────────────────────────
    header("STEP 2 — Query Real Data for Evidence")
    if not DB_PATH.exists():
        fail(f"Database not found: {DB_PATH}")
        sys.exit(1)
    conn = sqlite3.connect(str(DB_PATH))
    ok(f"Connected to {DB_PATH.name}")

    evidence = query_real_data(conn)
    conn.close()

    total_anomalies = sum(len(evidence[n]["anomalies"]) for n in TABLE_NAMES)
    if total_anomalies:
        warn(f"Found {total_anomalies} anomalies across all tables")
    else:
        ok("No anomalies detected in raw data")

    # ── Step 3: LLM Diagnosis ─────────────────────────────────────────
    header("STEP 3 — LLM Diagnosis (Nebius DeepSeek-R1)")
    context = build_llm_context(datahub_meta, evidence)
    info(f"Context size: {len(context):,} chars")

    diagnosis = call_nebius_diagnosis(context, evidence)
    health = diagnosis["pipeline_health"]
    num_issues = len(diagnosis.get("issues", []))

    if health == "CRITICAL":
        fail(f"Pipeline health: {badge('CRITICAL', C.RED)}  "
             f"({num_issues} issues)")
    elif health == "DEGRADED":
        warn(f"Pipeline health: {badge('DEGRADED', C.YELLOW)}  "
             f"({num_issues} issues)")
    else:
        ok(f"Pipeline health: {badge('HEALTHY', C.GREEN)}  "
           f"({num_issues} issues)")

    print(f"\n  {C.DIM}{diagnosis['summary']}{C.RESET}")

    # ── Step 4: Act — Update DataHub + Alert ──────────────────────────
    header("STEP 4 — Actions")

    subheader("Updating DataHub tags & descriptions")
    emitter = DatahubRestEmitter(gms_server=server, token=token)
    actions = update_datahub_from_diagnosis(emitter, diagnosis, evidence)
    for name, tag, status, count in actions:
        if status in ("CRITICAL", "WARNING"):
            warn(f"{name} → {badge(status, C.RED)}  tag={tag}  "
                 f"({count} issues)")
        else:
            ok(f"{name} → {badge(status, C.GREEN)}  tag={tag}")

    # Alerts
    print_alerts(diagnosis, evidence)

    # ── Step 5: Save report ───────────────────────────────────────────
    header("STEP 5 — Audit Report")
    report_path = save_audit_report(datahub_meta, evidence, diagnosis, actions)
    ok(f"Saved to {report_path}")

    elapsed = time.time() - start_time
    header("AUDIT COMPLETE", C.GREEN)
    print(f"  {C.DIM}Duration: {elapsed:.1f}s{C.RESET}")
    print(f"  {C.DIM}Pipeline: {badge(health, C.RED if health == 'CRITICAL' else C.YELLOW if health == 'DEGRADED' else C.GREEN)}{C.RESET}")
    print(f"  {C.DIM}Issues: {num_issues}  |  Anomalies: {total_anomalies}  "
          f"|  Tables updated: {len(actions)}{C.RESET}")
    print(f"  {C.DIM}Report: {report_path}{C.RESET}")
    print()


if __name__ == "__main__":
    main()
