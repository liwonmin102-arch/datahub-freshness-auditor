#!/usr/bin/env python3
"""
Level 2 — Write Freshness Status Back to DataHub
DataHub x Nebius Hackathon (Problem 3)

Registers pipeline tables as DataHub datasets, writes schema metadata,
lineage, freshness tags, and descriptions — then verifies via GraphQL.
"""

import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
import yaml

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TagAssociationClass,
    TagPropertiesClass,
    UpstreamClass,
    UpstreamLineageClass,
)

# ---------------------------------------------------------------------------
# Config (shared with Level 1)
# ---------------------------------------------------------------------------
DB_PATH = Path.home() / "static-assets" / "datasets" / "nyc-taxi" / "nyc_taxi_pipeline.db"
DATAHUB_ENV_PATH = Path.home() / ".datahubenv"
PLATFORM = "sqlite"
PLATFORM_INSTANCE = "nyc_taxi_pipeline"
ENV = "PROD"

PIPELINE_GRAPH = {
    "raw_trips": {"downstream": ["staging_trips"], "sla_hours": 24},
    "staging_trips": {"downstream": ["mart_daily_summary"], "sla_hours": 24},
    "mart_daily_summary": {"downstream": [], "sla_hours": 24},
}

TIMESTAMP_COLUMNS = {
    "raw_trips": "tpep_pickup_datetime",
    "staging_trips": "tpep_pickup_datetime",
    "mart_daily_summary": "trip_date",
}

# SQLite type → DataHub field type mapping
SQLITE_TYPE_MAP = {
    "TEXT": StringTypeClass,
    "INTEGER": NumberTypeClass,
    "REAL": NumberTypeClass,
    "": StringTypeClass,  # untyped columns default to string
}


# ---------------------------------------------------------------------------
# DataHub connection
# ---------------------------------------------------------------------------
def load_datahub_config() -> tuple[str, str]:
    """Read GMS server URL and token from ~/.datahubenv."""
    if not DATAHUB_ENV_PATH.exists():
        print(f"ERROR: {DATAHUB_ENV_PATH} not found", file=sys.stderr)
        sys.exit(1)
    config = yaml.safe_load(DATAHUB_ENV_PATH.read_text())
    server = config["gms"]["server"]
    token = config["gms"]["token"]
    return server, token


def make_urn(table_name: str) -> str:
    return make_dataset_urn_with_platform_instance(
        platform=PLATFORM,
        name=table_name,
        platform_instance=PLATFORM_INSTANCE,
        env=ENV,
    )


# ---------------------------------------------------------------------------
# Database introspection (reused from Level 1)
# ---------------------------------------------------------------------------
def get_table_metadata(conn: sqlite3.Connection) -> list[dict]:
    """Return schema info, row counts, and freshness timestamps for every table."""
    tables = []
    cursor = conn.cursor()

    for table_name, ts_col in TIMESTAMP_COLUMNS.items():
        cols = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
        columns = [{"name": c[1], "type": c[2]} for c in cols]

        row_count = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        max_ts = cursor.execute(f"SELECT MAX({ts_col}) FROM {table_name}").fetchone()[0]
        min_ts = cursor.execute(f"SELECT MIN({ts_col}) FROM {table_name}").fetchone()[0]

        tables.append({
            "table": table_name,
            "columns": columns,
            "row_count": row_count,
            "timestamp_column": ts_col,
            "max_timestamp": max_ts,
            "min_timestamp": min_ts,
            "pipeline": PIPELINE_GRAPH[table_name],
        })

    return tables


def compute_freshness(metadata: list[dict]) -> dict[str, dict]:
    """Compute staleness for each table relative to its upstream.

    A table is stale if:
      - It is directly behind its upstream (gap > 1 day), OR
      - Its upstream is itself stale (staleness propagates downstream).
    """
    max_timestamps = {t["table"]: t["max_timestamp"] for t in metadata}
    meta_by_name = {t["table"]: t for t in metadata}
    result = {}

    # Process in pipeline order (source first) so upstream results are ready
    ordered = ["raw_trips", "staging_trips", "mart_daily_summary"]

    for name in ordered:
        t = meta_by_name[name]
        upstream_tables = [
            k for k, v in PIPELINE_GRAPH.items() if name in v["downstream"]
        ]

        if upstream_tables:
            upstream_name = upstream_tables[0]
            upstream_ts = max_timestamps[upstream_name]
            own_ts = t["max_timestamp"]
            # Parse timestamps — handle both "YYYY-MM-DD HH:MM:SS" and "YYYY-MM-DD"
            fmt_up = "%Y-%m-%d %H:%M:%S" if " " in upstream_ts else "%Y-%m-%d"
            fmt_own = "%Y-%m-%d %H:%M:%S" if " " in own_ts else "%Y-%m-%d"
            delta = datetime.strptime(upstream_ts, fmt_up) - datetime.strptime(own_ts, fmt_own)
            gap_days = delta.days

            # Stale if directly behind upstream OR if upstream is already stale
            upstream_stale = result.get(upstream_name, {}).get("is_stale", False)
            is_stale = gap_days > 1 or upstream_stale

            # For inherited staleness, report gap relative to the source of truth
            source_ts = max_timestamps["raw_trips"]
            fmt_src = "%Y-%m-%d %H:%M:%S" if " " in source_ts else "%Y-%m-%d"
            source_gap = (datetime.strptime(source_ts, fmt_src) - datetime.strptime(own_ts, fmt_own)).days

            result[name] = {
                "is_stale": is_stale,
                "gap_days": gap_days,
                "source_gap_days": source_gap,
                "upstream": upstream_name,
                "upstream_ts": upstream_ts,
                "own_ts": own_ts,
                "tag": "freshness:critical_stale" if is_stale else "freshness:ok",
            }
        else:
            # Source table — no upstream to compare against
            result[name] = {
                "is_stale": False,
                "gap_days": 0,
                "source_gap_days": 0,
                "upstream": None,
                "upstream_ts": None,
                "own_ts": t["max_timestamp"],
                "tag": "freshness:ok",
            }

    return result


# ---------------------------------------------------------------------------
# Emit metadata to DataHub
# ---------------------------------------------------------------------------
def emit_tag_definitions(emitter: DatahubRestEmitter):
    """Create tag entities so they display nicely in the UI."""
    tags = {
        "freshness:ok": ("Data is fresh and within SLA", "#4CAF50"),
        "freshness:critical_stale": ("Data is critically stale — behind upstream SLA", "#F44336"),
    }
    for tag_name, (description, _color) in tags.items():
        mcp = MetadataChangeProposalWrapper(
            entityUrn=make_tag_urn(tag_name),
            aspect=TagPropertiesClass(
                name=tag_name,
                description=description,
            ),
        )
        emitter.emit(mcp)
    print("   Tags defined: freshness:ok, freshness:critical_stale")


def emit_dataset_properties(
    emitter: DatahubRestEmitter,
    table: dict,
    freshness: dict,
):
    """Register dataset with properties and freshness description."""
    name = table["table"]
    urn = make_urn(name)
    f = freshness[name]

    if f["is_stale"]:
        if f["gap_days"] > 1:
            description = (
                f"**STALE**: {f['gap_days']} days behind upstream "
                f"`{f['upstream']}`. Last data: {f['own_ts']}. "
                f"Upstream has data through: {f['upstream_ts']}."
            )
        else:
            description = (
                f"**STALE** (inherited): In sync with upstream "
                f"`{f['upstream']}`, but {f['source_gap_days']} days "
                f"behind source. Last data: {f['own_ts']}."
            )
    else:
        description = (
            f"**FRESH**: Data is current through {f['own_ts']}. Within SLA."
        )

    custom_props = {
        "row_count": str(table["row_count"]),
        "timestamp_column": table["timestamp_column"],
        "max_timestamp": table["max_timestamp"],
        "min_timestamp": table["min_timestamp"],
        "sla_hours": str(table["pipeline"]["sla_hours"]),
        "freshness_status": "STALE" if f["is_stale"] else "OK",
        "staleness_days": str(f["gap_days"]),
        "pipeline_stage": (
            "source" if not f["upstream"]
            else "intermediate" if table["pipeline"]["downstream"]
            else "terminal"
        ),
    }

    mcp = MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=DatasetPropertiesClass(
            name=name,
            description=description,
            customProperties=custom_props,
        ),
    )
    emitter.emit(mcp)


def emit_schema(emitter: DatahubRestEmitter, table: dict):
    """Emit schema metadata for a dataset."""
    urn = make_urn(table["table"])
    fields = []
    for col in table["columns"]:
        type_class = SQLITE_TYPE_MAP.get(col["type"], StringTypeClass)
        fields.append(
            SchemaFieldClass(
                fieldPath=col["name"],
                type=SchemaFieldDataTypeClass(type=type_class()),
                nativeDataType=col["type"] or "TEXT",
                description=f"{'Freshness timestamp column' if col['name'] == table['timestamp_column'] else ''}",
            )
        )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=SchemaMetadataClass(
            schemaName=table["table"],
            platform=make_data_platform_urn(PLATFORM),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        ),
    )
    emitter.emit(mcp)


def emit_tags(emitter: DatahubRestEmitter, table: dict, freshness: dict):
    """Attach freshness tags to a dataset."""
    urn = make_urn(table["table"])
    tag_name = freshness[table["table"]]["tag"]

    mcp = MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=GlobalTagsClass(
            tags=[TagAssociationClass(tag=make_tag_urn(tag_name))],
        ),
    )
    emitter.emit(mcp)


def emit_lineage(emitter: DatahubRestEmitter):
    """Emit pipeline lineage: raw_trips → staging_trips → mart_daily_summary."""
    lineage_edges = [
        ("staging_trips", "raw_trips"),
        ("mart_daily_summary", "staging_trips"),
    ]
    for downstream, upstream in lineage_edges:
        mcp = MetadataChangeProposalWrapper(
            entityUrn=make_urn(downstream),
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=make_urn(upstream),
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                ]
            ),
        )
        emitter.emit(mcp)
    print("   Lineage: raw_trips → staging_trips → mart_daily_summary")


# ---------------------------------------------------------------------------
# Verify via GraphQL
# ---------------------------------------------------------------------------
GRAPHQL_QUERY = """
query getDataset($urn: String!) {
  dataset(urn: $urn) {
    urn
    name
    properties {
      name
      description
      customProperties {
        key
        value
      }
    }
    tags {
      tags {
        tag {
          urn
          properties {
            name
            description
          }
        }
      }
    }
    schemaMetadata {
      fields {
        fieldPath
        nativeDataType
      }
    }
  }
}
"""


def verify_datasets(server: str, token: str, table_names: list[str]):
    """Query DataHub GraphQL to verify metadata was written."""
    graphql_url = f"{server}/api/graphql"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    print()
    print("-" * 60)
    print("  VERIFICATION (GraphQL)")
    print("-" * 60)

    for name in table_names:
        urn = make_urn(name)
        resp = requests.post(
            graphql_url,
            headers=headers,
            json={"query": GRAPHQL_QUERY, "variables": {"urn": urn}},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            print(f"\n  {name}: GraphQL errors: {data['errors']}")
            continue

        ds = data.get("data", {}).get("dataset")
        if not ds:
            print(f"\n  {name}: NOT FOUND in DataHub")
            continue

        props = ds.get("properties") or {}
        tags = ds.get("tags", {}).get("tags", [])
        fields = (ds.get("schemaMetadata") or {}).get("fields", [])
        custom = {p["key"]: p["value"] for p in (props.get("customProperties") or [])}

        tag_names = [t["tag"]["properties"]["name"] for t in tags if t.get("tag", {}).get("properties")]

        print(f"\n  {name}")
        print(f"    Description : {(props.get('description') or '')[:80]}")
        print(f"    Tags        : {', '.join(tag_names) or '(none)'}")
        print(f"    Columns     : {len(fields)}")
        print(f"    Row count   : {custom.get('row_count', '?')}")
        print(f"    Freshness   : {custom.get('freshness_status', '?')} (gap: {custom.get('staleness_days', '?')} days)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("  Level 2 — Write Freshness Status to DataHub")
    print("  DataHub x Nebius Hackathon")
    print("=" * 60)
    print()

    # Step 1: Load DataHub config
    server, token = load_datahub_config()
    print(f"🔗 DataHub GMS: {server}")

    # Step 2: Connect to SQLite and extract metadata
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(str(DB_PATH))
    metadata = get_table_metadata(conn)
    conn.close()
    print(f"📊 Extracted metadata for {len(metadata)} tables")

    # Step 3: Compute freshness
    freshness = compute_freshness(metadata)
    for name, f in freshness.items():
        status = f"STALE ({f['gap_days']}d behind {f['upstream']})" if f["is_stale"] else "OK"
        print(f"   • {name}: {status}")
    print()

    # Step 4: Create emitter and write to DataHub
    emitter = DatahubRestEmitter(gms_server=server, token=token)
    emitter.test_connection()
    print("✅ Connected to DataHub GMS")
    print()

    print("📤 Emitting metadata to DataHub…")

    # 4a: Tag definitions
    emit_tag_definitions(emitter)

    # 4b: Dataset properties, schema, and tags for each table
    for table in metadata:
        name = table["table"]
        emit_dataset_properties(emitter, table, freshness)
        emit_schema(emitter, table)
        emit_tags(emitter, table, freshness)
        tag = freshness[name]["tag"]
        print(f"   {name}: properties + schema + tag [{tag}]")

    # 4c: Lineage
    emit_lineage(emitter)

    print()
    print("✅ All metadata emitted successfully")

    # Step 5: Verify via GraphQL
    time.sleep(2)  # brief pause to let GMS index
    table_names = [t["table"] for t in metadata]
    verify_datasets(server, token, table_names)

    print()
    print("=" * 60)
    print("  Done! Tables are now visible in DataHub UI at localhost:9002")
    print("  Search for 'nyc_taxi_pipeline' or 'freshness' to find them.")
    print("=" * 60)


if __name__ == "__main__":
    main()
