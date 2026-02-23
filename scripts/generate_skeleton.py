"""Generate a starter YAML skeleton for a new table from BQ schema.

Reads column names + types from either live BQ INFORMATION_SCHEMA or offline
JSON files, then generates a catalog YAML with placeholders for enrichment.

Usage:
    python scripts/generate_skeleton.py --layer data --table newtable --offline
    python scripts/generate_skeleton.py --layer kpi --table newtable --live
    python scripts/generate_skeleton.py --layer data --table newtable --offline --force
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATALOG_DIR = PROJECT_ROOT / "catalog"
SCHEMA_DIR = PROJECT_ROOT / "schemas"

# ---------------------------------------------------------------------------
# Dataset placeholder map
# ---------------------------------------------------------------------------

DATASET_PLACEHOLDERS = {
    "kpi": "{kpi_dataset}",
    "data": "{data_dataset}",
}


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------


def load_schema_offline(layer: str, table: str) -> list[dict[str, str]]:
    """Load column schema from committed JSON file.

    Returns list of {name, type} dicts.
    """
    path = SCHEMA_DIR / layer / f"{table}.json"
    if not path.exists():
        msg = f"No offline schema found: {path}"
        raise FileNotFoundError(msg)

    with open(path) as f:
        schema = json.load(f)

    return [{"name": col["name"], "type": col["type"]} for col in schema]


def load_schema_live(layer: str, table: str) -> list[dict[str, str]]:
    """Load column schema from live BQ INFORMATION_SCHEMA.

    Returns list of {name, type} dicts.
    """
    from nl2sql_agent.config import Settings

    s = Settings()
    dataset = s.kpi_dataset if layer == "kpi" else s.data_dataset

    from google.cloud import bigquery

    client = bigquery.Client(project=s.gcp_project, location=s.bq_location)
    sql = (
        f"SELECT column_name, data_type "
        f"FROM `{s.gcp_project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
        f"WHERE table_name = '{table}' "
        f"ORDER BY ordinal_position"
    )
    rows = client.query(sql).result()

    # Map BQ types to simplified types
    type_map = {
        "INT64": "INTEGER",
        "FLOAT64": "FLOAT",
        "BOOL": "BOOLEAN",
        "STRING": "STRING",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "NUMERIC": "NUMERIC",
        "BYTES": "BYTES",
    }

    return [
        {"name": row.column_name, "type": type_map.get(row.data_type, row.data_type)}
        for row in rows
    ]


# ---------------------------------------------------------------------------
# YAML generation
# ---------------------------------------------------------------------------


def generate_skeleton_yaml(
    layer: str, table: str, columns: list[dict[str, str]]
) -> str:
    """Generate YAML content for a table skeleton.

    Returns the YAML string (not written to disk).
    """
    dataset_placeholder = DATASET_PLACEHOLDERS[layer]
    fqn = f"{{project}}.{dataset_placeholder}.{table}"
    # Escape single quotes for YAML single-quoted string
    fqn_escaped = fqn.replace("'", "''")

    lines = [
        "table:",
        f"  name: {table}",
        f'  dataset: "{dataset_placeholder}"',
        f"  fqn: '{fqn_escaped}'",
        f"  layer: {layer}",
        '  description: ""',
        "  partition_field: trade_date",
        "  columns:" if columns else "  columns: []",
    ]

    for col in columns:
        lines.append(f"    - name: {col['name']}")
        lines.append(f"      type: {col['type']}")
        lines.append('      description: ""')

    return "\n".join(lines) + "\n"


def write_skeleton(
    layer: str, table: str, columns: list[dict[str, str]], *, force: bool = False
) -> Path:
    """Write skeleton YAML to catalog directory.

    Returns path of the written file.
    Raises FileExistsError if file exists and force is False.
    """
    output_path = CATALOG_DIR / layer / f"{table}.yaml"

    if output_path.exists() and not force:
        msg = f"YAML already exists: {output_path}. Use --force to overwrite."
        raise FileExistsError(msg)

    content = generate_skeleton_yaml(layer, table, columns)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content)
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate skeleton YAML for a new table from BQ schema"
    )
    parser.add_argument("--layer", required=True, choices=["kpi", "data"])
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--live", action="store_true", help="Query live BQ")
    parser.add_argument(
        "--offline", action="store_true", help="Use schemas/*.json (default)"
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing YAML")
    args = parser.parse_args()

    if not args.live and not args.offline:
        args.offline = True

    try:
        if args.live:
            columns = load_schema_live(args.layer, args.table)
        else:
            columns = load_schema_offline(args.layer, args.table)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    if not columns:
        print(f"ERROR: No columns found for {args.layer}/{args.table}", file=sys.stderr)
        return 1

    try:
        path = write_skeleton(args.layer, args.table, columns, force=args.force)
    except FileExistsError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    print(f"Generated {path} ({len(columns)} columns)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
