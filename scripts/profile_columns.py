"""BQ data profiling to populate example_values in the YAML catalog.

Queries BigQuery APPROX_COUNT_DISTINCT and APPROX_TOP_COUNT for each
dimension/identifier STRING column, then updates YAML files with
example_values and comprehensive flags based on cardinality tiers.

Usage:
    python scripts/profile_columns.py [--dry-run] [--trade-date 2026-02-17]
"""

from __future__ import annotations

import argparse
import enum
import re
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATALOG_DIR = PROJECT_ROOT / "catalog"

from table_registry import ALL_TABLES, filter_tables

# Schema constraint
MAX_EXAMPLE_VALUES = 25

# ---------------------------------------------------------------------------
# Cardinality tiers
# ---------------------------------------------------------------------------


class Cardinality(enum.Enum):
    COMPREHENSIVE = "comprehensive"  # <25: store all
    ILLUSTRATIVE = "illustrative"  # 25-250: store top 10
    SKIP = "skip"  # 250+: skip


def classify_cardinality(n: int) -> Cardinality:
    """Classify a distinct count into a cardinality tier."""
    if n < 1:
        return Cardinality.SKIP
    if n < 25:
        return Cardinality.COMPREHENSIVE
    if n <= 250:
        return Cardinality.ILLUSTRATIVE
    return Cardinality.SKIP


# ---------------------------------------------------------------------------
# Column selection
# ---------------------------------------------------------------------------


def get_columns_to_profile(data: dict) -> list[dict]:
    """Return dim/id columns that need example_values populated.

    Filters to columns that:
    - Have category dimension or identifier
    - Don't already have example_values
    - Are STRING, INTEGER, or BOOLEAN type (profilable)
    """
    profilable_types = {"STRING", "INTEGER", "BOOLEAN", "INT64", "BOOL"}
    result = []
    for col in data.get("table", {}).get("columns", []):
        if col.get("category") not in ("dimension", "identifier"):
            continue
        if col.get("example_values") is not None:
            continue
        if col.get("type", "").upper() not in profilable_types:
            continue
        result.append(col)
    return result


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------


def build_profiling_sql(
    *,
    project: str,
    dataset: str,
    table: str,
    columns: list[str],
    trade_date: str,
) -> str | None:
    """Build a single SQL query that profiles multiple columns at once.

    Returns None if columns list is empty.

    Uses UNION ALL of per-column subqueries, each computing:
    - APPROX_COUNT_DISTINCT for cardinality
    - APPROX_TOP_COUNT for top values (up to 25 for comprehensive tier)
    """
    if not columns:
        return None

    fqn = f"`{project}.{dataset}.{table}`"
    parts = []

    for col_name in columns:
        part = f"""SELECT
  '{col_name}' AS column_name,
  APPROX_COUNT_DISTINCT(CAST(`{col_name}` AS STRING)) AS approx_distinct,
  APPROX_TOP_COUNT(CAST(`{col_name}` AS STRING), 25) AS top_values
FROM {fqn}
WHERE trade_date = '{trade_date}' AND `{col_name}` IS NOT NULL"""
        parts.append(part)

    return "\nUNION ALL\n".join(parts)


# ---------------------------------------------------------------------------
# Result transformation
# ---------------------------------------------------------------------------


def transform_profiling_results(rows: list[dict]) -> dict[str, dict]:
    """Transform BQ profiling result rows into example_values mapping.

    Returns dict mapping column_name → {example_values, comprehensive}.
    Columns with cardinality > 250 are excluded (SKIP tier).
    """
    result: dict[str, dict] = {}

    for row in rows:
        col_name = row["column_name"]
        distinct = row["approx_distinct"]
        top_values = row.get("top_values", [])

        tier = classify_cardinality(distinct)
        if tier == Cardinality.SKIP:
            continue

        # Extract values, filter nulls, sort by count (desc)
        sorted_vals = sorted(top_values, key=lambda x: x.get("count", 0), reverse=True)
        values = [v["value"] for v in sorted_vals if v.get("value") is not None]

        if tier == Cardinality.COMPREHENSIVE:
            # Store all values (up to MAX_EXAMPLE_VALUES)
            example_values = values[:MAX_EXAMPLE_VALUES]
            comprehensive = True
        else:
            # ILLUSTRATIVE: store top 10
            example_values = values[:10]
            comprehensive = False

        if example_values:
            result[col_name] = {
                "example_values": example_values,
                "comprehensive": comprehensive,
            }

    return result


# ---------------------------------------------------------------------------
# Table enrichment
# ---------------------------------------------------------------------------


def enrich_table_example_values(
    data: dict,
    profiling: dict[str, dict],
    *,
    return_stats: bool = False,
) -> dict | tuple[dict, dict]:
    """Apply profiling results to a table's columns."""
    stats = {"assigned": 0, "preserved": 0, "skipped": 0}
    columns = data.get("table", {}).get("columns", [])

    for col in columns:
        col_name = col["name"]

        # Skip if already has example_values
        if col.get("example_values") is not None:
            stats["preserved"] += 1
            continue

        # Only apply to dim/id columns
        if col.get("category") not in ("dimension", "identifier"):
            continue

        if col_name in profiling:
            info = profiling[col_name]
            col["example_values"] = info["example_values"]
            col["comprehensive"] = info["comprehensive"]
            stats["assigned"] += 1
        else:
            stats["skipped"] += 1

    if return_stats:
        return data, stats
    return data


# ---------------------------------------------------------------------------
# Surgical YAML editing
# ---------------------------------------------------------------------------


def _build_example_changes(
    yaml_path: Path,
    profiling: dict[str, dict],
) -> tuple[dict[str, dict], dict]:
    """Determine example_values changes without modifying the file."""
    data = yaml.safe_load(yaml_path.read_text())
    changes: dict[str, dict] = {}
    stats = {"assigned": 0, "preserved": 0}

    for col in data.get("table", {}).get("columns", []):
        col_name = col["name"]

        if col.get("example_values") is not None:
            stats["preserved"] += 1
            continue

        if col.get("category") not in ("dimension", "identifier"):
            continue

        if col_name in profiling:
            changes[col_name] = profiling[col_name]
            stats["assigned"] += 1

    return changes, stats


def _apply_example_changes(
    yaml_path: Path,
    changes: dict[str, dict],
) -> None:
    """Surgically insert example_values and comprehensive into a YAML file."""
    lines = yaml_path.read_text().splitlines()
    result: list[str] = []
    current_col: str | None = None
    handled: set[str] = set()
    in_columns = False

    for line in lines:
        col_match = re.match(r"^  - name: (.+?)(\s*#.*)?$", line)
        if col_match:
            in_columns = True
            _flush_examples(result, current_col, changes, handled)
            current_col = col_match.group(1).strip()
            result.append(line)
            continue

        if (
            in_columns
            and line
            and not line.startswith("    ")
            and not line.startswith("  - name:")
        ):
            _flush_examples(result, current_col, changes, handled)
            current_col = None
            in_columns = False

        result.append(line)

    _flush_examples(result, current_col, changes, handled)
    yaml_path.write_text("\n".join(result) + "\n")


def _flush_examples(
    result: list[str],
    current_col: str | None,
    changes: dict[str, dict],
    handled: set[str],
) -> None:
    if current_col is None or current_col in handled or current_col not in changes:
        return
    info = changes[current_col]
    result.append("    example_values:")
    for val in info["example_values"]:
        # Quote strings that could be misinterpreted by YAML
        if isinstance(val, str):
            escaped = val.replace("'", "''")
            result.append(f"    - '{escaped}'")
        else:
            result.append(f"    - {val}")
    result.append(f"    comprehensive: {str(info['comprehensive']).lower()}")
    handled.add(current_col)


# ---------------------------------------------------------------------------
# BQ query execution
# ---------------------------------------------------------------------------


def _query_bq(sql: str, project: str, location: str) -> list[dict]:
    """Execute a BQ query and return rows as list of dicts."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project, location=location)
    query_job = client.query(sql)
    rows = []
    for row in query_job.result():
        row_dict = dict(row)
        # Convert APPROX_TOP_COUNT result (ARRAY<STRUCT<value, count>>)
        if "top_values" in row_dict and row_dict["top_values"] is not None:
            row_dict["top_values"] = [
                {"value": v.get("value"), "count": v.get("count")}
                for v in row_dict["top_values"]
            ]
        rows.append(row_dict)
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(
    *,
    dry_run: bool = False,
    trade_date: str = "2026-02-17",
    layer: str | None = None,
    table: str | None = None,
) -> dict[str, dict]:
    """Run BQ profiling and update YAML catalog with example_values."""
    from nl2sql_agent.config import Settings

    s = Settings()
    project = s.gcp_project
    location = s.bq_location

    dataset_map = {"kpi": s.kpi_dataset, "data": s.data_dataset}
    all_stats: dict[str, dict] = {}
    target_tables = filter_tables(layer, table) if (layer or table) else ALL_TABLES

    for layer, tables in target_tables.items():
        dataset = dataset_map[layer]
        for table_name in tables:
            yaml_path = CATALOG_DIR / layer / f"{table_name}.yaml"
            if not yaml_path.exists():
                print(f"SKIP: {yaml_path} not found")
                continue

            data = yaml.safe_load(yaml_path.read_text())
            cols_to_profile = get_columns_to_profile(data)
            col_names = [c["name"] for c in cols_to_profile]

            key = f"{layer}/{table_name}"

            if not col_names:
                print(f"{key}: no columns to profile")
                all_stats[key] = {"assigned": 0, "preserved": 0}
                continue

            sql = build_profiling_sql(
                project=project,
                dataset=dataset,
                table=table_name,
                columns=col_names,
                trade_date=trade_date,
            )

            if sql is None:
                continue

            print(f"{key}: profiling {len(col_names)} columns...")
            rows = _query_bq(sql, project, location)
            profiling = transform_profiling_results(rows)

            if dry_run:
                for col_name, info in profiling.items():
                    n = len(info["example_values"])
                    comp = info["comprehensive"]
                    print(f"  {col_name}: {n} values, comprehensive={comp}")
                all_stats[key] = {"assigned": len(profiling), "preserved": 0}
            else:
                changes, stats = _build_example_changes(yaml_path, profiling)
                if changes:
                    _apply_example_changes(yaml_path, changes)
                all_stats[key] = stats
                print(
                    f"{key}: assigned={stats['assigned']}, "
                    f"preserved={stats['preserved']}"
                )

    return all_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Profile BQ columns and populate example_values in YAML catalog"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report profiling results without writing YAML",
    )
    parser.add_argument(
        "--trade-date",
        default="2026-02-17",
        help="Trade date partition filter (default: 2026-02-17)",
    )
    parser.add_argument("--layer", help="Filter to one layer (kpi/data)")
    parser.add_argument("--table", help="Filter to one table name")
    args = parser.parse_args()
    main(
        dry_run=args.dry_run,
        trade_date=args.trade_date,
        layer=args.layer,
        table=args.table,
    )
