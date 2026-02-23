"""Populate source fields by mapping columns to proto/KPI origins.

For data-layer columns: maps through data_loader_transforms.yaml → proto_fields.yaml
For KPI-layer columns: maps through kpi_computations.yaml

Usage:
    python scripts/enrich_source.py [--dry-run]
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATALOG_DIR = PROJECT_ROOT / "catalog"
METADATA_DIR = PROJECT_ROOT / "metadata"

from table_registry import ALL_TABLES, filter_combined_tables, filter_tables

# Kafka/infrastructure columns that don't come from proto definitions
_KAFKA_FIELDS = frozenset(
    {
        "kafka_partition",
        "kafka_offset",
        "kafka_message_timestamp",
        "record_written_timestamp",
        "partition_timestamp_local",
        "partition_number",
    }
)


# ---------------------------------------------------------------------------
# Proto column mapping
# ---------------------------------------------------------------------------


def build_proto_column_map(
    table_name: str,
    transforms: dict,
    proto_to_bq: dict,
) -> dict[str, str]:
    """Build a mapping of BQ column name → proto source string.

    Uses data_loader_transforms.yaml to find the source_field for each column,
    then maps to the proto message from proto_to_bq.
    """
    # Find the table in transforms
    table_info = None
    for t in transforms.get("tables", []):
        if t["name"] == table_name:
            table_info = t
            break

    if table_info is None:
        return {}

    # Get proto message info
    proto_info = proto_to_bq.get(table_name, {})
    message_name = proto_info.get("message", "")
    proto_file = proto_info.get("file", "")

    result: dict[str, str] = {}

    # Map regular columns
    for col in table_info.get("columns", []):
        col_name = col["name"]
        source_field = col.get("source_field", "")
        transform = col.get("transformation", "")

        if col_name in _KAFKA_FIELDS or source_field in _KAFKA_FIELDS:
            result[col_name] = "kafka infrastructure"
            continue

        if transform == "derive":
            result[col_name] = "derived (data-loader)"
            continue

        if transform == "direct" and source_field == col_name:
            # Direct passthrough — likely kafka/infrastructure
            if col_name in _KAFKA_FIELDS:
                result[col_name] = "kafka infrastructure"
            else:
                result[col_name] = "kafka infrastructure"
            continue

        # For unnest/rename: extract the actual field name
        field_name = source_field
        if "." in field_name:
            # e.g., props.tradePrice → tradePrice
            field_name = field_name.split(".")[-1]

        if message_name and proto_file:
            result[col_name] = f"{message_name}.{field_name} ({proto_file})"
        elif field_name:
            result[col_name] = f"proto field: {field_name}"

    # Map enrichment columns (from instrument join)
    for enrich_col in table_info.get("enrichment_columns_from_instruments", []):
        result[enrich_col] = "instrument enrichment (data-loader)"

    return result


# ---------------------------------------------------------------------------
# KPI source mapping
# ---------------------------------------------------------------------------


def build_kpi_source_map(table_name: str, kpi_yaml: dict) -> dict[str, str]:
    """Build source mapping for KPI columns from kpi_computations.yaml.

    Handles both dict-style (test fixtures) and list-style (real YAML) structures.
    """
    result: dict[str, str] = {}

    # Shared formulas (always a dict keyed by name)
    shared = kpi_yaml.get("shared_formulas", {})
    if isinstance(shared, dict):
        for col_name in shared:
            result[col_name] = f"KPI shared formula ({col_name})"

    # Trade-type-specific: can be dict (test) or list (real YAML)
    trade_types = kpi_yaml.get("trade_types", {})

    if isinstance(trade_types, list):
        trade_type = {}
        for tt in trade_types:
            if tt.get("name") == table_name:
                trade_type = tt
                break
    elif isinstance(trade_types, dict):
        trade_type = trade_types.get(table_name, {})
    else:
        trade_type = {}

    # Metrics: can be dict (test) or list (real YAML)
    metrics = trade_type.get("metrics", {})
    if isinstance(metrics, list):
        for m in metrics:
            col_name = m.get("name", "")
            if col_name:
                result[col_name] = f"KPI computation ({table_name}.{col_name})"
    elif isinstance(metrics, dict):
        for col_name in metrics:
            result[col_name] = f"KPI computation ({table_name}.{col_name})"

    # Intermediate calculations: can be dict (test) or list (real YAML)
    intermediates = trade_type.get("intermediate_calculations", {})
    if isinstance(intermediates, list):
        for ic in intermediates:
            col_name = ic.get("name", "")
            if col_name:
                result[col_name] = f"KPI intermediate ({table_name}.{col_name})"
    elif isinstance(intermediates, dict):
        for col_name in intermediates:
            result[col_name] = f"KPI intermediate ({table_name}.{col_name})"

    return result


# ---------------------------------------------------------------------------
# Table enrichment
# ---------------------------------------------------------------------------


def enrich_table_source(
    data: dict,
    source_map: dict[str, str],
    *,
    return_stats: bool = False,
) -> dict | tuple[dict, dict]:
    """Apply source mapping to a table's columns."""
    stats = {"assigned": 0, "preserved": 0}
    columns = data.get("table", {}).get("columns", [])

    for col in columns:
        col_name = col["name"]

        if col.get("source") is not None:
            stats["preserved"] += 1
            continue

        if col_name in source_map:
            col["source"] = source_map[col_name]
            stats["assigned"] += 1

    if return_stats:
        return data, stats
    return data


# ---------------------------------------------------------------------------
# Surgical YAML editing
# ---------------------------------------------------------------------------


def _build_source_changes(
    yaml_path: Path,
    source_map: dict[str, str],
) -> tuple[dict[str, str], dict]:
    """Determine source field changes without modifying the file."""
    data = yaml.safe_load(yaml_path.read_text())
    changes: dict[str, str] = {}
    stats = {"assigned": 0, "preserved": 0}

    for col in data.get("table", {}).get("columns", []):
        col_name = col["name"]

        if col.get("source") is not None:
            stats["preserved"] += 1
            continue

        if col_name in source_map:
            changes[col_name] = source_map[col_name]
            stats["assigned"] += 1

    return changes, stats


def _apply_source_changes(
    yaml_path: Path,
    changes: dict[str, str],
) -> None:
    """Surgically insert source fields into a YAML file."""
    lines = yaml_path.read_text().splitlines()
    result: list[str] = []
    current_col: str | None = None
    handled: set[str] = set()
    in_columns = False
    field_indent = "    "

    for line in lines:
        col_match = re.match(r"^(\s+)- name: (.+?)(\s*#.*)?$", line)
        if col_match:
            in_columns = True
            _flush_source(result, current_col, changes, handled, field_indent)
            col_indent = col_match.group(1)
            field_indent = col_indent + "  "
            current_col = col_match.group(2).strip()
            result.append(line)
            continue

        if in_columns and line and not line[0].isspace():
            _flush_source(result, current_col, changes, handled, field_indent)
            current_col = None
            in_columns = False

        result.append(line)

    _flush_source(result, current_col, changes, handled, field_indent)
    yaml_path.write_text("\n".join(result) + "\n")


def _flush_source(
    result: list[str],
    current_col: str | None,
    changes: dict[str, str],
    handled: set[str],
    field_indent: str = "    ",
) -> None:
    if current_col is None or current_col in handled or current_col not in changes:
        return
    source = changes[current_col]
    # Quote if contains special YAML characters
    if any(c in source for c in ":#{}[]"):
        escaped = source.replace("'", "''")
        result.append(f"{field_indent}source: '{escaped}'")
    else:
        result.append(f"{field_indent}source: {source}")
    handled.add(current_col)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(
    dry_run: bool = False,
    layer: str | None = None,
    table: str | None = None,
    *,
    all_markets: bool = False,
) -> dict[str, dict]:
    """Run source field enrichment across table YAMLs."""
    # Load metadata indexes
    transforms = yaml.safe_load(
        (METADATA_DIR / "data_loader_transforms.yaml").read_text()
    )
    proto_data = yaml.safe_load((METADATA_DIR / "proto_fields.yaml").read_text())
    proto_to_bq = proto_data.get("proto_to_bq", {})
    kpi_data = yaml.safe_load((METADATA_DIR / "kpi_computations.yaml").read_text())

    all_stats: dict[str, dict] = {}
    if all_markets or (layer and layer not in ALL_TABLES):
        target_tables = filter_combined_tables(layer, table, include_markets=True)
    else:
        target_tables = filter_tables(layer, table) if (layer or table) else ALL_TABLES

    for layer, tables in target_tables.items():
        for table_name in tables:
            yaml_path = CATALOG_DIR / layer / f"{table_name}.yaml"
            if not yaml_path.exists():
                print(f"SKIP: {yaml_path} not found")
                continue

            # Build source map based on layer
            # Market directories (e.g. arb_data, brazil_data) use data-layer mappings
            if layer == "kpi":
                # KPI layer: combine proto origins + KPI computation sources
                source_map = build_proto_column_map(table_name, transforms, proto_to_bq)
                kpi_sources = build_kpi_source_map(table_name, kpi_data)
                source_map.update(kpi_sources)
            else:
                source_map = build_proto_column_map(table_name, transforms, proto_to_bq)

            key = f"{layer}/{table_name}"

            if not source_map:
                print(f"{key}: no source mappings found")
                all_stats[key] = {"assigned": 0, "preserved": 0}
                continue

            if dry_run:
                data = yaml.safe_load(yaml_path.read_text())
                _, stats = enrich_table_source(data, source_map, return_stats=True)
            else:
                changes, stats = _build_source_changes(yaml_path, source_map)
                if changes:
                    _apply_source_changes(yaml_path, changes)

            all_stats[key] = stats
            print(
                f"{key}: assigned={stats['assigned']}, preserved={stats['preserved']}"
            )

    return all_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Populate source fields from proto/KPI metadata"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report changes without writing",
    )
    parser.add_argument(
        "--layer", help="Filter to one layer/market (kpi/data/arb_data/...)"
    )
    parser.add_argument("--table", help="Filter to one table name")
    parser.add_argument(
        "--all-markets", action="store_true", help="Include all market directories"
    )
    args = parser.parse_args()
    main(
        dry_run=args.dry_run,
        layer=args.layer,
        table=args.table,
        all_markets=args.all_markets,
    )
