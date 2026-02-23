"""Assign category to every column in the YAML catalog.

Deterministic heuristic rules assign one of four categories:
  time, identifier, dimension, measure

Priority order:
  1. Time: DATE/TIMESTAMP types, or _ns/_date/_timestamp name patterns
  2. Identifier: _hash, _id, _key patterns + kafka/sequence fields
  3. Dimension: STRING/BOOLEAN without measure signals + known dimension names
  4. Measure: has formula, or numeric type not matched above

Usage:
    python scripts/enrich_categories.py [--dry-run]
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

from table_registry import ALL_TABLES, filter_combined_tables, filter_tables

# ---------------------------------------------------------------------------
# Heuristic patterns
# ---------------------------------------------------------------------------

# Time patterns: match column names that represent timestamps or dates
_TIME_SUFFIXES = ("_timestamp", "_timestamp_ns", "_date", "_ns")
_TIME_EXACT = frozenset({"trade_date", "event_date", "exchange_date"})
_TIME_TYPES = frozenset({"DATE", "TIMESTAMP"})

# Identifier patterns: system IDs, hashes, keys, infrastructure fields
_ID_SUFFIXES = ("_hash", "_id", "_key")
_ID_EXACT = frozenset(
    {
        "kafka_partition",
        "kafka_offset",
        "sequence_number",
        "revision",
    }
)

# Dimension: known categorical column names
_DIMENSION_EXACT = frozenset(
    {
        "symbol",
        "portfolio",
        "algo",
        "currency",
        "exchange",
        "counterparty",
        "account",
        "book",
    }
)
_DIMENSION_SUFFIXES = ("_side", "_type", "_name", "_type_name", "_source")


# ---------------------------------------------------------------------------
# Core categorization
# ---------------------------------------------------------------------------


def categorize_column(
    name: str,
    col_type: str,
    *,
    has_formula: bool = False,
) -> str:
    """Assign a category to a single column using deterministic heuristics.

    Returns one of: "time", "identifier", "dimension", "measure".
    """
    # Rule 1: Time columns
    if col_type in _TIME_TYPES:
        return "time"
    if name in _TIME_EXACT:
        return "time"
    if any(name.endswith(s) for s in _TIME_SUFFIXES) and col_type != "STRING":
        # STRING columns with timestamp-like names (e.g. expiry_timestamp)
        # are dimensions, not time columns
        return "time"

    # Rule 2: Identifier columns
    if name in _ID_EXACT:
        return "identifier"
    if any(name.endswith(s) for s in _ID_SUFFIXES):
        return "identifier"

    # Rule 3: Dimension columns
    if col_type == "BOOLEAN":
        return "dimension"
    if name in _DIMENSION_EXACT:
        return "dimension"
    if any(name.endswith(s) for s in _DIMENSION_SUFFIXES):
        return "dimension"
    if col_type == "STRING":
        return "dimension"

    # Rule 4: Measure columns
    if has_formula:
        return "measure"
    if col_type in ("FLOAT", "INTEGER"):
        return "measure"

    # Fallback: dimension (conservative)
    return "dimension"


# ---------------------------------------------------------------------------
# Table enrichment
# ---------------------------------------------------------------------------


def enrich_table_categories(
    data: dict,
    *,
    return_stats: bool = False,
) -> dict | tuple[dict, dict]:
    """Add category to every column in a table YAML dict.

    Preserves existing categories — only assigns where missing.
    """
    stats: dict = {
        "assigned": 0,
        "preserved": 0,
        "by_category": {"time": 0, "identifier": 0, "dimension": 0, "measure": 0},
    }

    for col in data.get("table", {}).get("columns", []):
        existing = col.get("category")
        if existing is not None:
            stats["preserved"] += 1
            stats["by_category"][existing] += 1
            continue

        category = categorize_column(
            col["name"],
            col["type"],
            has_formula="formula" in col,
        )
        col["category"] = category
        stats["assigned"] += 1
        stats["by_category"][category] += 1

    if return_stats:
        return data, stats
    return data


# ---------------------------------------------------------------------------
# Surgical YAML editing (same approach as enrich_formulas.py)
# ---------------------------------------------------------------------------


def _quote_category(category: str) -> str:
    """Category values are simple strings, no quoting needed."""
    return category


def _build_category_changes(
    yaml_path: Path,
) -> tuple[dict[str, str], dict]:
    """Determine category assignments without modifying the file."""
    data = yaml.safe_load(yaml_path.read_text())
    changes: dict[str, str] = {}
    stats: dict = {
        "assigned": 0,
        "preserved": 0,
        "by_category": {"time": 0, "identifier": 0, "dimension": 0, "measure": 0},
    }

    for col in data.get("table", {}).get("columns", []):
        col_name = col["name"]
        existing = col.get("category")
        if existing is not None:
            stats["preserved"] += 1
            stats["by_category"][existing] += 1
            continue

        category = categorize_column(
            col_name,
            col["type"],
            has_formula="formula" in col,
        )
        changes[col_name] = category
        stats["assigned"] += 1
        stats["by_category"][category] += 1

    return changes, stats


def _flush_pending(
    result: list[str],
    current_col: str | None,
    changes: dict[str, str],
    handled: set[str],
    field_indent: str = "    ",
) -> None:
    """Insert pending category for the current column if needed."""
    if (
        current_col is not None
        and current_col in changes
        and current_col not in handled
    ):
        result.append(f"{field_indent}category: {changes[current_col]}")
        handled.add(current_col)


def _apply_category_changes(
    yaml_path: Path,
    changes: dict[str, str],
) -> None:
    """Surgically insert category fields into a YAML file."""
    lines = yaml_path.read_text().splitlines()
    result: list[str] = []
    current_col: str | None = None
    handled: set[str] = set()
    in_columns = False
    field_indent = "    "  # default; detected from first column

    for line in lines:
        # Detect start of a new column block (2 or 4 space indent)
        col_match = re.match(r"^(\s+)- name: (.+?)(\s*#.*)?$", line)
        if col_match:
            in_columns = True
            _flush_pending(result, current_col, changes, handled, field_indent)
            col_indent = col_match.group(1)
            field_indent = col_indent + "  "  # fields are 2 more than list item
            current_col = col_match.group(2).strip()
            result.append(line)
            continue

        # Detect end of columns section: a non-empty line with less indent
        # than a column field
        if in_columns and line and not line[0].isspace():
            _flush_pending(result, current_col, changes, handled, field_indent)
            current_col = None
            in_columns = False

        result.append(line)

    # Handle case where file ends inside columns section
    _flush_pending(result, current_col, changes, handled, field_indent)

    yaml_path.write_text("\n".join(result) + "\n")


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
    """Run category assignment across table YAMLs."""
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

            changes, stats = _build_category_changes(yaml_path)

            if not dry_run and changes:
                _apply_category_changes(yaml_path, changes)

            key = f"{layer}/{table_name}"
            all_stats[key] = stats

            cats = stats["by_category"]
            print(
                f"{key}: assigned={stats['assigned']}, "
                f"preserved={stats['preserved']} | "
                f"time={cats['time']}, id={cats['identifier']}, "
                f"dim={cats['dimension']}, meas={cats['measure']}"
            )

    return all_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Assign categories to YAML catalog columns"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Report changes without writing"
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
