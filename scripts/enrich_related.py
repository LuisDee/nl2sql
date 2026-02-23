"""Populate related_columns by extracting column references from formulas.

For each column with a formula, parses the SQL expression to find other
column names referenced, filters to columns that exist in the same table,
and stores up to 5 as related_columns.

Usage:
    python scripts/enrich_related.py [--dry-run]
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

MAX_RELATED = 5

# SQL keywords and functions to exclude from column reference extraction
_SQL_KEYWORDS = frozenset(
    {
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "AND",
        "OR",
        "NOT",
        "IN",
        "IS",
        "NULL",
        "TRUE",
        "FALSE",
        "BETWEEN",
        "LIKE",
        "AS",
        "FROM",
        "WHERE",
        "SELECT",
        "GROUP",
        "BY",
        "ORDER",
        "HAVING",
        "LIMIT",
        "UNION",
        "ALL",
        "DISTINCT",
        "ON",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "OUTER",
        "CROSS",
        "EXISTS",
        "ANY",
        "SOME",
        "ABS",
        "COALESCE",
        "NULLIF",
        "IF",
        "IIF",
        "CAST",
        "CONVERT",
        "POW",
        "POWER",
        "SQRT",
        "LOG",
        "EXP",
        "ROUND",
        "FLOOR",
        "CEIL",
        "LENGTH",
        "CONCAT",
        "TRIM",
        "UPPER",
        "LOWER",
        "SUBSTR",
        "SUM",
        "AVG",
        "COUNT",
        "MIN",
        "MAX",
        "ARRAY_LENGTH",
        "BUY",
        "SELL",
        "BUYSELL_BUY",
        "BUYSELL_SELL",
        "NULL_BUYSELL",
    }
)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def extract_formula_references(formula: str) -> set[str]:
    """Extract potential column name references from a SQL formula.

    Returns a set of identifier-like tokens that are not SQL keywords,
    string literals, or numeric literals.
    """
    # Remove string literals (single and double quoted)
    cleaned = re.sub(r"'[^']*'", "", formula)
    cleaned = re.sub(r'"[^"]*"', "", cleaned)

    # Extract word tokens (identifiers)
    tokens = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", cleaned)

    # Filter out SQL keywords and functions (case-insensitive)
    refs = set()
    for token in tokens:
        if token.upper() not in _SQL_KEYWORDS and not token.isdigit():
            refs.add(token)

    return refs


def enrich_table_related(
    data: dict,
    *,
    return_stats: bool = False,
) -> dict | tuple[dict, dict]:
    """Add related_columns to columns with formulas."""
    stats = {"assigned": 0, "preserved": 0}

    # Build set of all column names in this table
    columns = data.get("table", {}).get("columns", [])
    all_col_names = {c["name"] for c in columns}

    for col in columns:
        # Skip if already has related_columns
        if col.get("related_columns") is not None:
            stats["preserved"] += 1
            continue

        formula = col.get("formula")
        if not formula:
            continue

        # Extract references and filter to same-table columns
        refs = extract_formula_references(formula)
        related = sorted(refs & all_col_names - {col["name"]})

        if not related:
            continue

        col["related_columns"] = related[:MAX_RELATED]
        stats["assigned"] += 1

    if return_stats:
        return data, stats
    return data


# ---------------------------------------------------------------------------
# Surgical YAML editing
# ---------------------------------------------------------------------------


def _build_related_changes(
    yaml_path: Path,
) -> tuple[dict[str, list[str]], dict]:
    """Determine related_columns changes without modifying the file."""
    data = yaml.safe_load(yaml_path.read_text())
    changes: dict[str, list[str]] = {}
    stats = {"assigned": 0, "preserved": 0}

    columns = data.get("table", {}).get("columns", [])
    all_col_names = {c["name"] for c in columns}

    for col in columns:
        if col.get("related_columns") is not None:
            stats["preserved"] += 1
            continue

        formula = col.get("formula")
        if not formula:
            continue

        refs = extract_formula_references(formula)
        related = sorted(refs & all_col_names - {col["name"]})

        if related:
            changes[col["name"]] = related[:MAX_RELATED]
            stats["assigned"] += 1

    return changes, stats


def _apply_related_changes(
    yaml_path: Path,
    changes: dict[str, list[str]],
) -> None:
    """Surgically insert related_columns fields into a YAML file."""
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
            _flush_related(result, current_col, changes, handled, field_indent)
            col_indent = col_match.group(1)
            field_indent = col_indent + "  "
            current_col = col_match.group(2).strip()
            result.append(line)
            continue

        if in_columns and line and not line[0].isspace():
            _flush_related(result, current_col, changes, handled, field_indent)
            current_col = None
            in_columns = False

        result.append(line)

    _flush_related(result, current_col, changes, handled, field_indent)
    yaml_path.write_text("\n".join(result) + "\n")


def _flush_related(
    result: list[str],
    current_col: str | None,
    changes: dict[str, list[str]],
    handled: set[str],
    field_indent: str = "    ",
) -> None:
    if current_col is None or current_col in handled or current_col not in changes:
        return
    related = changes[current_col]
    result.append(f"{field_indent}related_columns:")
    for col_name in related:
        result.append(f"{field_indent}- {col_name}")
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
    """Run related columns enrichment across table YAMLs."""
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

            changes, stats = _build_related_changes(yaml_path)

            if not dry_run and changes:
                _apply_related_changes(yaml_path, changes)

            key = f"{layer}/{table_name}"
            all_stats[key] = stats
            print(
                f"{key}: assigned={stats['assigned']}, preserved={stats['preserved']}"
            )

    return all_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Populate related_columns from formula references"
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
