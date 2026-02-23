"""Assign typical_aggregation and filterable to YAML catalog columns.

Deterministic heuristic rules:
- typical_aggregation: SUM for additive metrics (PnL, edge, slippage, size),
  AVG for non-additive (price, TV, greeks, per-unit, ratios)
- filterable: True for dimension/identifier columns commonly used in WHERE,
  plus trade_date (partition key)

Usage:
    python scripts/enrich_aggregation.py [--dry-run]
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

from table_registry import ALL_TABLES, filter_tables

# ---------------------------------------------------------------------------
# Aggregation patterns
# ---------------------------------------------------------------------------

# SUM patterns: additive metrics
_SUM_PATTERNS = (
    "_pnl",
    "_edge",
    "_slippage",
    "_fees",
    "instant_pnl",
    "instant_edge",
    "fees",
    "total_slippage",
)

_SUM_CONTAINS = ("_size", "_volume", "volume", "traded_size", "routed_size")

# AVG patterns: non-additive
_AVG_PATTERNS = (
    "price",
    "_tv",
    "tv",
    "delta",
    "gamma",
    "vega",
    "theta",
    "rho",
    "strike",
    "multiplier",
    "contract_size",
    "leg_ratio",
    "_per_unit",
    "_adjustment",
    "_val",
    "_bv_",
    "raw_bv_",
    "vol_path_estimate",
    "calculated_base_val",
    "adjusted_tv",
    "tv_change",
    "mid_base",
    "ref_",
)

# Filterable: columns commonly used in WHERE clauses
_FILTERABLE_NAMES = frozenset(
    {
        "symbol",
        "portfolio",
        "algo",
        "currency",
        "exchange",
        "counterparty",
        "trade_date",
        "event_date",
        "exchange_date",
    }
)

_FILTERABLE_SUFFIXES = ("_side", "_type", "_name", "_type_name", "_source")


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


_AVG_EXACT = frozenset(
    {
        "contract_size",
        "leg_ratio",
        "buy_sell_multiplier",
        "market_mako_multiplier",
    }
)


def assign_aggregation(name: str) -> str:
    """Assign typical_aggregation for a measure column.

    Returns "SUM" or "AVG".
    """
    # Per-unit metrics are always AVG (not additive)
    if "_per_unit" in name:
        return "AVG"

    # Exact AVG names (checked before SUM patterns to avoid false matches)
    if name in _AVG_EXACT:
        return "AVG"

    # SUM: PnL, edge, slippage, fees
    for pattern in _SUM_PATTERNS:
        if pattern in name:
            return "SUM"

    # SUM: size/volume
    for pattern in _SUM_CONTAINS:
        if pattern in name:
            return "SUM"

    # AVG: price, TV, greeks, intermediates, adjustments
    for pattern in _AVG_PATTERNS:
        if pattern in name:
            return "AVG"

    # Default: AVG (conservative — non-additive is safer default)
    return "AVG"


def assign_filterable(name: str, category: str) -> bool:
    """Assign filterable flag based on column name and category."""
    if category == "measure":
        return False

    # Partition keys and common filter dimensions
    if name in _FILTERABLE_NAMES:
        return True

    # Dimension/identifier columns with filter-friendly patterns
    if category in ("dimension", "identifier"):
        if any(name.endswith(s) for s in _FILTERABLE_SUFFIXES):
            return True
        # All identifiers (hash, id, key) are filterable
        if category == "identifier":
            return True
        # BOOLEANs are filterable (is_parent, parent_is_combo, etc.)
        return True

    # Time columns: only trade_date/event_date are commonly filtered
    if category == "time":
        return name in ("trade_date", "event_date", "exchange_date")

    return False


# ---------------------------------------------------------------------------
# Table enrichment
# ---------------------------------------------------------------------------


def enrich_table_aggregation(
    data: dict,
    *,
    return_stats: bool = False,
) -> dict | tuple[dict, dict]:
    """Add typical_aggregation and filterable to columns."""
    stats = {
        "agg_assigned": 0,
        "agg_preserved": 0,
        "filterable_assigned": 0,
        "filterable_preserved": 0,
    }

    for col in data.get("table", {}).get("columns", []):
        category = col.get("category")

        # Aggregation: only for measures
        if category == "measure":
            if col.get("typical_aggregation") is not None:
                stats["agg_preserved"] += 1
            else:
                col["typical_aggregation"] = assign_aggregation(col["name"])
                stats["agg_assigned"] += 1

        # Filterable: for non-measures
        if category in ("dimension", "identifier", "time"):
            if col.get("filterable") is not None:
                stats["filterable_preserved"] += 1
            else:
                filterable = assign_filterable(col["name"], category)
                if filterable:
                    col["filterable"] = True
                    stats["filterable_assigned"] += 1

    if return_stats:
        return data, stats
    return data


# ---------------------------------------------------------------------------
# Surgical YAML editing
# ---------------------------------------------------------------------------


def _build_agg_changes(
    yaml_path: Path,
) -> tuple[dict[str, dict[str, str | bool]], dict]:
    """Determine aggregation/filterable changes without modifying the file."""
    data = yaml.safe_load(yaml_path.read_text())
    changes: dict[str, dict[str, str | bool]] = {}
    stats = {
        "agg_assigned": 0,
        "agg_preserved": 0,
        "filterable_assigned": 0,
        "filterable_preserved": 0,
    }

    for col in data.get("table", {}).get("columns", []):
        col_name = col["name"]
        category = col.get("category")
        col_changes: dict[str, str | bool] = {}

        if category == "measure":
            if col.get("typical_aggregation") is not None:
                stats["agg_preserved"] += 1
            else:
                col_changes["typical_aggregation"] = assign_aggregation(col_name)
                stats["agg_assigned"] += 1

        if category in ("dimension", "identifier", "time"):
            if col.get("filterable") is not None:
                stats["filterable_preserved"] += 1
            else:
                filterable = assign_filterable(col_name, category)
                if filterable:
                    col_changes["filterable"] = True
                    stats["filterable_assigned"] += 1

        if col_changes:
            changes[col_name] = col_changes

    return changes, stats


def _apply_agg_changes(
    yaml_path: Path,
    changes: dict[str, dict[str, str | bool]],
) -> None:
    """Surgically insert aggregation/filterable fields into a YAML file."""
    lines = yaml_path.read_text().splitlines()
    result: list[str] = []
    current_col: str | None = None
    handled: set[str] = set()
    in_columns = False

    for line in lines:
        col_match = re.match(r"^  - name: (.+?)(\s*#.*)?$", line)
        if col_match:
            in_columns = True
            _flush_agg(result, current_col, changes, handled)
            current_col = col_match.group(1).strip()
            result.append(line)
            continue

        if (
            in_columns
            and line
            and not line.startswith("    ")
            and not line.startswith("  - name:")
        ):
            _flush_agg(result, current_col, changes, handled)
            current_col = None
            in_columns = False

        result.append(line)

    _flush_agg(result, current_col, changes, handled)
    yaml_path.write_text("\n".join(result) + "\n")


def _flush_agg(
    result: list[str],
    current_col: str | None,
    changes: dict[str, dict[str, str | bool]],
    handled: set[str],
) -> None:
    if current_col is None or current_col in handled or current_col not in changes:
        return
    for key, value in changes[current_col].items():
        if isinstance(value, bool):
            result.append(f"    {key}: {str(value).lower()}")
        else:
            result.append(f"    {key}: {value}")
    handled.add(current_col)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(
    dry_run: bool = False,
    layer: str | None = None,
    table: str | None = None,
) -> dict[str, dict]:
    """Run aggregation + filterable assignment across table YAMLs."""
    all_stats: dict[str, dict] = {}
    target_tables = filter_tables(layer, table) if (layer or table) else ALL_TABLES

    for layer, tables in target_tables.items():
        for table_name in tables:
            yaml_path = CATALOG_DIR / layer / f"{table_name}.yaml"
            if not yaml_path.exists():
                print(f"SKIP: {yaml_path} not found")
                continue

            changes, stats = _build_agg_changes(yaml_path)

            if not dry_run and changes:
                _apply_agg_changes(yaml_path, changes)

            key = f"{layer}/{table_name}"
            all_stats[key] = stats

            print(
                f"{key}: "
                f"agg={stats['agg_assigned']}, "
                f"filterable={stats['filterable_assigned']}"
            )

    return all_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Assign aggregation and filterable to catalog columns"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Report changes without writing"
    )
    parser.add_argument("--layer", help="Filter to one layer (kpi/data)")
    parser.add_argument("--table", help="Filter to one table name")
    args = parser.parse_args()
    main(dry_run=args.dry_run, layer=args.layer, table=args.table)
