"""Enrich KPI table YAMLs with verified formulas from kpi_computations.yaml.

Reads the structural index produced by Track 23 and adds/updates/verifies
formula fields on every KPI column that has a corresponding formula in the
source data.

Usage:
    python scripts/enrich_formulas.py [--dry-run]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
METADATA_DIR = PROJECT_ROOT / "metadata"
CATALOG_DIR = PROJECT_ROOT / "catalog"
KPI_COMPUTATIONS_PATH = METADATA_DIR / "kpi_computations.yaml"

KPI_TABLES = ["markettrade", "quotertrade", "brokertrade", "clicktrade", "otoswing"]


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------


def load_kpi_computations(path: Path) -> dict:
    """Load and parse kpi_computations.yaml."""
    with open(path) as f:
        return yaml.safe_load(f)


def get_all_intervals(computations: dict) -> list[str]:
    """Extract all interval names from time_intervals section."""
    intervals: list[str] = []
    ti = computations.get("time_intervals", {})
    for entry in ti.get("intraday", []):
        intervals.append(entry["name"])
    for entry in ti.get("multiday", []):
        intervals.append(entry["name"])
    return intervals


def _resolve_formula(
    metric: dict,
    trade_type: str,
    shared_formulas: dict,
) -> str | None:
    """Extract the formula text from a metric/intermediate entry.

    Resolution order:
    1. Direct ``formula`` key (if it doesn't reference shared_formulas)
    2. ``standard`` key (for entries with regional variants)
    3. Shared formula reference ("See shared_formulas.X")
    4. Trade-type specific shared formula
    """
    formula = metric.get("formula")

    # Check for shared formula reference
    if isinstance(formula, str) and formula.startswith("See shared_formulas."):
        ref_name = formula.split(".")[-1]
        shared = shared_formulas.get(ref_name, {})
        # Use standard variant
        return shared.get("standard", shared.get(trade_type))

    if formula is not None:
        # Normalize multiline YAML strings
        return str(formula).strip()

    # Fall back to standard variant
    standard = metric.get("standard")
    if standard is not None:
        return str(standard).strip()

    return None


def _find_trade_type(computations: dict, name: str) -> dict | None:
    """Find a trade type entry by name in kpi_computations."""
    for tt in computations.get("trade_types", []):
        if tt["name"] == name:
            return tt
    return None


def build_formula_index(
    computations: dict,
    trade_type: str,
    intervals: list[str],
) -> dict[str, str]:
    """Build a {column_name: formula} lookup for a given trade type.

    Handles:
    - Direct metrics and intermediate calculations
    - Per-interval expansion ({interval} → concrete interval names)
    - Shared formula resolution (adjusted_tv, vol_path_estimate)
    """
    index: dict[str, str] = {}
    shared_formulas = computations.get("shared_formulas", {})
    tt = _find_trade_type(computations, trade_type)
    if tt is None:
        return index

    # Process metrics and intermediate_calculations
    for section_key in ("metrics", "intermediate_calculations"):
        entries = tt.get(section_key, [])
        for entry in entries:
            name_template = entry["name"]
            formula = _resolve_formula(entry, trade_type, shared_formulas)
            if formula is None:
                continue

            if entry.get("per_interval"):
                # Expand to all intervals
                for interval in intervals:
                    col_name = name_template.replace("{interval}", interval)
                    expanded = formula.replace("{interval}", interval)
                    index[col_name] = expanded
            elif "{interval}" in name_template:
                # Template name but not marked per_interval — expand anyway
                for interval in intervals:
                    col_name = name_template.replace("{interval}", interval)
                    expanded = formula.replace("{interval}", interval)
                    index[col_name] = expanded
            else:
                index[name_template] = formula

    # Process shared formulas that are per-trade-type and per-interval
    # (vol_path_estimate has per-trade-type variants)
    for sf_name, sf_data in shared_formulas.items():
        # Skip if already handled via metrics (e.g., adjusted_tv is referenced
        # from metrics entries). Only handle formulas with per-trade-type
        # variants that aren't referenced elsewhere.
        if isinstance(sf_data, dict) and trade_type in sf_data:
            template_formula = sf_data[trade_type]
            template_col = f"{sf_name}_{{interval}}"
            # Only add if not already present from metrics/intermediates
            test_name = f"{sf_name}_1s"
            if test_name not in index:
                for interval in intervals:
                    col_name = template_col.replace("{interval}", interval)
                    expanded = template_formula.replace("{interval}", interval)
                    index[col_name] = expanded

    return index


def enrich_table_yaml(
    data: dict,
    formula_index: dict[str, str],
    *,
    return_stats: bool = False,
) -> dict | tuple[dict, dict[str, int]]:
    """Add/update formulas in a table YAML dict using the formula index.

    Args:
        data: Parsed YAML dict with ``table.columns`` list.
        formula_index: ``{column_name: formula_text}`` lookup.
        return_stats: If True, return ``(data, stats)`` tuple.

    Returns:
        Modified data dict (mutated in place), or ``(data, stats)`` if
        ``return_stats`` is True.
    """
    stats: dict[str, int] = {
        "added": 0,
        "updated": 0,
        "verified": 0,
        "total_in_source": 0,
    }
    columns = data.get("table", {}).get("columns", [])

    for col in columns:
        col_name = col["name"]
        if col_name not in formula_index:
            continue

        stats["total_in_source"] += 1
        source_formula = formula_index[col_name]
        existing = col.get("formula")

        if existing is None:
            col["formula"] = source_formula
            stats["added"] += 1
        elif existing.strip() != source_formula.strip():
            col["formula"] = source_formula
            stats["updated"] += 1
        else:
            stats["verified"] += 1

    if return_stats:
        return data, stats
    return data


# ---------------------------------------------------------------------------
# YAML I/O helpers
# ---------------------------------------------------------------------------


def _load_yaml(path: Path) -> dict:
    """Load a YAML file."""
    with open(path) as f:
        return yaml.safe_load(f)


def _write_yaml(path: Path, data: dict) -> None:
    """Write data back to a YAML file preserving readability."""
    with open(path, "w") as f:
        yaml.dump(
            data,
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
            width=120,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(dry_run: bool = False) -> dict[str, dict[str, int]]:
    """Run formula enrichment across all KPI tables.

    Returns:
        ``{table_name: stats_dict}`` for each table processed.
    """
    computations = load_kpi_computations(KPI_COMPUTATIONS_PATH)
    intervals = get_all_intervals(computations)
    all_stats: dict[str, dict[str, int]] = {}

    for table_name in KPI_TABLES:
        yaml_path = CATALOG_DIR / "kpi" / f"{table_name}.yaml"
        if not yaml_path.exists():
            print(f"SKIP: {yaml_path} not found")
            continue

        data = _load_yaml(yaml_path)
        formula_index = build_formula_index(computations, table_name, intervals)
        _, stats = enrich_table_yaml(data, formula_index, return_stats=True)
        all_stats[table_name] = stats

        if not dry_run:
            _write_yaml(yaml_path, data)

        print(
            f"{table_name}: "
            f"added={stats['added']}, "
            f"updated={stats['updated']}, "
            f"verified={stats['verified']}, "
            f"total_in_source={stats['total_in_source']}"
        )

    return all_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enrich KPI YAMLs with verified formulas"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Report changes without writing"
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)
