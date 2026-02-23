"""Coverage gate — reports enrichment gaps per table in the YAML catalog.

Checks each column for required enrichment fields based on its category and
layer, then reports coverage percentages and pass/fail status.

Usage:
    python scripts/check_coverage.py                       # all tables
    python scripts/check_coverage.py --table data/theodata  # single table
    python scripts/check_coverage.py --json                 # JSON output
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml
from table_registry import ALL_TABLES, filter_tables

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATALOG_DIR = PROJECT_ROOT / "catalog"

# ---------------------------------------------------------------------------
# Default thresholds
# ---------------------------------------------------------------------------

DEFAULT_THRESHOLDS = {
    "min_category": 95,
    "min_source": 90,
    "min_formula": 85,
    "min_description": 100,
}


# ---------------------------------------------------------------------------
# Coverage checking
# ---------------------------------------------------------------------------


def _is_placeholder_description(desc: str | None) -> bool:
    """Return True if description is empty or a placeholder."""
    if not desc:
        return True
    stripped = desc.strip()
    if not stripped:
        return True
    # Common placeholder patterns
    return stripped.lower() in ("", "todo", "tbd", "placeholder", "description")


def check_table_coverage(
    layer: str, table_name: str, thresholds: dict[str, int] | None = None
) -> dict[str, Any]:
    """Check enrichment coverage for a single table.

    Returns a dict with coverage percentages, gap details, and pass/fail.
    """
    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS

    yaml_path = CATALOG_DIR / layer / f"{table_name}.yaml"
    if not yaml_path.exists():
        return {
            "table": f"{layer}/{table_name}",
            "passed": False,
            "error": f"YAML not found: {yaml_path}",
            "coverage": {},
            "gaps": ["YAML file missing"],
        }

    data = yaml.safe_load(yaml_path.read_text())
    columns = data.get("table", {}).get("columns", [])
    total = len(columns)

    if total == 0:
        return {
            "table": f"{layer}/{table_name}",
            "passed": False,
            "error": "No columns found",
            "coverage": {},
            "gaps": ["No columns"],
        }

    # Count fields
    has_category = 0
    has_source = 0
    has_description = 0
    kpi_measure_total = 0
    kpi_measure_with_formula = 0
    measure_total = 0
    measure_with_agg = 0
    dim_id_total = 0
    dim_id_with_filterable = 0

    for col in columns:
        # Category
        if col.get("category"):
            has_category += 1

        # Source
        if col.get("source"):
            has_source += 1

        # Description (non-empty, non-placeholder)
        if not _is_placeholder_description(col.get("description")):
            has_description += 1

        # Formula (KPI measure columns only)
        cat = col.get("category")
        if layer == "kpi" and cat == "measure":
            kpi_measure_total += 1
            if col.get("formula"):
                kpi_measure_with_formula += 1

        # Aggregation (measure columns)
        if cat == "measure":
            measure_total += 1
            if col.get("typical_aggregation"):
                measure_with_agg += 1

        # Filterable (dimension/identifier columns)
        if cat in ("dimension", "identifier"):
            dim_id_total += 1
            if col.get("filterable") is not None:
                dim_id_with_filterable += 1

    # Calculate percentages
    pct_category = (has_category / total * 100) if total else 0
    pct_source = (has_source / total * 100) if total else 0
    pct_description = (has_description / total * 100) if total else 0
    pct_formula = (
        (kpi_measure_with_formula / kpi_measure_total * 100)
        if kpi_measure_total
        else None  # N/A for non-KPI or no measures
    )
    pct_aggregation = (
        (measure_with_agg / measure_total * 100) if measure_total else None
    )
    pct_filterable = (
        (dim_id_with_filterable / dim_id_total * 100) if dim_id_total else None
    )

    coverage = {
        "category": round(pct_category, 1),
        "source": round(pct_source, 1),
        "description": round(pct_description, 1),
        "formula": round(pct_formula, 1) if pct_formula is not None else "N/A",
        "aggregation": round(pct_aggregation, 1)
        if pct_aggregation is not None
        else "N/A",
        "filterable": round(pct_filterable, 1) if pct_filterable is not None else "N/A",
    }

    # Check thresholds
    gaps: list[str] = []
    if pct_category < thresholds["min_category"]:
        gaps.append(f"category={pct_category:.0f}%<{thresholds['min_category']}%")
    if pct_source < thresholds["min_source"]:
        gaps.append(f"source={pct_source:.0f}%<{thresholds['min_source']}%")
    if pct_description < thresholds["min_description"]:
        gaps.append(
            f"description={pct_description:.0f}%<{thresholds['min_description']}%"
        )
    if pct_formula is not None and pct_formula < thresholds["min_formula"]:
        gaps.append(f"formula={pct_formula:.0f}%<{thresholds['min_formula']}%")

    passed = len(gaps) == 0

    return {
        "table": f"{layer}/{table_name}",
        "passed": passed,
        "coverage": coverage,
        "gaps": gaps,
        "total_columns": total,
    }


def check_all_tables(
    thresholds: dict[str, int] | None = None,
    layer_filter: str | None = None,
    table_filter: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Check coverage for all registered tables (or filtered subset).

    Returns ``{table_key: report_dict}`` for each table.
    """
    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS

    target = (
        filter_tables(layer_filter, table_filter)
        if (layer_filter or table_filter)
        else ALL_TABLES
    )

    results: dict[str, dict[str, Any]] = {}
    for layer, tables in target.items():
        for table_name in tables:
            key = f"{layer}/{table_name}"
            results[key] = check_table_coverage(layer, table_name, thresholds)

    return results


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def _format_text(results: dict[str, dict]) -> str:
    """Human-readable coverage report."""
    lines: list[str] = []
    passed_count = 0
    total_count = len(results)

    for key, report in sorted(results.items()):
        cov = report["coverage"]
        status = "PASS" if report["passed"] else "FAIL"
        if report["passed"]:
            passed_count += 1

        parts = []
        for field in (
            "category",
            "source",
            "description",
            "formula",
            "aggregation",
            "filterable",
        ):
            val = cov.get(field, "N/A")
            parts.append(f"{field}={val}{'%' if val != 'N/A' else ''}")

        gap_info = f"  ({', '.join(report['gaps'])})" if report["gaps"] else ""
        lines.append(f"{key:30s}  {' '.join(parts):60s}  {status}{gap_info}")

    lines.append("")
    lines.append(
        f"Overall: {passed_count}/{total_count} tables pass ({passed_count / total_count * 100:.0f}%)"
        if total_count
        else "No tables found"
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check enrichment coverage of YAML catalog tables"
    )
    parser.add_argument(
        "--table",
        help="Filter to layer/table (e.g. 'data/theodata') or just table name",
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--min-category", type=int, default=95)
    parser.add_argument("--min-source", type=int, default=90)
    parser.add_argument("--min-formula", type=int, default=85)
    parser.add_argument("--min-description", type=int, default=100)
    args = parser.parse_args()

    thresholds = {
        "min_category": args.min_category,
        "min_source": args.min_source,
        "min_formula": args.min_formula,
        "min_description": args.min_description,
    }

    # Parse --table argument
    layer_filter = None
    table_filter = None
    if args.table:
        if "/" in args.table:
            layer_filter, table_filter = args.table.split("/", 1)
        else:
            table_filter = args.table

    results = check_all_tables(thresholds, layer_filter, table_filter)

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print(_format_text(results))

    # Exit code: 0 if all pass, 1 otherwise
    all_passed = all(r["passed"] for r in results.values())
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
