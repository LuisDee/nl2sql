"""Orchestrator — single-command table onboarding pipeline.

Chains skeleton generation, enrichment scripts, and coverage checking
for one or all tables.

Usage:
    # Onboard a new table
    python scripts/onboard_table.py --layer data --table newtable --live

    # Re-enrich an existing table
    python scripts/onboard_table.py --layer kpi --table markettrade --enrich-only

    # Re-enrich ALL registered tables
    python scripts/onboard_table.py --all --enrich-only

    # Dry run
    python scripts/onboard_table.py --layer data --table newtable --dry-run
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml
from table_registry import ALL_TABLES, filter_tables

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATALOG_DIR = PROJECT_ROOT / "catalog"


# ---------------------------------------------------------------------------
# Step 1: Skeleton generation
# ---------------------------------------------------------------------------


def step_generate_skeleton(
    layer: str, table: str, *, live: bool = False, dry_run: bool = False
) -> bool:
    """Generate skeleton YAML for a new table. Returns True if generated."""
    yaml_path = CATALOG_DIR / layer / f"{table}.yaml"
    if yaml_path.exists():
        print(f"  [skeleton] SKIP: {yaml_path} already exists")
        return False

    from generate_skeleton import load_schema_live, load_schema_offline, write_skeleton

    try:
        if live:
            columns = load_schema_live(layer, table)
        else:
            columns = load_schema_offline(layer, table)
    except FileNotFoundError as e:
        print(f"  [skeleton] ERROR: {e}")
        return False

    if not columns:
        print("  [skeleton] ERROR: no columns found")
        return False

    if dry_run:
        print(
            f"  [skeleton] DRY-RUN: would generate {yaml_path} ({len(columns)} columns)"
        )
        return True

    path = write_skeleton(layer, table, columns, force=False)
    print(f"  [skeleton] Generated {path} ({len(columns)} columns)")
    return True


# ---------------------------------------------------------------------------
# Step 2: Registry + _dataset.yaml update
# ---------------------------------------------------------------------------


def step_update_dataset_yaml(layer: str, table: str, *, dry_run: bool = False) -> bool:
    """Add table to _dataset.yaml tables list if not already present."""
    dataset_path = CATALOG_DIR / layer / "_dataset.yaml"
    if not dataset_path.exists():
        print(f"  [registry] SKIP: {dataset_path} not found")
        return False

    data = yaml.safe_load(dataset_path.read_text())
    tables_list = data.get("dataset", {}).get("tables", [])

    if table in tables_list:
        print(f"  [registry] SKIP: {table} already in {layer}/_dataset.yaml")
        return False

    if dry_run:
        print(f"  [registry] DRY-RUN: would add '{table}' to {layer}/_dataset.yaml")
        return True

    # Surgical insertion — find the tables: list and add the new table
    lines = dataset_path.read_text().splitlines()
    result: list[str] = []
    in_tables = False
    inserted = False
    last_entry_indent = "    "  # default 4-space

    for line in lines:
        # Detect "tables:" key
        if re.match(r"^\s+tables:\s*$", line):
            in_tables = True
            result.append(line)
            continue

        if in_tables:
            m = re.match(r"^(\s+-\s+)(\S+)", line)
            if m:
                last_entry_indent = m.group(1)
                current = m.group(2)
                if not inserted and table < current:
                    result.append(f"{last_entry_indent}{table}")
                    inserted = True
                result.append(line)
            else:
                # End of tables list
                if not inserted:
                    result.append(f"{last_entry_indent}{table}")
                    inserted = True
                in_tables = False
                result.append(line)
        else:
            result.append(line)

    # If we reached end of file still in tables list
    if in_tables and not inserted:
        result.append(f"{last_entry_indent}{table}")

    dataset_path.write_text("\n".join(result) + "\n")
    print(f"  [registry] Added '{table}' to {layer}/_dataset.yaml")
    return True


# ---------------------------------------------------------------------------
# Step 3: Enrichment scripts
# ---------------------------------------------------------------------------


def step_enrich(
    layer: str, table: str, *, dry_run: bool = False, live: bool = False
) -> dict[str, dict]:
    """Run all enrichment scripts for a single table.

    Returns dict of script_name -> stats.
    """
    results: dict[str, dict] = {}

    # 3a. Categories
    print("  [enrich] Running enrich_categories...")
    import enrich_categories

    stats = enrich_categories.main(dry_run=dry_run, layer=layer, table=table)
    results["categories"] = stats

    # 3b. Formulas (KPI only)
    if layer == "kpi":
        print("  [enrich] Running enrich_formulas...")
        import enrich_formulas

        stats = enrich_formulas.main(dry_run=dry_run, table=table)
        results["formulas"] = stats

    # 3c. Aggregation
    print("  [enrich] Running enrich_aggregation...")
    import enrich_aggregation

    stats = enrich_aggregation.main(dry_run=dry_run, layer=layer, table=table)
    results["aggregation"] = stats

    # 3d. Source
    print("  [enrich] Running enrich_source...")
    import enrich_source

    stats = enrich_source.main(dry_run=dry_run, layer=layer, table=table)
    results["source"] = stats

    # 3e. Related
    print("  [enrich] Running enrich_related...")
    import enrich_related

    stats = enrich_related.main(dry_run=dry_run, layer=layer, table=table)
    results["related"] = stats

    return results


def step_profile(
    layer: str, table: str, *, dry_run: bool = False, live: bool = False
) -> dict:
    """Run profile_columns for a single table (requires live BQ)."""
    if not live:
        print("  [profile] SKIP: requires --live flag")
        return {}

    print("  [profile] Running profile_columns...")
    import profile_columns

    return profile_columns.main(dry_run=dry_run, layer=layer, table=table)


# ---------------------------------------------------------------------------
# Step 5: Coverage check
# ---------------------------------------------------------------------------


def step_check_coverage(layer: str, table: str) -> dict:
    """Run coverage check for a single table."""
    print("  [coverage] Checking coverage...")
    from check_coverage import check_table_coverage

    result = check_table_coverage(layer, table)
    status = "PASS" if result["passed"] else "FAIL"
    cov = result["coverage"]
    print(f"  [coverage] {layer}/{table}: {status}")
    for field, val in cov.items():
        print(f"    {field}: {val}{'%' if val != 'N/A' else ''}")
    if result["gaps"]:
        print(f"    Gaps: {', '.join(result['gaps'])}")
    return result


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def onboard_single_table(
    layer: str,
    table: str,
    *,
    enrich_only: bool = False,
    dry_run: bool = False,
    live: bool = False,
) -> dict:
    """Run the full onboarding pipeline for a single table."""
    print(f"\n{'=' * 60}")
    print(f"Onboarding: {layer}/{table}")
    print(f"{'=' * 60}")

    results: dict = {"layer": layer, "table": table}

    # Step 1: Skeleton
    if not enrich_only:
        results["skeleton"] = step_generate_skeleton(
            layer, table, live=live, dry_run=dry_run
        )

        # Step 2: Update _dataset.yaml
        results["dataset_updated"] = step_update_dataset_yaml(
            layer, table, dry_run=dry_run
        )

    # Step 3: Enrichment
    results["enrichment"] = step_enrich(layer, table, dry_run=dry_run, live=live)

    # Step 4: Profile (live only)
    results["profile"] = step_profile(layer, table, dry_run=dry_run, live=live)

    # Step 5: Coverage
    results["coverage"] = step_check_coverage(layer, table)

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Table onboarding pipeline — generate, enrich, validate"
    )
    parser.add_argument("--layer", choices=["kpi", "data"], help="Table layer")
    parser.add_argument("--table", help="Table name")
    parser.add_argument(
        "--all", action="store_true", help="Process all registered tables"
    )
    parser.add_argument(
        "--enrich-only",
        action="store_true",
        help="Skip skeleton generation (re-enrich existing table)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report without writing")
    parser.add_argument(
        "--live", action="store_true", help="Use live BQ for schema/profiling"
    )
    args = parser.parse_args()

    if not args.all and not (args.layer and args.table):
        parser.error("Provide --layer and --table, or use --all")

    target_tables = ALL_TABLES if args.all else filter_tables(args.layer, args.table)

    all_results: list[dict] = []
    for layer, tables in target_tables.items():
        for table_name in tables:
            result = onboard_single_table(
                layer,
                table_name,
                enrich_only=args.enrich_only,
                dry_run=args.dry_run,
                live=args.live,
            )
            all_results.append(result)

    # Summary
    print(f"\n{'=' * 60}")
    print("Summary")
    print(f"{'=' * 60}")
    passed = sum(1 for r in all_results if r.get("coverage", {}).get("passed"))
    total = len(all_results)
    print(f"Tables processed: {total}")
    print(f"Coverage passed: {passed}/{total}")

    for r in all_results:
        status = "PASS" if r.get("coverage", {}).get("passed") else "FAIL"
        print(f"  {r['layer']}/{r['table']}: {status}")

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
