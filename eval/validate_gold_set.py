"""Validate the gold standard evaluation set.

Checks:
- All 13 tables appear in expected_tables across the set
- No duplicate IDs
- All 9 categories present
- Required fields present per query
- {project} placeholder in all SQL

Usage:
    python eval/validate_gold_set.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from eval.run_eval import load_gold_queries

EXPECTED_TABLE_COUNT = 13
ALL_TABLES = {
    # KPI (5)
    "markettrade",
    "quotertrade",
    "brokertrade",
    "clicktrade",
    "otoswing",
    # Data (8) — some overlap in name with KPI but routed to different dataset
    "theodata",
    "marketdata",
    "marketdepth",
    "swingdata",
    # These appear in both KPI and Data: markettrade, quotertrade, clicktrade
    # but are counted once by name for table coverage
}

ALL_CATEGORIES = {
    "kpi_basic",
    "data_basic",
    "disambiguation",
    "union_all",
    "slippage_intervals",
    "broker_comparison",
    "special_cases",
    "complex_patterns",
    "edge_cases",
}

REQUIRED_QUERY_FIELDS = {
    "id",
    "question",
    "category",
    "expected_tables",
    "expected_dataset",
    "gold_sql",
    "complexity",
    "tags",
}


def validate_gold_set(queries: list[dict[str, Any]] | None = None) -> list[str]:
    """Validate the gold query set. Returns list of error strings (empty = valid)."""
    if queries is None:
        queries = load_gold_queries()

    errors: list[str] = []

    # Check query count
    if len(queries) < 50:
        errors.append(f"Expected at least 50 queries, got {len(queries)}")

    # Check for duplicate IDs
    ids = [q.get("id", "") for q in queries]
    seen = set()
    for qid in ids:
        if qid in seen:
            errors.append(f"Duplicate query ID: {qid}")
        seen.add(qid)

    # Check required fields
    for q in queries:
        qid = q.get("id", "???")
        missing = REQUIRED_QUERY_FIELDS - set(q.keys())
        if missing:
            errors.append(f"{qid}: missing fields {missing}")

        # Check {project} placeholder in SQL
        sql = q.get("gold_sql", "")
        if "{project}" not in sql:
            errors.append(f"{qid}: gold_sql missing {{project}} placeholder")

        # Check category is valid
        cat = q.get("category", "")
        if cat not in ALL_CATEGORIES:
            errors.append(f"{qid}: unknown category '{cat}'")

    # Check all categories covered
    found_categories = {q.get("category") for q in queries}
    missing_categories = ALL_CATEGORIES - found_categories
    if missing_categories:
        errors.append(f"Missing categories: {missing_categories}")

    # Check all tables covered
    found_tables: set[str] = set()
    for q in queries:
        for t in q.get("expected_tables", []):
            found_tables.add(t)
    missing_tables = ALL_TABLES - found_tables
    if missing_tables:
        errors.append(f"Missing tables in expected_tables: {missing_tables}")

    return errors


def main():
    queries = load_gold_queries()
    print(f"Loaded {len(queries)} gold queries")

    errors = validate_gold_set(queries)

    if errors:
        print(f"\nValidation FAILED with {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        # Summary
        categories = {}
        tables: set[str] = set()
        for q in queries:
            cat = q["category"]
            categories[cat] = categories.get(cat, 0) + 1
            for t in q["expected_tables"]:
                tables.add(t)

        print(f"\nValidation PASSED")
        print(f"  Queries: {len(queries)}")
        print(f"  Categories ({len(categories)}):")
        for cat, count in sorted(categories.items()):
            print(f"    {cat}: {count}")
        print(f"  Tables covered: {len(tables)}")
        for t in sorted(tables):
            print(f"    {t}")
        sys.exit(0)


if __name__ == "__main__":
    main()
