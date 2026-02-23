"""Centralized table registry — single source of truth for all table lists.

Every enrichment script, validation script, and orchestrator imports from here
instead of maintaining their own hardcoded ALL_TABLES dicts.
"""

from __future__ import annotations

ALL_TABLES: dict[str, list[str]] = {
    "kpi": ["markettrade", "quotertrade", "brokertrade", "clicktrade", "otoswing"],
    "data": [
        "markettrade",
        "quotertrade",
        "clicktrade",
        "swingdata",
        "theodata",
        "marketdata",
        "marketdepth",
    ],
}

# KPI-only subset (used by enrich_formulas)
KPI_TABLES: list[str] = ALL_TABLES["kpi"]


def all_table_pairs() -> list[tuple[str, str]]:
    """Return flat list of (layer, table) tuples for iteration."""
    return [(layer, t) for layer, tables in ALL_TABLES.items() for t in tables]


def filter_tables(
    layer: str | None = None, table: str | None = None
) -> dict[str, list[str]]:
    """Return a filtered copy of ALL_TABLES based on optional layer/table args.

    Used by enrichment scripts to support --layer/--table CLI filtering.
    """
    if layer and layer not in ALL_TABLES:
        msg = f"Unknown layer '{layer}'. Valid layers: {list(ALL_TABLES.keys())}"
        raise ValueError(msg)

    result: dict[str, list[str]] = {}
    for lyr, tables in ALL_TABLES.items():
        if layer and lyr != layer:
            continue
        if table:
            filtered = [t for t in tables if t == table]
            if filtered:
                result[lyr] = filtered
        else:
            result[lyr] = list(tables)

    if table and not result:
        msg = f"Table '{table}' not found in any layer"
        raise ValueError(msg)

    return result
