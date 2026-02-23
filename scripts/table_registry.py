"""Centralized table registry — single source of truth for all table lists.

Every enrichment script, validation script, and orchestrator imports from here
instead of maintaining their own hardcoded ALL_TABLES dicts.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# OMX legacy layers (used by existing enrichment scripts and KPI pipeline)
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Multi-market table registry (all data-layer tables per exchange)
# ---------------------------------------------------------------------------

MARKET_TABLES: dict[str, list[str]] = {
    "omx_data": sorted(
        [
            "algostartup",
            "brokertrade_2",
            "clicktrade",
            "eodpostradessnap",
            "instrumentreceival",
            "instruments",
            "livestats",
            "mako_underlyingtrade",
            "marketdata",
            "marketdata_trades",
            "marketdepth",
            "marketdepth_snapshot",
            "marketstate",
            "marketstate_open_close_times",
            "markettrade",
            "norm_strike_voldata",
            "pcaporder",
            "posdata",
            "quoterdelete",
            "quotertrade",
            "quoterupdate",
            "streammetadatamessage",
            "swingdata",
            "theodata",
            "theodata_snapshot",
            "tradedata",
        ]
    ),
    "arb_data": sorted(
        [
            "algostartup",
            "brokertrade_2",
            "brokertrade_data",
            "clicktrade",
            "eodpostradessnap",
            "instrumentreceival",
            "instruments",
            "livestats",
            "mako_underlyingtrade",
            "marketdata",
            "marketdata_trades",
            "marketdepth",
            "marketdepth_snapshot",
            "markettrade",
            "norm_strike_voldata",
            "posdata",
            "quoterdelete",
            "quotertrade",
            "quoterupdate",
            "streammetadatamessage",
            "theodata",
            "theodata_snapshot",
            "tradedata",
        ]
    ),
    "asx_data": sorted(
        [
            "algostartup",
            "clicktrade",
            "eodpostradessnap",
            "instrumentreceival",
            "instruments",
            "livestats",
            "mako_underlyingtrade",
            "marketdata",
            "marketdata_trades",
            "marketdataext",
            "marketdepth",
            "marketdepth_snapshot",
            "marketstate",
            "marketstate_open_close_times",
            "markettrade",
            "norm_strike_voldata",
            "oroswingdata",
            "posdata",
            "quoterdelete",
            "quotertrade",
            "quoterupdate",
            "streammetadatamessage",
            "swingdata",
            "theodata",
            "theodata_snapshot",
            "tradedata",
        ]
    ),
    "brazil_data": sorted(
        [
            "algostartup",
            "basevaldata",
            "boshorder",
            "boshorderupdate",
            "brokertrade_2",
            "brokertrade_data",
            "clicktrade",
            "eodpostradessnap",
            "feedarbitrage",
            "instrumentmetadata",
            "instrumentreceival",
            "instruments",
            "livestats",
            "mako_underlyingtrade",
            "marketdata",
            "marketdata_trades",
            "marketdataext",
            "marketdepth",
            "marketdepth_snapshot",
            "marketstate",
            "marketstate_open_close_times",
            "markettrade",
            "multilevel_data",
            "norm_strike_voldata",
            "orderdeletenotificationevent",
            "orderfillevent",
            "orderposdata",
            "ordertransevent",
            "pcaporder",
            "posdata",
            "quoterdelete",
            "quoterpull",
            "quotertrade",
            "quoterupdate",
            "streammetadatamessage",
            "swingdata",
            "theodata",
            "theodata_snapshot",
            "tradedata",
        ]
    ),
    "eurex_data": sorted(
        [
            "algostartup",
            "brokertrade_2",
            "brokertrade_data",
            "clicktrade",
            "eodpostradessnap",
            "instrumentreceival",
            "instruments",
            "livestats",
            "mako_underlyingtrade",
            "marketdata",
            "marketdata_trades",
            "marketdataext",
            "marketdepth",
            "marketdepth_snapshot",
            "marketstate",
            "marketstate_open_close_times",
            "markettrade",
            "norm_strike_voldata",
            "oroswingdata",
            "pcaporder",
            "pcapquote",
            "posdata",
            "quoterdelete",
            "quotertrade",
            "quoterupdate",
            "streammetadatamessage",
            "swingdata",
            "theodata",
            "theodata_snapshot",
            "tradedata",
        ]
    ),
    "euronext_data": sorted(
        [
            "algostartup",
            "brokertrade_2",
            "clicktrade",
            "eodpostradessnap",
            "instrumentreceival",
            "instruments",
            "livestats",
            "mako_underlyingtrade",
            "marketdata",
            "marketdata_trades",
            "marketdataext",
            "marketdepth",
            "marketdepth_snapshot",
            "marketstate",
            "marketstate_open_close_times",
            "markettrade",
            "norm_strike_voldata",
            "orderdeletenotificationevent",
            "orderfillevent",
            "ordertransevent",
            "pcaporder",
            "pcapquote",
            "posdata",
            "quoterdelete",
            "quotertrade",
            "quoterupdate",
            "streammetadatamessage",
            "swingdata",
            "theodata",
            "theodata_snapshot",
            "tradedata",
        ]
    ),
    "ice_data": sorted(
        [
            "algostartup",
            "brokertrade_2",
            "brokertrade_data",
            "clicktrade",
            "eodpostradessnap",
            "instrumentreceival",
            "instruments",
            "livestats",
            "mako_underlyingtrade",
            "marketdata",
            "marketdata_trades",
            "marketdepth",
            "marketdepth_snapshot",
            "marketstate",
            "marketstate_open_close_times",
            "markettrade",
            "norm_strike_voldata",
            "oroswingdata",
            "posdata",
            "quoterdelete",
            "quoterpull",
            "quotertrade",
            "quoterupdate",
            "streammetadatamessage",
            "swingdata",
            "theodata",
            "theodata_snapshot",
            "tradedata",
        ]
    ),
    "korea_data": sorted(
        [
            "algostartup",
            "basevaldata",
            "clicktrade",
            "eodpostradessnap",
            "feedarbitrage",
            "instrumentreceival",
            "instruments",
            "livestats",
            "mako_underlyingtrade",
            "marketdata",
            "marketdata_trades",
            "marketdepth",
            "marketdepth_snapshot",
            "marketstate",
            "marketstate_open_close_times",
            "markettrade",
            "norm_strike_voldata",
            "pcaporder",
            "posdata",
            "quoterdelete",
            "quotertrade",
            "quoterupdate",
            "streammetadatamessage",
            "swingdata",
            "theodata",
            "theodata_snapshot",
            "tradedata",
        ]
    ),
    "nse_data": sorted(
        [
            "algostartup",
            "clicktrade",
            "eodpostradessnap",
            "instrumentreceival",
            "instruments",
            "kpibaseval",
            "livestats",
            "mako_underlyingtrade",
            "makotonuvamapcapdata",
            "markettrade",
            "nuvamatonsepcapdata",
            "posdata",
            "quoterdelete",
            "quotertrade",
            "quoterupdate",
            "streammetadatamessage",
            "swingdata",
            "theodata",
            "theodata_snapshot",
            "tradedata",
        ]
    ),
}

ALL_MARKETS = sorted(MARKET_TABLES.keys())

# All unique table names across all markets
ALL_UNIQUE_TABLES = sorted({t for tables in MARKET_TABLES.values() for t in tables})


def all_table_pairs() -> list[tuple[str, str]]:
    """Return flat list of (layer, table) tuples for iteration."""
    return [(layer, t) for layer, tables in ALL_TABLES.items() for t in tables]


def all_market_table_pairs() -> list[tuple[str, str]]:
    """Return flat list of (market, table) tuples across all markets."""
    return [(m, t) for m, tables in MARKET_TABLES.items() for t in tables]


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


def filter_market_tables(
    market: str | None = None, table: str | None = None
) -> dict[str, list[str]]:
    """Return a filtered copy of MARKET_TABLES based on optional market/table args."""
    if market and market not in MARKET_TABLES:
        msg = f"Unknown market '{market}'. Valid markets: {ALL_MARKETS}"
        raise ValueError(msg)

    result: dict[str, list[str]] = {}
    for mkt, tables in MARKET_TABLES.items():
        if market and mkt != market:
            continue
        if table:
            filtered = [t for t in tables if t == table]
            if filtered:
                result[mkt] = filtered
        else:
            result[mkt] = list(tables)

    if table and not result:
        msg = f"Table '{table}' not found in any market"
        raise ValueError(msg)

    return result
