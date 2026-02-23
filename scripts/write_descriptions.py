#!/usr/bin/env python3
"""Write table-level descriptions into YAML catalog files that have description: "".

This script surgically replaces `description: ""` with actual descriptions
for all table types across catalog/data/*.yaml and catalog/*_data/*.yaml.
Descriptions are factual, 1-3 sentences, based on proto definitions and
the data pipeline documentation.
"""

import os
import re
import sys

# Base directory for the catalog
CATALOG_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "catalog"
)

# Table name -> description mapping
# Descriptions are 1-3 sentences, factual, mentioning data contents and pipeline role.
TABLE_DESCRIPTIONS: dict[str, str] = {
    # -------------------------------------------------------------------------
    # Core data layer tables (catalog/data/)
    # -------------------------------------------------------------------------
    "algostartup": (
        "Algo trading engine startup configuration events. Contains the initial "
        "parameters, portfolio assignments, and algorithm settings recorded when "
        "a trading algo instance starts up. Partitioned by trade_date."
    ),
    "brokertrade_2": (
        "Broker trade execution records in version 2 format, sourced from the "
        "OffFloorData proto (BrokerTrade message). Contains off-floor/OTC trade "
        "details including broker, counterparty, portfolio, leg fills, and "
        "instrument enrichment. Partitioned by trade_date."
    ),
    "eodpostradessnap": (
        "End-of-day post-trade position snapshots. Contains a snapshot of "
        "positions and trade summaries captured after the trading session closes. "
        "Partitioned by trade_date."
    ),
    "instrumentreceival": (
        "Instrument definition messages received from the exchange feed. Records "
        "when new instrument definitions (symbol, strike, expiry, contract size, "
        "etc.) are received and processed by the trading system. Partitioned by "
        "trade_date."
    ),
    "instruments": (
        "Master instrument reference data containing symbol, strike, expiry, "
        "option type, contract size, currency, and instrument hash. Used as the "
        "lookup table for instrument enrichment joins across all other data "
        "tables. Partitioned by trade_date."
    ),
    "livestats": (
        "Live trading statistics with running tallies captured during the trading "
        "session. Contains real-time aggregated metrics such as fill counts, "
        "volumes, and PnL snapshots updated throughout the day. Partitioned by "
        "trade_date."
    ),
    "mako_underlyingtrade": (
        "Mako's underlying hedge trade executions. Contains trades in the "
        "underlying instrument used to delta-hedge options positions, including "
        "price, size, and hedge context. Partitioned by trade_date."
    ),
    "marketdata_trades": (
        "Market data feed filtered to individual trade tick events only. A subset "
        "of the full marketdata stream containing only trade executions (not "
        "quote updates or book changes). Partitioned by trade_date."
    ),
    "marketdepth_snapshot": (
        "Point-in-time snapshots of the market depth order book, similar to "
        "marketdepth but captured at specific intervals rather than on every "
        "book change. Contains multi-level bid/ask prices and volumes. "
        "Partitioned by trade_date."
    ),
    "marketstate": (
        "Exchange market state transition events recording changes between "
        "trading phases (pre-open, open, auction, close, halt, etc.). Use for "
        "determining when instruments were tradeable. Partitioned by trade_date."
    ),
    "marketstate_open_close_times": (
        "Market session open and close time records derived from market state "
        "transitions. Provides the definitive open/close timestamps for each "
        "instrument or market segment on a given trading day. Partitioned by "
        "trade_date."
    ),
    "norm_strike_voldata": (
        "Normalized strike volatility surface data containing implied volatility "
        "values mapped to normalized strike coordinates. Used for volatility "
        "surface analysis and skew studies. Partitioned by trade_date."
    ),
    "pcaporder": (
        "PCAP-captured order messages from raw network packet capture. Contains "
        "order-level data extracted from network traffic for latency measurement "
        "and order flow reconstruction. Partitioned by trade_date."
    ),
    "posdata": (
        "Position data snapshots from the PositionEvent proto (PosData.proto). "
        "Contains position state records including instrument, portfolio, size, "
        "price, and settlement details. The tradedata and clicktrade tables are "
        "derived subsets of this data. Partitioned by trade_date."
    ),
    "quoterdelete": (
        "Quoter delete and cancel events recording when the auto-quoter removes "
        "or cancels resting quotes from the order book. Contains the instrument, "
        "side, timestamp, and reason for the deletion. Partitioned by trade_date."
    ),
    "quoterupdate": (
        "Quoter quote update events recording when the auto-quoter modifies "
        "resting quotes in the order book. Contains updated price, size, and "
        "the theoretical values driving the update. Partitioned by trade_date."
    ),
    "streammetadatamessage": (
        "Stream metadata messages containing connection and session information "
        "for the data streaming infrastructure. Records stream IDs, data source "
        "identifiers, and pipeline metadata. Partitioned by trade_date."
    ),
    "theodata_snapshot": (
        "Point-in-time snapshots of theoretical pricing data, similar to "
        "theodata but captured at specific intervals. Contains theo values, "
        "Greeks, and volatility parameters per instrument. Partitioned by "
        "trade_date."
    ),
    "tradedata": (
        "Raw trade execution data from the PositionEvent proto (PosData.proto). "
        "Contains all position and trade events including algorithm name, "
        "position type, and transaction details. This is the superset table "
        "from which the clicktrade KPI table is derived by filtering on "
        "algorithm and position type. Partitioned by trade_date."
    ),
    # -------------------------------------------------------------------------
    # Market-specific tables (catalog/*_data/)
    # -------------------------------------------------------------------------
    "brokertrade_data": (
        "Broker trade data records containing off-floor/OTC trade execution "
        "details. Similar to brokertrade but in a separate data format with "
        "broker, counterparty, and fill information. Partitioned by trade_date."
    ),
    "basevaldata": (
        "Base valuation data containing reference pricing parameters used for "
        "theoretical value calculations. Includes base bid/ask values, forward "
        "prices, and carry rates for valuation models. Partitioned by "
        "trade_date."
    ),
    "boshorder": (
        "Bosh order data capturing order submissions to the exchange. Contains "
        "order price, size, side, instrument details, and submission timestamps. "
        "Partitioned by trade_date."
    ),
    "boshorderupdate": (
        "Bosh order update events recording modifications to previously "
        "submitted orders. Contains updated price, size, and order state "
        "information. Partitioned by trade_date."
    ),
    "feedarbitrage": (
        "Feed arbitrage detection data capturing discrepancies between multiple "
        "market data feeds. Used for monitoring feed quality and detecting "
        "arbitrage opportunities across feed sources. Partitioned by trade_date."
    ),
    "instrumentmetadata": (
        "Instrument metadata attributes providing additional reference data "
        "beyond the core instruments table. Contains exchange-specific metadata "
        "fields for instrument classification and configuration. Partitioned by "
        "trade_date."
    ),
    "multilevel_data": (
        "Multi-level pricing data containing bid/ask prices and volumes across "
        "multiple order book levels. Provides a detailed view of market "
        "microstructure at various price levels. Partitioned by trade_date."
    ),
    "orderdeletenotificationevent": (
        "Order delete notification events from the exchange confirming that a "
        "previously submitted order has been deleted or cancelled. Contains "
        "order identifiers, timestamps, and cancellation reasons. Partitioned "
        "by trade_date."
    ),
    "orderfillevent": (
        "Order fill events from the exchange confirming partial or complete "
        "fills of submitted orders. Contains fill price, size, order "
        "identifiers, and exchange timestamps. Partitioned by trade_date."
    ),
    "orderposdata": (
        "Order position data combining order execution details with resulting "
        "position changes. Links order fill events to position snapshots for "
        "order-level position tracking. Partitioned by trade_date."
    ),
    "ordertransevent": (
        "Order transaction events recording the full lifecycle of order "
        "submissions, modifications, and acknowledgements from the exchange. "
        "Contains transaction type, order state, and exchange response details. "
        "Partitioned by trade_date."
    ),
    "pcapquote": (
        "PCAP-captured quote messages from raw network packet capture. Contains "
        "quote-level data extracted from network traffic for latency measurement "
        "and quote flow analysis. Partitioned by trade_date."
    ),
    "quoterpull": (
        "Quoter pull-back events recording when the auto-quoter temporarily "
        "withdraws quotes from the market, typically in response to adverse "
        "price movements or risk limits. Partitioned by trade_date."
    ),
    "oroswingdata": (
        "ORO (One-Ratio-One) swing pricing data from the OroSwingData proto. "
        "Contains swing trade opportunities routed by the ORO algorithm, "
        "including routed price/size, market price, fill volumes, and latency "
        "statistics. The KPI-layer otoswing table is derived from this data. "
        "Partitioned by trade_date."
    ),
    "marketdataext": (
        "Extended market data from the MarketDataExtended proto. Supplements "
        "the standard marketdata feed with additional fields including buyer/"
        "seller IDs, aggressor timestamps, trade exchange timestamps, and "
        "entering firm identifiers. Partitioned by trade_date."
    ),
    "kpibaseval": (
        "KPI base valuation reference data containing baseline pricing "
        "parameters used in KPI computations. Provides the reference values "
        "for edge and PnL calculations in the KPI layer. Partitioned by "
        "trade_date."
    ),
    "makotonuvamapcapdata": (
        "Mako-to-Nuvama PCAP data capturing network packets sent from Mako's "
        "trading infrastructure to the Nuvama broker gateway. Used for "
        "order-level latency analysis on the outbound path. Partitioned by "
        "trade_date."
    ),
    "nuvamatonsepcapdata": (
        "Nuvama-to-NSE PCAP data capturing network packets sent from the "
        "Nuvama broker gateway to the NSE exchange. Used for measuring "
        "broker-to-exchange latency on the outbound order path. Partitioned "
        "by trade_date."
    ),
}


def update_table_description(filepath: str, table_name: str, description: str) -> bool:
    """Replace the table-level description: "" with the actual description in a YAML file.

    Only replaces the FIRST occurrence of `description: ""` which is the table-level one
    (column-level descriptions also use `description: ""` but appear later and indented
    differently).

    Returns True if a replacement was made, False otherwise.
    """
    with open(filepath) as f:
        content = f.read()

    # Match the table-level description: ""
    # It appears early in the file, indented at 2 spaces (under `table:`)
    # Pattern: line starting with "  description: """
    # We only replace the FIRST match to avoid touching column-level descriptions
    pattern = r'^(  description: )""'
    replacement = rf'\1"{description}"'

    new_content, count = re.subn(
        pattern, replacement, content, count=1, flags=re.MULTILINE
    )

    if count == 0:
        return False

    with open(filepath, "w") as f:
        f.write(new_content)

    return True


def find_yaml_files_for_table(table_name: str) -> list[str]:
    """Find all YAML catalog files for a given table name.

    Searches in catalog/data/ and all catalog/*_data/ directories.
    """
    files = []

    # Check catalog/data/{table_name}.yaml
    data_path = os.path.join(CATALOG_DIR, "data", f"{table_name}.yaml")
    if os.path.isfile(data_path):
        files.append(data_path)

    # Check all exchange-specific directories: catalog/*_data/{table_name}.yaml
    for entry in sorted(os.listdir(CATALOG_DIR)):
        if entry.endswith("_data") and os.path.isdir(os.path.join(CATALOG_DIR, entry)):
            exchange_path = os.path.join(CATALOG_DIR, entry, f"{table_name}.yaml")
            if os.path.isfile(exchange_path):
                files.append(exchange_path)

    return files


def main() -> int:
    """Main entry point."""
    total_updated = 0
    total_skipped = 0
    total_missing = 0

    for table_name, description in sorted(TABLE_DESCRIPTIONS.items()):
        files = find_yaml_files_for_table(table_name)
        if not files:
            print(f"  WARNING: No YAML files found for table '{table_name}'")
            total_missing += 1
            continue

        for filepath in files:
            rel_path = os.path.relpath(filepath, os.path.dirname(CATALOG_DIR))
            if update_table_description(filepath, table_name, description):
                print(f"  UPDATED: {rel_path}")
                total_updated += 1
            else:
                print(
                    f"  SKIPPED: {rel_path} (already has description or pattern not found)"
                )
                total_skipped += 1

    print(
        f"\nSummary: {total_updated} updated, {total_skipped} skipped, {total_missing} missing"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
