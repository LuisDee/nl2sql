#!/usr/bin/env python3
"""Generate and apply column-level descriptions for table YAMLs in the catalog.

Uses a 3-tier strategy:
  Tier 1: Copy from enriched OMX/KPI equivalents (highest quality)
  Tier 2: Build from proto field comments + data loader transform context
  Tier 3: Name-pattern heuristic for remaining columns

Usage:
    python scripts/enrich_descriptions.py                    # kpi + data layers only
    python scripts/enrich_descriptions.py --all-markets      # all layers + markets
    python scripts/enrich_descriptions.py --layer arb_data   # one layer/market
    python scripts/enrich_descriptions.py --table instruments # one table type
    python scripts/enrich_descriptions.py --dry-run           # report without writing
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

import yaml
from table_registry import (
    ALL_TABLES,
    MARKET_TABLES,
    filter_combined_tables,
    filter_tables,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATALOG_DIR = PROJECT_ROOT / "catalog"
METADATA_DIR = PROJECT_ROOT / "metadata"

# Reference directories containing enriched column metadata
OMX_DATA_DIR = CATALOG_DIR / "data"
KPI_DIR = CATALOG_DIR / "kpi"

# Column-level fields to copy from OMX/KPI reference (Tier 1)
COPY_FIELDS = ["description", "source", "synonyms", "business_rules", "related_columns"]


# ---------------------------------------------------------------------------
# Tier 1: OMX/KPI Reference Builder
# ---------------------------------------------------------------------------


def build_omx_reference() -> dict[str, dict[str, dict[str, Any]]]:
    """Build mapping of (table_base_name, column_name) -> column metadata.

    Reads enriched YAMLs from catalog/data/ and catalog/kpi/ to serve as
    reference for matching columns in market-specific YAMLs.

    KPI tables are included because some columns (like KPI-specific metrics)
    only appear in the KPI layer. Data-layer columns take priority when both
    exist (data descriptions tend to be more specific about the raw data).
    """
    reference: dict[str, dict[str, dict[str, Any]]] = {}

    for ref_dir in [OMX_DATA_DIR, KPI_DIR]:
        if not ref_dir.exists():
            continue
        for yaml_path in sorted(ref_dir.glob("*.yaml")):
            if yaml_path.name.startswith("_"):
                continue
            data = yaml.safe_load(yaml_path.read_text())
            table = data.get("table", {})
            table_name = table.get("name", yaml_path.stem)

            if table_name not in reference:
                reference[table_name] = {}

            for col in table.get("columns", []):
                col_name = col["name"]
                desc = col.get("description", "")
                if not desc or str(desc).strip() == "":
                    continue  # skip empty descriptions in reference

                col_meta: dict[str, Any] = {}
                for field in COPY_FIELDS:
                    if col.get(field):
                        val = col[field]
                        # Skip empty values
                        if isinstance(val, str) and val.strip() == "":
                            continue
                        if isinstance(val, list) and len(val) == 0:
                            continue
                        col_meta[field] = val

                # Only store if we have a description
                if "description" in col_meta:
                    # Data-layer overwrites KPI-layer for same column
                    if col_name not in reference[table_name]:
                        reference[table_name][col_name] = col_meta
                    elif ref_dir == OMX_DATA_DIR:
                        # Data dir takes precedence
                        reference[table_name][col_name] = col_meta

    return reference


# ---------------------------------------------------------------------------
# Tier 2: Proto/Transform Mapping
# ---------------------------------------------------------------------------


def _load_proto_fields() -> dict[str, list[dict[str, Any]]]:
    """Load proto_fields.yaml and return {message_name: [field_dicts]}."""
    path = METADATA_DIR / "proto_fields.yaml"
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text())
    result: dict[str, list[dict[str, Any]]] = {}
    for msg in data.get("messages", []):
        result[msg["name"]] = msg.get("fields", [])
    return result


def _load_proto_to_bq() -> dict[str, dict[str, Any]]:
    """Load the proto_to_bq section from proto_fields.yaml."""
    path = METADATA_DIR / "proto_fields.yaml"
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text())
    return data.get("proto_to_bq", {})


def build_proto_description_map(
    proto_messages: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """Build mapping from proto field name -> {comment, type, message_name}.

    Handles VtCommon embedding: VtCommon fields are indexed under their own
    names and will be used for columns that come from the props.* path.
    """
    result: dict[str, dict[str, Any]] = {}

    for msg_name, fields in proto_messages.items():
        for field in fields:
            fname = field["name"]
            comment = field.get("comment", "").strip()
            if not comment:
                continue  # no description to use
            entry = {
                "comment": comment,
                "type": field.get("type", ""),
                "message": msg_name,
            }
            # VtCommon fields used across many tables, store with message context
            if fname not in result:
                result[fname] = entry
            elif msg_name == "VtCommon":
                # VtCommon is the canonical source for shared fields
                result[fname] = entry

    return result


def _load_transforms() -> dict[str, list[dict[str, Any]]]:
    """Load data_loader_transforms.yaml and return {table_name: [column_dicts]}."""
    path = METADATA_DIR / "data_loader_transforms.yaml"
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text())
    result: dict[str, list[dict[str, Any]]] = {}
    for table in data.get("tables", []):
        result[table["name"]] = table.get("columns", [])
    return result


def build_transform_map(
    transforms: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Build mapping of (table_name, bq_column) -> transform info.

    Transform info includes source_field, transformation type, and the
    extracted proto field name (last component of dot-separated source).
    """
    result: dict[str, dict[str, dict[str, Any]]] = {}
    for table_name, columns in transforms.items():
        table_map: dict[str, dict[str, Any]] = {}
        for col in columns:
            bq_name = col["name"]
            source_field = col.get("source_field", "")
            transformation = col.get("transformation", "")

            # Extract the proto field name from dotted path
            proto_field = source_field
            if "." in proto_field:
                parts = proto_field.split(".")
                proto_field = parts[-1]

            # Strip _name suffix for enum name extractions
            clean_proto_field = proto_field
            if clean_proto_field.endswith("_name"):
                clean_proto_field = clean_proto_field[: -len("_name")]
            if clean_proto_field.endswith("_ns"):
                # Nanosecond variant: the base field is the one without _ns
                clean_proto_field = clean_proto_field[: -len("_ns")]

            table_map[bq_name] = {
                "source_field": source_field,
                "transformation": transformation,
                "proto_field": proto_field,
                "proto_field_clean": clean_proto_field,
                "notes": col.get("notes", ""),
            }
        result[table_name] = table_map

    return result


def _snake_to_camel(name: str) -> str:
    """Convert snake_case to camelCase for proto field lookup."""
    parts = name.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


def _build_tier2_description(
    col_name: str,
    col_type: str,
    table_name: str,
    transform_map: dict[str, dict[str, dict[str, Any]]],
    proto_desc_map: dict[str, dict[str, Any]],
    proto_to_bq: dict[str, dict[str, Any]],
) -> str | None:
    """Try to build a description from proto comment + transform context."""
    # Find the transform entry for this column
    t_info = None
    for tbl in transform_map:
        if tbl == table_name and col_name in transform_map[tbl]:
            t_info = transform_map[tbl][col_name]
            break

    proto_field = None
    proto_field_clean = None
    source_field = None
    transformation = None
    is_vtcommon = False

    if t_info is not None:
        proto_field = t_info["proto_field"]
        proto_field_clean = t_info["proto_field_clean"]
        source_field = t_info["source_field"]
        transformation = t_info["transformation"]
        is_vtcommon = source_field.startswith("props.")

        # Skip NULL/hardcoded fields
        if source_field == "NULL":
            return None

        # Skip kafka infrastructure — they get a pattern-based description (Tier 3)
        if transformation == "direct" and source_field == col_name:
            return None
    else:
        # No transform entry: try to match by converting snake_case to camelCase
        # This handles tables not in data_loader_transforms (algostartup, etc.)
        camel = _snake_to_camel(col_name)
        # Also try stripping _name / _ns suffixes
        camel_clean = camel
        if col_name.endswith("_name"):
            camel_clean = _snake_to_camel(col_name[:-5])
        elif col_name.endswith("_ns"):
            camel_clean = _snake_to_camel(col_name[:-3])

        # Check if there's a proto field match
        candidates = [camel, camel_clean, col_name]
        found = False
        for c in candidates:
            if c in proto_desc_map:
                proto_field = c
                proto_field_clean = c
                found = True
                break
        if not found:
            return None

    # Look up proto comment
    proto_comment = None
    for field_name in [proto_field, proto_field_clean]:
        if field_name and field_name in proto_desc_map:
            proto_comment = proto_desc_map[field_name]["comment"]
            break

    if not proto_comment:
        return None

    # Get the proto message context
    bq_info = proto_to_bq.get(table_name, {})
    message_name = bq_info.get("message", "")

    # Build a concise description from the proto comment
    desc = proto_comment

    # Capitalize first letter
    if desc and desc[0].islower():
        desc = desc[0].upper() + desc[1:]

    # Ensure it ends with a period
    if desc and not desc.endswith("."):
        desc = desc + "."

    # Add context about the source
    if is_vtcommon:
        desc += " From VtCommon proto (shared across trade tables)."
    elif message_name:
        desc += f" From {message_name} proto."

    # Add nanosecond precision note for _ns columns
    if col_name.endswith("_ns"):
        base_col = col_name[:-3]
        desc = f"Nanosecond-precision component of {base_col.replace('_', ' ')}. {desc}"

    # Add enum/human-readable name note for _name columns
    if col_name.endswith("_name"):
        is_enum_note = t_info and t_info.get("notes", "").startswith("enum")
        # If description was inherited from the base field (without _name),
        # add context that this is the human-readable name
        if is_enum_note or (proto_field and proto_field == proto_field_clean):
            desc = desc.rstrip(".") + " (human-readable enum name)."
        else:
            desc = f"Human-readable name for: {desc}"

    return desc


# ---------------------------------------------------------------------------
# Tier 3: Name-Pattern Heuristic
# ---------------------------------------------------------------------------

# Map of column name patterns to description generators
_PATTERN_DESCRIPTIONS: list[tuple[str, str]] = [
    # Kafka infrastructure
    (
        "kafka_message_timestamp",
        "Kafka ingestion timestamp recording when this message was produced to the Kafka topic. Used internally for data pipeline monitoring and replay.",
    ),
    (
        "record_written_timestamp",
        "Timestamp when this record was written to the data warehouse by the data pipeline. Used for tracking data freshness and pipeline latency.",
    ),
    (
        "kafka_partition",
        "Kafka partition number from which this record was consumed. Used internally for data pipeline tracking and replay.",
    ),
    (
        "kafka_offset",
        "Kafka offset of the record within its partition. Used for exactly-once processing guarantees and data pipeline replay.",
    ),
    (
        "partition_timestamp_local",
        "Local-timezone timestamp of the data partition. Used for partition-level data management in the pipeline.",
    ),
    (
        "partition_number",
        "Numeric partition identifier within the data pipeline. Used for data distribution and parallel processing.",
    ),
    # Common identifiers
    (
        "instrument_hash",
        "Unique SHA256 hash identifier for the instrument, deterministically computed from the instrument defining attributes. Used as the primary join key across all data and KPI tables.",
    ),
    (
        "trade_date",
        "Trading session date used as the partition column. Always filter on this field for efficient queries.",
    ),
    # Common VtCommon fields
    ("portfolio", "Name of the Mako portfolio to which the trading algorithm belongs."),
    ("algo", "Name of the originating trading algorithm or strategy."),
    ("symbol", "Mako symbol identifier for the traded instrument."),
    # Instrument enrichment columns
    (
        "mako_symbol",
        "Mako-internal symbol identifier, enriched from the instruments reference table via instrument_hash join.",
    ),
    (
        "currency",
        "Currency code for the instrument, enriched from the instruments reference table.",
    ),
    (
        "inst_type_name",
        "Human-readable instrument type name (e.g. option, future, stock), enriched from the instruments reference table.",
    ),
    (
        "term",
        "Expiry term/maturity label for the instrument, enriched from the instruments reference table.",
    ),
    (
        "strike",
        "Strike price for options, enriched from the instruments reference table. NULL for non-option instruments.",
    ),
    (
        "option_type",
        "Option type code (C=Call, P=Put), enriched from the instruments reference table. NULL for non-option instruments.",
    ),
    (
        "option_type_name",
        "Human-readable option type name (Call/Put), enriched from the instruments reference table.",
    ),
    (
        "expiry_timestamp",
        "Expiry timestamp for the instrument, enriched from the instruments reference table.",
    ),
    # Common timestamp fields
    (
        "event_timestamp",
        "Timestamp of the event, recording when the VT processed this event.",
    ),
    (
        "exchange_timestamp",
        "Exchange-provided timestamp of when the event occurred on the exchange.",
    ),
    (
        "hardware_nic_rx_timestamp",
        "Hardware NIC receive timestamp, recording when the price packet arrived at the network card. Used for precise latency measurement.",
    ),
    # Common exchange/trade fields
    (
        "underlying_exe_exch",
        "Numeric exchange code where the underlying instrument trades.",
    ),
    (
        "base_valuation_type",
        "Numeric code for the base valuation method used by the pricing model.",
    ),
    ("message_trade_date", "Trade date embedded within the original proto message."),
    (
        "edge",
        "Edge (theoretical profit) seen on this trade, computed as the difference between theo value and trade price.",
    ),
    (
        "contract_size",
        "Number of units per contract lot. Streamed from TradableInstrument.contractSize proto field.",
    ),
    (
        "raw_proto",
        "Raw serialized protobuf bytes of the original message. Used for debugging and replay.",
    ),
    ("pid", "Process ID of the data source that produced this record."),
    ("hostname", "Hostname of the data source that produced this record."),
    # PositionEvent / trade management fields
    ("mako_id", "Mako-internal unique identifier for this trade or order."),
    ("algorithm", "Name of the Mako algorithm that generated this trade or event."),
    ("order_id", "Mako-internal order identifier linking fills to the parent order."),
    (
        "fill_id",
        "Unique identifier for this fill (partial or full execution) within an order.",
    ),
    ("transaction_id", "Mako-internal transaction identifier for this event."),
    (
        "exchange_transaction_id",
        "Exchange-assigned transaction identifier for this event.",
    ),
    (
        "position_type",
        "Numeric code classifying the position event type (e.g. trade, adjustment, exercise).",
    ),
    (
        "position_id",
        "Identifier for the position this event belongs to within the portfolio.",
    ),
    ("traded_size", "Number of lots traded in this transaction."),
    (
        "total_traded_size",
        "Cumulative number of lots traded across all fills for this order.",
    ),
    (
        "company_position",
        "Net company-wide position for this instrument after this event.",
    ),
    ("settlement", "Settlement price for the instrument."),
    ("settlement_date", "Date on which settlement occurs for this trade."),
    ("value_date", "Value date for the trade, relevant for settlement calculations."),
    ("cash_value", "Cash value of this trade or position change."),
    ("pos_value_currency", "Currency in which the position value is denominated."),
    ("agg_pos_date", "Aggregated position date for end-of-day processing."),
    (
        "max_trade_id",
        "Maximum trade ID in the batch, used for ordering and deduplication.",
    ),
    ("ectv", "Exchange-cleared theoretical value used for settlement calculations."),
    ("routed_exchange", "Exchange to which this order was routed for execution."),
    ("executable_exchange", "Exchange on which this instrument is executable."),
    ("channel", "Data channel or feed identifier for this event."),
    ("trade_id", "Exchange-assigned trade identifier."),
    ("exchange_date", "Date as reported by the exchange for this event."),
    (
        "event_date",
        "Date of the event, may differ from trade_date for overnight sessions.",
    ),
    # Common trading fields
    ("price", "Price of the trade or order."),
    ("size", "Number of lots or contracts in the trade or order."),
    ("counterparty", "Counterparty identifier for this trade."),
    ("broker", "Broker identifier associated with this trade."),
    ("inventory", "Inventory account associated with this trade or position."),
    ("account", "Account identifier for this trade."),
    ("source", "Source system or feed that originated this record."),
    ("trader_id", "Identifier of the trader responsible for this trade."),
    (
        "user_order_id",
        "User-submitted order identifier linking back to the order entry system.",
    ),
    ("exchange_info", "Additional exchange-provided information for this event."),
    (
        "exchange_qualifier",
        "Exchange qualifier code providing additional event context.",
    ),
    ("statistics", "Statistical summary or classification data for this event."),
    (
        "text",
        "Free-text field containing additional descriptive information about this event.",
    ),
    ("revisor", "Identifier of the user or process that last revised this record."),
    ("revision", "Revision number tracking modifications to this record."),
    # Greeks and pricing
    ("vol", "Implied volatility of the instrument at the time of this event."),
    (
        "delta",
        "Option delta (rate of change of option price with respect to underlying price).",
    ),
    (
        "gamma",
        "Option gamma (rate of change of delta with respect to underlying price).",
    ),
    (
        "vega",
        "Option vega (sensitivity of option price to changes in implied volatility).",
    ),
    (
        "theta",
        "Option theta (time decay, rate of change of option price with respect to time).",
    ),
    ("rho", "Option rho (sensitivity of option price to changes in interest rates)."),
    ("under", "Price of the underlying instrument at the time of this event."),
    ("tte", "Time to expiry in years for the instrument."),
    ("fwd", "Forward price of the underlying instrument."),
    (
        "curve_id",
        "Identifier for the volatility/pricing curve used in theoretical calculations.",
    ),
    (
        "carry_id",
        "Identifier for the carry/interest rate curve used in theoretical calculations.",
    ),
    ("liquid", "Liquidity indicator or classification for the instrument."),
    (
        "deflection_scale",
        "Scale factor for the deflection adjustment applied to pricing.",
    ),
    # Instrument fields
    ("no_legs", "Number of legs in a combo/spread instrument."),
    ("ratio", "Leg ratio within a combo instrument, indicating relative weighting."),
    ("inst_type", "Numeric instrument type code."),
    ("exercise_type", "Exercise type for options (European, American, etc.)."),
    ("expiry_timezone", "Timezone of the instrument expiry."),
    ("issue_date", "Issue date for bonds or newly listed instruments."),
    ("coupon", "Coupon rate for bond instruments."),
    (
        "has_underlying",
        "Boolean indicating whether this instrument has an underlying reference.",
    ),
    (
        "payment_type",
        "Payment type for the instrument (physical, cash settlement, etc.).",
    ),
    ("pricing_count", "Number of pricing inputs available for this instrument."),
    # Quoter fields
    ("delete_side", "Side (BID/ASK) of the quote that was deleted."),
    ("delete_type", "Type classification of the quote deletion event."),
    (
        "trigger_type",
        "Type of trigger that caused the quote action (price move, risk event, etc.).",
    ),
    ("trigger_price", "Price level that triggered the quote action."),
    ("active_price", "Current active price of the resting quote before the action."),
    ("active_size", "Current active size of the resting quote before the action."),
    ("update_type", "Type classification of the quote update event."),
    (
        "pull_restriction",
        "Pull restriction level indicating the severity of quote withdrawal.",
    ),
    ("bucket", "Bucket or group identifier for the quoter event."),
    ("max_placable", "Maximum placeable volume for the quoter at this level."),
    ("currently_placed", "Volume currently placed by the quoter at this level."),
    ("want_to_place", "Volume the quoter algorithm wants to place."),
    ("refill_pct_rate", "Refill percentage rate for quote replenishment."),
    ("max_edge_loss", "Maximum edge loss threshold for the BOSH order."),
    ("edge_loss", "Realized edge loss on this order."),
    ("edge_units", "Edge measurement units for this order."),
    ("atm_vega", "At-the-money vega used for order sizing."),
    ("base_atm_size", "Base ATM size parameter for order sizing."),
    ("max_size_multiplier", "Maximum size multiplier for order scaling."),
    ("order_option", "Order option type or configuration."),
    (
        "resting_time_limit_nanos",
        "Time limit in nanoseconds for how long the order may rest.",
    ),
    ("side", "Side of the order or quote: BID (buy) or ASK (sell)."),
    ("action", "Action type for order lifecycle events (new, modify, cancel, fill)."),
    ("cancel_reason", "Reason code for order cancellation."),
    ("residual_volume", "Remaining unfilled volume on the order."),
    # PCAP / Network fields
    ("source_ip", "Source IP address of the captured network packet."),
    ("source_port", "Source port of the captured network packet."),
    ("destination_ip", "Destination IP address of the captured network packet."),
    ("destination_port", "Destination port of the captured network packet."),
    ("message_type", "Message type classification for the captured packet."),
    (
        "normalised_message_type",
        "Normalised message type for cross-exchange comparison.",
    ),
    ("native_type", "Exchange-native message type code."),
    ("channel_id", "Channel identifier for the data feed or connection."),
    ("connection_kind", "Type of connection (TCP, UDP, multicast, etc.)."),
    ("connection_feed", "Feed identifier for the market data connection."),
    ("host", "Hostname of the network endpoint."),
    ("port", "Port number of the network endpoint."),
    ("spark_event_type", "Spark algorithm event type classification."),
    ("native_event_type", "Exchange-native event type code."),
    ("broker_name", "Name of the broker for this event."),
    ("order_identifier", "Exchange or broker order identifier."),
    ("exchange_order_id", "Exchange-assigned order identifier."),
    ("mako_order_id", "Mako-internal order identifier for PCAP correlation."),
    ("pcap_id", "PCAP correlation identifier linking to raw network captures."),
    ("batch_id", "Batch identifier grouping related events together."),
    ("dealer_id", "Dealer identifier at the exchange."),
    # Market state fields
    (
        "market_state",
        "Numeric code representing the current market state (pre-open, open, auction, halt, close, etc.).",
    ),
    (
        "instrument_type",
        "Type classification of the instrument for market state purposes.",
    ),
    (
        "data_order_identifier",
        "Data ordering identifier for sequencing market state updates.",
    ),
    (
        "keyframe",
        "Boolean indicating whether this event is a keyframe (full state snapshot vs. incremental).",
    ),
    ("open_timestamp", "Timestamp when the market opened for trading on this date."),
    ("close_timestamp", "Timestamp when the market closed for trading on this date."),
    # Vol surface fields
    ("rec_date", "Recording date for the volatility surface snapshot."),
    ("vol_region", "Volatility region or surface zone identifier."),
    # Latency stats fields
    (
        "is_breached",
        "Boolean indicating whether market-making compliance was breached.",
    ),
    ("num_of_breaches", "Number of market-making compliance breaches so far today."),
    ("max_breach_permitted", "Maximum number of breaches permitted before penalty."),
    (
        "requirement_check_timestamp",
        "Timestamp of the last market-making requirement check.",
    ),
    # Stream metadata
    ("stream_id", "Unique identifier for the data stream."),
    ("event_type", "Type classification of the streaming event."),
    # Brazil-specific
    (
        "is_market_maker_instrument",
        "Boolean indicating whether this instrument is designated for market-making.",
    ),
    ("level", "Price level index in the order book."),
    ("market_price_level", "Market price at this order book level."),
    ("volume_ahead_insert", "Volume ahead of our order at insertion time."),
    ("volume_ahead_tob", "Volume ahead of our order relative to top-of-book."),
    ("volume_behind_tob", "Volume behind our order relative to top-of-book."),
    ("our_pos", "Our queue position at this price level in the order book."),
    ("order_book_side", "Side of the order book (BID/ASK) for this position."),
    (
        "market_event_type",
        "Type classification of the market event that triggered this update.",
    ),
    ("delta_bucket", "Delta bucket classification for this instrument."),
    ("reason", "Reason code for this order or event action."),
    ("reason_name", "Human-readable name for the reason code."),
    ("client_id", "Client identifier at the exchange."),
    ("session_group", "Session group identifier for exchange connectivity."),
    ("session", "Session identifier for exchange connectivity."),
    ("client_order_id", "Client-side order identifier sent to the exchange."),
    ("exch_order_id", "Exchange-assigned order identifier."),
    ("exch_fill_id", "Exchange-assigned fill identifier."),
    ("trans_id", "Transaction identifier at the exchange."),
    ("order_side", "Side of the order (BUY/SELL)."),
    ("order_type", "Type of order (limit, market, etc.)."),
    ("total_volume", "Total volume of the order."),
    ("resid_volume", "Residual (remaining unfilled) volume of the order."),
    ("result_code", "Result code from the exchange for this order transaction."),
    ("result_info", "Additional result information from the exchange."),
    ("result_tag", "Result tag or classification from the exchange."),
    ("leg_head_type", "Leg head type for combo order fills."),
    ("trade_time", "Exchange trade time for this fill."),
    ("worst_price", "Worst acceptable price for this BOSH order."),
    ("largest_size", "Largest fill size seen for this BOSH order."),
    # NSE-specific
    (
        "option_underlying",
        "Underlying instrument value for option base value calculation.",
    ),
    ("option_roll", "Roll value for the option underlying."),
    (
        "option_underlying_is_valid",
        "Boolean indicating whether the option underlying value is valid.",
    ),
    ("option_term", "Option term/maturity for base value calculation."),
    ("base_term", "Base term for value calculation."),
    ("option_underlying_type", "Type of the option underlying instrument."),
    (
        "update_instrument_hash",
        "Instrument hash of the market data update that triggered this value.",
    ),
    ("update_sequence_number", "Sequence number of the triggering market data update."),
    (
        "nnf_message_type",
        "NNF (Nuvama Native Format) message type for NSE order tracking.",
    ),
    ("trader_id", "Trader identifier at the exchange."),
    # Single-word columns that the catch-all won't cover (no underscore)
    (
        "forward",
        "Forward price of the underlying instrument used in theoretical calculations.",
    ),
    ("timestamp", "Timestamp of this event or record."),
    ("label", "Label or tag identifying the type of this record."),
    ("component", "System component name that generated this event."),
    ("uuid", "Universally unique identifier for this record."),
    ("user", "User identifier associated with this trade or action."),
    ("company", "Company or firm identifier for this trade."),
    ("note", "Free-text note or annotation attached to this record."),
    ("trader", "Trader name or identifier responsible for this action."),
    ("payload", "Raw message payload for debugging or replay purposes."),
    ("quantity", "Quantity of the trade or order in number of lots."),
    ("summary", "Nested summary record containing aggregated event statistics."),
    (
        "metadata",
        "Nested metadata record containing stream identification and entity scope.",
    ),
    (
        "sequence_number",
        "Monotonically increasing sequence number for ordering events.",
    ),
    (
        "is_snap",
        "Boolean indicating whether this event is a snapshot (full state) or incremental update.",
    ),
    ("data_timestamp", "Timestamp from the data feed for this event."),
    ("lvol", "Log-transformed volatility value used in internal pricing calculations."),
    ("norms", "Normalized parameter values for the pricing model."),
    ("mode", "Operating mode of the algorithm or pricing model."),
    ("volume", "Volume (number of lots) for this fill or order event."),
    # VtCommon shared fields with better descriptions
    (
        "is_spark",
        "Boolean flag indicating whether the VT is using a sparkOrder interface.",
    ),
    (
        "is_bv_invalid",
        "Boolean flag indicating whether the base valuation was invalid at the time of this event.",
    ),
    (
        "is_ref_theo_invalid",
        "Boolean flag indicating whether the reference theoretical values were invalid at the time of this event.",
    ),
    (
        "lb_enabled",
        "Boolean flag indicating whether the layered base feature was enabled.",
    ),
    (
        "edge_model_type",
        "Numeric code for the edge computation model type used by the VT.",
    ),
    (
        "bv_side",
        "Side (BUY/SELL) of the base valuation the VT is referencing for pricing.",
    ),
    (
        "tv",
        "Theoretical value the VT computed for this instrument at the time of the event.",
    ),
    ("fees", "Per-lot trading fees applicable to this instrument."),
    (
        "roll",
        "Roll value at the time of the event, representing cost of carry adjustments.",
    ),
]


def _camel_to_words(name: str) -> str:
    """Convert camelCase to space-separated words."""
    s = re.sub(r"([A-Z])", r" \1", name)
    return s.strip().lower()


def _snake_to_words(name: str) -> str:
    """Convert snake_case to space-separated words."""
    return name.replace("_", " ").strip()


def generate_tier3_description(col_name: str, col_type: str) -> str | None:
    """Generate a description from naming patterns when Tier 1 and 2 fail."""
    # Exact match patterns
    for pattern_name, desc in _PATTERN_DESCRIPTIONS:
        if col_name == pattern_name:
            return desc

    # Vol surface strike bucket patterns: strike_minus_N, strike_plus_N, strike_atm_N
    strike_match = re.match(r"^strike_(minus|plus|atm)_(\d+)$", col_name)
    if strike_match:
        direction = strike_match.group(1)
        offset = strike_match.group(2)
        if direction == "atm":
            return f"Strike level at ATM offset {offset} on the normalized volatility surface."
        sign = "-" if direction == "minus" else "+"
        return f"Strike level at {sign}{offset} normalized offset from ATM on the volatility surface."

    # Implied vol surface: ivol_minus_N, ivol_plus_N, ivol_atm_N
    ivol_match = re.match(r"^ivol_(minus|plus|atm)_(\d+)$", col_name)
    if ivol_match:
        direction = ivol_match.group(1)
        offset = ivol_match.group(2)
        if direction == "atm":
            return f"Implied volatility at ATM offset {offset} on the normalized strike surface."
        sign = "-" if direction == "minus" else "+"
        return (
            f"Implied volatility at {sign}{offset} normalized strike offset from ATM."
        )

    # Smoothed vol surface: svol_minus_N, svol_plus_N, svol_atm_N
    svol_match = re.match(r"^svol_(minus|plus|atm)_(\d+)$", col_name)
    if svol_match:
        direction = svol_match.group(1)
        offset = svol_match.group(2)
        if direction == "atm":
            return f"Smoothed volatility at ATM offset {offset} on the normalized strike surface."
        sign = "-" if direction == "minus" else "+"
        return (
            f"Smoothed volatility at {sign}{offset} normalized strike offset from ATM."
        )

    # Underlying_* fields (instrument reference, not VtCommon ref_*)
    if col_name.startswith("underlying_") and not col_name.startswith(
        "underlying_exe_exch"
    ):
        rest = col_name[11:].replace("_", " ")
        return f"Underlying instrument {rest}, mirrored from the parent instrument definition."

    # Latency stat fields: latency_stats_*
    latency_match = re.match(r"^latency_stats?_(\w+)$", col_name)
    if latency_match:
        component = latency_match.group(1).replace("_", " ")
        return f"Latency statistics for the {component} processing stage (nanoseconds)."

    # Gateway/matching engine timestamps
    gw_match = re.match(
        r"^(gateway|matching_engine|rms|adapter|internal_server)_(received|ingress|egress|rx|tx)_timestamp(_ns)?$",
        col_name,
    )
    if gw_match:
        component = gw_match.group(1).replace("_", " ")
        direction = gw_match.group(2)
        ns = " (nanosecond component)" if gw_match.group(3) else ""
        return f"Timestamp when the message was {direction}d at the {component}{ns}."

    # Ingress/egress software/hardware timestamps
    ie_match = re.match(
        r"^(ingress|egress)_(software|hardware)_timestamp(_ns)?$", col_name
    )
    if ie_match:
        direction = ie_match.group(1)
        layer = ie_match.group(2)
        ns = " (nanosecond component)" if ie_match.group(3) else ""
        return f"Timestamp at {layer} {direction} point{ns}."

    # Custom greek fields
    custom_greek_match = re.match(r"^custom_greek_(\d+)$", col_name)
    if custom_greek_match:
        num = custom_greek_match.group(1)
        return f"Custom Greek value #{num}, a user-defined sensitivity metric."

    # theo_* fields (from TheoData)
    if col_name.startswith("theo_") and col_name not in ("theo_compute_type",):
        rest = col_name[5:].replace("_", " ")
        return f"Theoretical pricing parameter: {rest}."

    # param_* fields (vol surface parameters)
    if col_name.startswith("param_"):
        rest = col_name[6:].replace("_", " ")
        return f"Volatility surface parameter: {rest}."

    # greek_* fields (vol surface greeks)
    if col_name.startswith("greek_"):
        rest = col_name[6:].replace("_", " ")
        return f"Volatility surface Greek parameter: {rest}."

    # snap_ts / rec_ts for snapshot tables
    if col_name == "snap_ts":
        return "Timestamp when this point-in-time snapshot was captured."
    if col_name == "rec_ts":
        return "Timestamp when this record was received or recorded."

    # bid_orders_N / ask_orders_N
    orders_match = re.match(r"^(bid|ask)_orders_(\d+)$", col_name)
    if orders_match:
        side = orders_match.group(1)
        level = orders_match.group(2)
        return f"Number of {side}-side orders at level {level} of the order book."

    # *_exchange fields
    if col_name.endswith("_exchange") and col_name not in (
        "routed_exchange",
        "executable_exchange",
    ):
        base = col_name[:-9].replace("_", " ")
        return f"Exchange identifier for the {base}."

    # Suffix/prefix patterns
    if col_name.endswith("_timestamp_ns"):
        base = col_name[:-13].replace("_", " ")
        return f"Nanosecond-precision component of the {base} timestamp. Combine with {col_name[:-3]} for full nanosecond-resolution timing."

    if col_name.endswith("_timestamp") and col_name != "partition_timestamp_local":
        base = col_name[:-10].replace("_", " ")
        return f"Timestamp recording when the {base} occurred."

    if col_name.endswith("_ns") and col_type in ("INTEGER", "INT64"):
        base_col = col_name[:-3]
        base = base_col.replace("_", " ")
        return f"Nanosecond-precision component of {base}. Combine with {base_col} for full nanosecond-resolution timing."

    if col_name.endswith("_hash"):
        base = col_name[:-5].replace("_", " ")
        return f"SHA256 hash identifier for the {base}."

    if col_name.startswith("is_") and col_type in ("BOOLEAN", "BOOL"):
        what = col_name[3:].replace("_", " ")
        return f"Boolean flag indicating whether the {what} condition is true."

    if col_name.startswith("is_") and col_type in ("INTEGER", "INT64"):
        what = col_name[3:].replace("_", " ")
        return f"Flag indicating whether the {what} condition is true. Stored as integer (0=false, 1=true)."

    # Order book levels: bid_price_N, ask_price_N, bid_volume_N, ask_volume_N
    book_match = re.match(r"^(bid|ask)_(price|volume)_(\d+)$", col_name)
    if book_match:
        side = book_match.group(1)
        metric = book_match.group(2)
        level = int(book_match.group(3))
        return f"Unadjusted {side}-side {metric} at level {level} of the order book."

    # Depth levels: e.g. depth_bid_price_1, depth_ask_volume_3
    depth_match = re.match(r"^depth_(bid|ask)_(price|volume)_(\d+)$", col_name)
    if depth_match:
        side = depth_match.group(1)
        metric = depth_match.group(2)
        level = int(depth_match.group(3))
        return f"Market depth {side}-side {metric} at level {level}."

    # Implied levels
    implied_match = re.match(r"^implied_(bid|ask)_(price|volume)_(\d+)$", col_name)
    if implied_match:
        side = implied_match.group(1)
        metric = implied_match.group(2)
        level = int(implied_match.group(3))
        return f"Implied order book {side}-side {metric} at level {level}."

    # *_bid / *_ask suffixes
    if col_name.endswith("_bid"):
        base = col_name[:-4].replace("_", " ")
        return f"Bid-side value of {base}."

    if col_name.endswith("_ask"):
        base = col_name[:-4].replace("_", " ")
        return f"Ask-side value of {base}."

    # *_name enum columns
    name_match = re.match(r"^(.+)_name$", col_name)
    if name_match and col_type == "STRING":
        base = name_match.group(1).replace("_", " ")
        return f"Human-readable name for the {base} enum value."

    # Kafka infrastructure
    if col_name.startswith("kafka_"):
        rest = col_name[6:].replace("_", " ")
        return f"Kafka infrastructure field: {rest}. Used for data pipeline tracking."

    # streamsource_* and datasource_* fields
    if col_name.startswith("streamsource_"):
        rest = col_name[13:].replace("_", " ")
        return (
            f"Stream source {rest} identifier from the data streaming infrastructure."
        )

    if col_name.startswith("datasource_"):
        rest = col_name[11:].replace("_", " ")
        return f"Data source {rest} identifier from the data pipeline infrastructure."

    # parent_* fields (instrument reference)
    if col_name.startswith("parent_"):
        rest = col_name[7:].replace("_", " ")
        return f"Parent instrument {rest} from the instrument definition."

    # child_* / leg_* fields
    if col_name.startswith("child_"):
        rest = col_name[6:].replace("_", " ")
        return f"Child leg {rest} from the combo instrument definition."

    if col_name.startswith("leg_"):
        rest = col_name[4:].replace("_", " ")
        return f"Combo leg {rest}."

    # ref_* fields (reference values from VtCommon)
    if col_name.startswith("ref_"):
        rest = col_name[4:].replace("_", " ")
        return f"Reference {rest} from the last vol curve update (theoServer callback)."

    # slippage columns (KPI layer)
    slip_match = re.match(
        r"^(delta|vol|roll|residual|total|underlying)_slippage_(\w+)$", col_name
    )
    if slip_match:
        component = slip_match.group(1)
        interval = slip_match.group(2).replace("_", " ")
        return f"{component.capitalize()} component of slippage at the {interval} interval."

    # edge columns
    if "edge" in col_name and col_name != "edge":
        words = _snake_to_words(col_name)
        return f"Edge metric: {words}."

    # pnl columns
    if "pnl" in col_name:
        words = _snake_to_words(col_name)
        return f"PnL metric: {words}."

    # new_price_* / new_size_* / raw_price_* / delete_trigger_price_*
    new_match = re.match(r"^(new|raw|delete_trigger)_(price|size)_(bid|ask)$", col_name)
    if new_match:
        prefix = new_match.group(1).replace("_", " ")
        metric = new_match.group(2)
        side = new_match.group(3)
        return (
            f"{prefix.capitalize()} {metric} for the {side} side of the quote update."
        )

    # change_bid / change_ask
    if col_name in ("change_bid", "change_ask"):
        side = col_name.split("_")[1]
        return f"Boolean flag indicating whether the {side} side was changed in this update."

    # market_or_mako_trade / is_mako_or_market
    if col_name == "market_or_mako_trade":
        return (
            "Indicator distinguishing whether this is a market trade or a Mako trade."
        )
    if col_name == "is_mako_or_market":
        return "Flag indicating whether this trade leg is from Mako or from the market."

    # *_id fields (generic)
    if col_name.endswith("_id") and col_type == "STRING":
        base = col_name[:-3].replace("_", " ")
        return f"Identifier for the {base}."

    # *_type fields (generic)
    if col_name.endswith("_type") and col_name not in (
        "position_type",
        "inst_type",
        "exercise_type",
        "payment_type",
        "order_type",
        "delete_type",
        "trigger_type",
        "update_type",
    ):
        base = col_name[:-5].replace("_", " ")
        return f"Type classification for the {base}."

    # *_count fields
    if col_name.endswith("_count"):
        base = col_name[:-6].replace("_", " ")
        return f"Count of {base}."

    # *_rate fields
    if col_name.endswith("_rate"):
        base = col_name[:-5].replace("_", " ")
        return f"Rate value for {base}."

    # *_date fields
    if col_name.endswith("_date") and col_name not in (
        "trade_date",
        "rec_date",
        "event_date",
        "exchange_date",
        "agg_pos_date",
        "issue_date",
        "settlement_date",
        "value_date",
    ):
        base = col_name[:-5].replace("_", " ")
        return f"Date of {base}."

    # *_value fields
    if col_name.endswith("_value") and col_name not in ("cash_value",):
        base = col_name[:-6].replace("_", " ")
        return f"Value of {base}."

    # *_size fields
    if col_name.endswith("_size") and not re.match(r"^(bid|ask|depth)", col_name):
        base = col_name[:-5].replace("_", " ")
        return f"Size (quantity) of {base}."

    # *_price fields
    if col_name.endswith("_price") and not re.match(
        r"^(bid|ask|depth|new|raw|delete_trigger)", col_name
    ):
        base = col_name[:-6].replace("_", " ")
        return f"Price of {base}."

    # *_volume fields
    if col_name.endswith("_volume") and not re.match(
        r"^(bid|ask|depth|implied)", col_name
    ):
        base = col_name[:-7].replace("_", " ")
        return f"Volume of {base}."

    # Catch-all: generate from snake_case name + type
    if "_" in col_name:
        words = _snake_to_words(col_name)
        type_hint = {
            "STRING": "text",
            "INTEGER": "numeric",
            "INT64": "numeric",
            "FLOAT64": "numeric value",
            "BOOLEAN": "flag",
            "BOOL": "flag",
            "TIMESTAMP": "timestamp",
            "DATE": "date",
        }.get(col_type, "field")
        return f"{words.capitalize()} ({type_hint})."

    return None


# ---------------------------------------------------------------------------
# Synonym Generation
# ---------------------------------------------------------------------------


def generate_synonyms(col_name: str, proto_field_name: str | None) -> list[str] | None:
    """Generate synonyms from proto field names and common patterns."""
    synonyms: list[str] = []

    if proto_field_name and proto_field_name != col_name:
        # Convert camelCase proto name to human readable
        human = _camel_to_words(proto_field_name)
        if human and human != _snake_to_words(col_name):
            synonyms.append(human)

    return synonyms if synonyms else None


# ---------------------------------------------------------------------------
# Source String Generation
# ---------------------------------------------------------------------------


def generate_source(
    table_name: str,
    col_name: str,
    transform_map: dict[str, dict[str, dict[str, Any]]],
    proto_to_bq: dict[str, dict[str, Any]],
) -> str | None:
    """Generate a source string from proto/transform mappings."""
    t_info = transform_map.get(table_name, {}).get(col_name)
    if t_info is None:
        return None

    source_field = t_info["source_field"]
    transformation = t_info["transformation"]

    if source_field == "NULL":
        return None

    # Kafka infrastructure
    if transformation == "direct" and source_field == col_name:
        return "kafka infrastructure"

    # Derived columns
    if transformation == "derive":
        return "derived (data-loader)"

    # Get proto message name
    bq_info = proto_to_bq.get(table_name, {})
    message_name = bq_info.get("message", "")
    proto_file = bq_info.get("file", "")

    if source_field.startswith("props."):
        proto_field = source_field[6:]  # strip "props."
        # Strip _name and _ns suffixes for source references
        clean = proto_field
        if clean.endswith("_name"):
            clean = clean[:-5]
        if clean.endswith("_ns"):
            clean = clean[:-3]
        return f'"VtCommon.proto::{clean}"'

    # Direct proto field reference
    proto_field = t_info["proto_field"]
    clean = proto_field
    if clean.endswith("_name"):
        clean = clean[:-5]
    if clean.endswith("_ns"):
        clean = clean[:-3]

    if message_name and proto_file:
        return f'"{proto_file}::{clean}"'
    elif proto_field:
        return f'"proto field: {clean}"'

    return None


# ---------------------------------------------------------------------------
# Main Description Generator
# ---------------------------------------------------------------------------


def generate_changes(
    table_name: str,
    yaml_path: Path,
    omx_ref: dict[str, dict[str, dict[str, Any]]],
    transform_map: dict[str, dict[str, dict[str, Any]]],
    proto_desc_map: dict[str, dict[str, Any]],
    proto_to_bq: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Determine what changes to make for each column.

    Returns: {col_name: {field: value, ...}} for columns that need updates.
    Only includes fields that are currently empty/missing and can be filled.
    """
    data = yaml.safe_load(yaml_path.read_text())
    columns = data.get("table", {}).get("columns", [])
    changes: dict[str, dict[str, Any]] = {}

    for col in columns:
        col_name = col["name"]
        col_type = col.get("type", "")
        col_changes: dict[str, Any] = {}

        # --- Description ---
        existing_desc = col.get("description", "")
        if not existing_desc or str(existing_desc).strip() == "":
            desc = None
            tier = 0

            # Tier 1: Copy from OMX/KPI reference
            ref_cols = omx_ref.get(table_name, {})
            if col_name in ref_cols and "description" in ref_cols[col_name]:
                desc = ref_cols[col_name]["description"]
                tier = 1

            # Tier 2: Proto + transform
            if desc is None:
                desc = _build_tier2_description(
                    col_name,
                    col_type,
                    table_name,
                    transform_map,
                    proto_desc_map,
                    proto_to_bq,
                )
                if desc:
                    tier = 2

            # Tier 3: Name patterns
            if desc is None:
                tier3_desc = generate_tier3_description(col_name, col_type)
                if tier3_desc:
                    desc = tier3_desc
                    tier = 3

            # If Tier 2 produced a very short description (< 30 chars), prefer
            # Tier 3 if it has a longer, more informative result
            if desc and tier == 2 and len(desc) < 30:
                tier3_desc = generate_tier3_description(col_name, col_type)
                if tier3_desc and len(tier3_desc) > len(desc):
                    desc = tier3_desc
                    tier = 3

            if desc:
                col_changes["description"] = desc
                col_changes["_tier"] = tier

        # --- Source ---
        existing_source = col.get("source")
        if not existing_source:
            # Tier 1: Copy from OMX reference
            ref_cols = omx_ref.get(table_name, {})
            if col_name in ref_cols and "source" in ref_cols[col_name]:
                col_changes["source"] = ref_cols[col_name]["source"]
            else:
                src = generate_source(table_name, col_name, transform_map, proto_to_bq)
                if src:
                    col_changes["source"] = src

        # --- Synonyms ---
        existing_synonyms = col.get("synonyms")
        if existing_synonyms is None:
            # Tier 1: Copy from OMX reference
            ref_cols = omx_ref.get(table_name, {})
            if col_name in ref_cols and "synonyms" in ref_cols[col_name]:
                col_changes["synonyms"] = ref_cols[col_name]["synonyms"]

        # --- Business rules ---
        existing_rules = col.get("business_rules")
        if not existing_rules:
            ref_cols = omx_ref.get(table_name, {})
            if col_name in ref_cols and "business_rules" in ref_cols[col_name]:
                col_changes["business_rules"] = ref_cols[col_name]["business_rules"]

        # --- Related columns ---
        existing_related = col.get("related_columns")
        if not existing_related:
            ref_cols = omx_ref.get(table_name, {})
            if col_name in ref_cols and "related_columns" in ref_cols[col_name]:
                col_changes["related_columns"] = ref_cols[col_name]["related_columns"]

        if col_changes:
            changes[col_name] = col_changes

    return changes


# ---------------------------------------------------------------------------
# Surgical YAML Editing
# ---------------------------------------------------------------------------


def _needs_yaml_quoting(s: str) -> bool:
    """Check if a YAML scalar value needs quoting."""
    if ": " in s or s.endswith(":"):
        return True
    if " #" in s or s.startswith("#"):
        return True
    return s.startswith(("{", "[", "&", "*", "!", "%", "|", ">"))


def _format_description(desc: str, indent: str) -> list[str]:
    """Format a description value into YAML lines."""
    desc_str = str(desc).strip()
    lines: list[str] = []

    if not desc_str:
        lines.append(f'{indent}description: ""')
        return lines

    if _needs_yaml_quoting(desc_str):
        if len(desc_str) > 80:
            # Use single-quoted multi-line format for long descriptions
            # with YAML-sensitive characters
            escaped = desc_str.replace("'", "''")
            # Single-quoted scalar: wrap at 120 cols
            words = escaped.split()
            current_line = f"{indent}description: '{words[0]}"
            continuation = indent + "  "
            for word in words[1:]:
                if len(current_line) + 1 + len(word) <= 118:  # account for trailing '
                    current_line += " " + word
                else:
                    lines.append(current_line)
                    current_line = continuation + word
            lines.append(current_line + "'")
        else:
            escaped = desc_str.replace("'", "''")
            lines.append(f"{indent}description: '{escaped}'")
    elif len(desc_str) > 80 or "\n" in desc_str:
        # Multi-line: word-wrap at ~120 columns
        words = desc_str.split()
        current_line = f"{indent}description: {words[0]}"
        continuation = indent + "  "
        for word in words[1:]:
            if len(current_line) + 1 + len(word) <= 120:
                current_line += " " + word
            else:
                lines.append(current_line)
                current_line = continuation + word
        lines.append(current_line)
    else:
        lines.append(f"{indent}description: {desc_str}")

    return lines


def _format_source(src: str, indent: str) -> str:
    """Format a source value as a single YAML line."""
    src_str = str(src).strip()
    # If already quoted, use as-is
    if src_str.startswith('"') and src_str.endswith('"'):
        return f"{indent}source: {src_str}"
    # Quote if contains special characters
    if any(c in src_str for c in ":#{}[]"):
        escaped = src_str.replace("'", "''")
        return f"{indent}source: '{escaped}'"
    return f"{indent}source: {src_str}"


def _format_synonyms(synonyms: list[str], indent: str) -> list[str]:
    """Format synonyms as YAML lines."""
    if not synonyms:
        return [f"{indent}synonyms: []"]
    lines = [f"{indent}synonyms:"]
    for syn in synonyms:
        lines.append(f"{indent}- {syn}")
    return lines


def _format_business_rules(rules: str, indent: str) -> list[str]:
    """Format business_rules as YAML lines."""
    rules_str = str(rules).strip()
    lines: list[str] = []
    if len(rules_str) > 80:
        lines.append(f"{indent}business_rules: >-")
        words = rules_str.split()
        continuation = indent + "  "
        current_line = continuation + words[0]
        for word in words[1:]:
            if len(current_line) + 1 + len(word) <= 120:
                current_line += " " + word
            else:
                lines.append(current_line)
                current_line = continuation + word
        lines.append(current_line)
    elif _needs_yaml_quoting(rules_str):
        escaped = rules_str.replace("'", "''")
        lines.append(f"{indent}business_rules: '{escaped}'")
    else:
        lines.append(f"{indent}business_rules: {rules_str}")
    return lines


def _format_related_columns(cols: list[str], indent: str) -> list[str]:
    """Format related_columns as YAML lines."""
    if not cols:
        return []
    lines = [f"{indent}related_columns:"]
    for rc in cols:
        lines.append(f"{indent}- {rc}")
    return lines


def apply_changes(
    yaml_path: Path,
    changes: dict[str, dict[str, Any]],
) -> None:
    """Surgically edit a YAML file to insert/replace enrichment fields.

    Only replaces description: "" lines with generated descriptions.
    Inserts source/synonyms/business_rules/related_columns after existing fields
    when those fields are missing from the current column block.
    """
    lines = yaml_path.read_text().splitlines()
    result: list[str] = []
    current_col: str | None = None
    col_indent = ""
    field_indent = ""
    in_columns = False

    # Track which fields we've seen for the current column, so we can
    # insert missing ones at the end of the column block
    seen_fields: set[str] = set()
    pending_inserts: dict[str, Any] = {}

    def _flush_pending():
        """Insert any pending fields for the current column."""
        nonlocal pending_inserts
        if not pending_inserts or not current_col:
            return

        fi = field_indent

        if "source" in pending_inserts:
            result.append(_format_source(pending_inserts["source"], fi))
        if "synonyms" in pending_inserts:
            result.extend(_format_synonyms(pending_inserts["synonyms"], fi))
        if "business_rules" in pending_inserts:
            result.extend(_format_business_rules(pending_inserts["business_rules"], fi))
        if "related_columns" in pending_inserts:
            result.extend(
                _format_related_columns(pending_inserts["related_columns"], fi)
            )

        pending_inserts = {}

    i = 0
    n = len(lines)

    while i < n:
        line = lines[i]

        # Detect columns section
        if re.match(r"^\s+columns:\s*$", line):
            in_columns = True
            result.append(line)
            i += 1
            continue

        # Detect a new column block
        col_match = re.match(r"^(\s+)-\s+name:\s+(\S+)\s*$", line)
        if col_match and in_columns:
            # Flush any pending inserts for the previous column
            _flush_pending()

            col_indent = col_match.group(1)
            field_indent = col_indent + "  "
            current_col = col_match.group(2).strip()
            seen_fields = {"name"}

            # Prepare pending inserts for this column
            col_changes = changes.get(current_col, {})
            pending_inserts = {}
            for field in ["source", "synonyms", "business_rules", "related_columns"]:
                if field in col_changes:
                    pending_inserts[field] = col_changes[field]

            result.append(line)
            i += 1
            continue

        # Inside a column block: detect field lines
        if in_columns and current_col:
            # Check for description line
            desc_match = re.match(r'^(\s+)description:\s*(""|\'\'|)\s*$', line)
            if desc_match:
                seen_fields.add("description")
                col_changes = changes.get(current_col, {})
                if "description" in col_changes:
                    fi = desc_match.group(1)
                    desc_lines = _format_description(col_changes["description"], fi)
                    result.extend(desc_lines)
                    i += 1
                    continue
                else:
                    result.append(line)
                    i += 1
                    continue

            # Check for existing non-empty description (multi-line)
            desc_content_match = re.match(r"^(\s+)description:\s+\S", line)
            if desc_content_match:
                seen_fields.add("description")
                result.append(line)
                i += 1
                # Consume continuation lines of multi-line description
                while i < n:
                    next_line = lines[i]
                    # A continuation line is more deeply indented than the field key
                    # and doesn't match a known field pattern
                    if next_line and re.match(r"^\s+\S", next_line):
                        # Check if this is a continuation or a new field
                        is_field = re.match(
                            r"^\s+(name|type|description|source|synonyms|category|"
                            r"filterable|example_values|typical_aggregation|"
                            r"business_rules|related_columns|comprehensive|formula|"
                            r"notes|preferred_timestamps|disambiguation|"
                            r"source_sql|proto_source|business_context|"
                            r"partition_field|cluster_fields|row_count_approx|"
                            r"dataset|fqn|layer):",
                            next_line,
                        )
                        is_list_item = re.match(r"^\s+-\s+name:", next_line)
                        if is_field or is_list_item:
                            break
                        # Check indent level: continuation lines are indented
                        # deeper than the field line
                        field_key_indent = len(desc_content_match.group(1))
                        next_indent = len(next_line) - len(next_line.lstrip())
                        if next_indent > field_key_indent:
                            result.append(next_line)
                            i += 1
                        else:
                            break
                    else:
                        break
                continue

            # Track other existing fields to avoid duplicating
            for field_name in [
                "source",
                "synonyms",
                "business_rules",
                "related_columns",
                "category",
                "filterable",
                "example_values",
                "typical_aggregation",
                "comprehensive",
                "formula",
                "type",
            ]:
                if re.match(rf"^\s+{field_name}:", line):
                    seen_fields.add(field_name)
                    # Remove from pending since it already exists
                    pending_inserts.pop(field_name, None)
                    break

            # Detect end of column block: next column or end of columns
            # We need to flush pending before the next column starts
            if i + 1 < n:
                next_line = lines[i + 1]
                next_col = re.match(r"^\s+-\s+name:\s+\S+", next_line)
                if next_col:
                    result.append(line)
                    _flush_pending()
                    i += 1
                    continue

        # Handle end of file for last column
        if i == n - 1 and in_columns and current_col:
            result.append(line)
            _flush_pending()
            i += 1
            continue

        result.append(line)
        i += 1

    # Final flush for last column
    _flush_pending()

    yaml_path.write_text("\n".join(result) + "\n")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_yaml(yaml_path: Path) -> bool:
    """Validate that a YAML file still parses correctly after editing."""
    try:
        data = yaml.safe_load(yaml_path.read_text())
        return bool(data) and "table" in data
    except yaml.YAMLError:
        return False


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def count_coverage(yaml_path: Path) -> dict[str, int]:
    """Count enrichment coverage for a single YAML file."""
    data = yaml.safe_load(yaml_path.read_text())
    columns = data.get("table", {}).get("columns", [])
    total = len(columns)
    has_desc = 0
    has_source = 0
    has_synonyms = 0

    for col in columns:
        desc = col.get("description", "")
        if desc and str(desc).strip() != "":
            has_desc += 1
        if col.get("source"):
            has_source += 1
        if col.get("synonyms") is not None:
            has_synonyms += 1

    return {
        "total": total,
        "descriptions": has_desc,
        "sources": has_source,
        "synonyms": has_synonyms,
    }


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
    """Run description enrichment across catalog YAMLs."""

    print("=" * 70)
    print("Column Description Enrichment")
    print("=" * 70)
    print()

    # Load metadata indexes
    print("Loading metadata indexes...")
    proto_messages = _load_proto_fields()
    proto_desc_map = build_proto_description_map(proto_messages)
    proto_to_bq = _load_proto_to_bq()
    transforms = _load_transforms()
    transform_map = build_transform_map(transforms)
    omx_ref = build_omx_reference()

    print(f"  Proto messages: {len(proto_messages)}")
    print(f"  Proto field descriptions: {len(proto_desc_map)}")
    print(f"  Transform tables: {len(transform_map)}")
    print(f"  OMX reference tables: {len(omx_ref)}")
    omx_cols = sum(len(cols) for cols in omx_ref.values())
    print(f"  OMX reference columns: {omx_cols}")
    print()

    # Determine which tables to process
    if all_markets or (layer and layer not in ALL_TABLES):
        target_tables = filter_combined_tables(layer, table, include_markets=True)
    else:
        target_tables = filter_tables(layer, table) if (layer or table) else ALL_TABLES

    # Also include OMX data-layer tables (catalog/data/) not in the core ALL_TABLES.
    # These are in MARKET_TABLES["omx_data"] but combined_tables() skips omx_data
    # because they live under catalog/data/, not catalog/omx_data/.
    if all_markets and "data" in target_tables:
        omx_extra = MARKET_TABLES.get("omx_data", [])
        core_data = set(ALL_TABLES.get("data", []))
        extra = [t for t in omx_extra if t not in core_data]
        if table:
            extra = [t for t in extra if t == table]
        if extra:
            target_tables["data"] = list(set(target_tables["data"]) | set(extra))

    all_stats: dict[str, dict] = {}
    grand_total_desc = 0
    grand_total_source = 0
    grand_total_synonyms = 0
    grand_total_rules = 0
    grand_total_related = 0
    tier_counts = {1: 0, 2: 0, 3: 0}
    validation_failures: list[str] = []

    for lyr, tables in sorted(target_tables.items()):
        print(f"--- {lyr} ---")

        for table_name in sorted(tables):
            yaml_path = CATALOG_DIR / lyr / f"{table_name}.yaml"
            if not yaml_path.exists():
                print(f"  SKIP: {yaml_path.name} not found")
                continue

            key = f"{lyr}/{table_name}"

            # Generate changes
            changes = generate_changes(
                table_name,
                yaml_path,
                omx_ref,
                transform_map,
                proto_desc_map,
                proto_to_bq,
            )

            if not changes:
                all_stats[key] = {
                    "descriptions": 0,
                    "sources": 0,
                    "synonyms": 0,
                    "business_rules": 0,
                    "related_columns": 0,
                }
                print(f"  {table_name}: no changes needed")
                continue

            # Count changes by type
            desc_count = sum(1 for c in changes.values() if "description" in c)
            source_count = sum(1 for c in changes.values() if "source" in c)
            synonym_count = sum(1 for c in changes.values() if "synonyms" in c)
            rules_count = sum(1 for c in changes.values() if "business_rules" in c)
            related_count = sum(1 for c in changes.values() if "related_columns" in c)

            # Count tiers
            for c in changes.values():
                t = c.get("_tier")
                if t in tier_counts:
                    tier_counts[t] += 1

            stats = {
                "descriptions": desc_count,
                "sources": source_count,
                "synonyms": synonym_count,
                "business_rules": rules_count,
                "related_columns": related_count,
            }
            all_stats[key] = stats

            grand_total_desc += desc_count
            grand_total_source += source_count
            grand_total_synonyms += synonym_count
            grand_total_rules += rules_count
            grand_total_related += related_count

            parts = []
            if desc_count:
                parts.append(f"desc={desc_count}")
            if source_count:
                parts.append(f"src={source_count}")
            if synonym_count:
                parts.append(f"syn={synonym_count}")
            if rules_count:
                parts.append(f"rules={rules_count}")
            if related_count:
                parts.append(f"related={related_count}")

            action = "WOULD" if dry_run else "APPLIED"
            print(f"  {table_name}: {action} {', '.join(parts)}")

            if not dry_run:
                apply_changes(yaml_path, changes)

                # Validate the result
                if not validate_yaml(yaml_path):
                    validation_failures.append(key)
                    print(f"    WARNING: YAML validation failed for {key}!")

        print()

    # Summary
    print("=" * 70)
    mode = "DRY RUN" if dry_run else "APPLIED"
    print(f"Summary ({mode}):")
    print(f"  Descriptions: {grand_total_desc}")
    print(f"    Tier 1 (OMX copy): {tier_counts[1]}")
    print(f"    Tier 2 (proto+transform): {tier_counts[2]}")
    print(f"    Tier 3 (name pattern): {tier_counts[3]}")
    print(f"  Sources: {grand_total_source}")
    print(f"  Synonyms: {grand_total_synonyms}")
    print(f"  Business rules: {grand_total_rules}")
    print(f"  Related columns: {grand_total_related}")
    print(f"  Tables processed: {len(all_stats)}")

    if validation_failures:
        print(f"\n  VALIDATION FAILURES ({len(validation_failures)}):")
        for vf in validation_failures:
            print(f"    - {vf}")

    print("=" * 70)

    return all_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate and apply column-level descriptions for catalog YAMLs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report changes without writing files",
    )
    parser.add_argument(
        "--layer",
        help="Filter to one layer/market (kpi/data/arb_data/...)",
    )
    parser.add_argument(
        "--table",
        help="Filter to one table name (e.g. markettrade, instruments)",
    )
    parser.add_argument(
        "--all-markets",
        action="store_true",
        help="Include all market directories",
    )
    args = parser.parse_args()
    main(
        dry_run=args.dry_run,
        layer=args.layer,
        table=args.table,
        all_markets=args.all_markets,
    )
