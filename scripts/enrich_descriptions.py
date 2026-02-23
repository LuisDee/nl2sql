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
from table_registry import ALL_TABLES, filter_combined_tables, filter_tables

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
