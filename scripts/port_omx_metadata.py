#!/usr/bin/env python3
"""Port metadata from rich OMX catalog YAMLs to skeleton market YAMLs.

Reads the 7 enriched OMX data-layer YAMLs (catalog/data/) and ports column-level
metadata (description, synonyms, category, typical_aggregation, filterable,
example_values, business_rules, related_columns, source) to matching columns in
skeleton YAMLs for 8 other markets (catalog/{market}_data/).

Uses surgical line-by-line editing to preserve skeleton formatting, comments, and
market-specific fields (dataset, fqn, layer).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CATALOG_DIR = Path(__file__).resolve().parent.parent / "catalog"
OMX_DIR = CATALOG_DIR / "data"

# The 7 tables that have rich metadata in the OMX catalog
RICH_TABLES = [
    "markettrade",
    "quotertrade",
    "clicktrade",
    "theodata",
    "marketdata",
    "marketdepth",
    "swingdata",
]

# Target markets (each has a {market}_data/ directory under catalog/)
TARGET_MARKETS = [
    "arb_data",
    "asx_data",
    "brazil_data",
    "eurex_data",
    "euronext_data",
    "ice_data",
    "korea_data",
    "nse_data",
]

# Column-level fields to port from OMX -> skeleton
# Order matters: this is the order they will appear in the output YAML
COLUMN_FIELDS = [
    "description",
    "source",
    "synonyms",
    "business_rules",
    "category",
    "typical_aggregation",
    "filterable",
    "example_values",
    "related_columns",
]


# ---------------------------------------------------------------------------
# Parse OMX rich YAML into a lookup structure
# ---------------------------------------------------------------------------


def load_omx_table(table_name: str) -> dict[str, Any]:
    """Load an OMX rich YAML and return a structured dict.

    Returns:
        {
            "table_description": str,
            "columns": {
                "col_name": {
                    "description": str,
                    "source": str | None,
                    "synonyms": list | None,
                    "category": str | None,
                    ...
                }
            }
        }
    """
    path = OMX_DIR / f"{table_name}.yaml"
    with open(path) as f:
        data = yaml.safe_load(f)

    table = data["table"]
    result: dict[str, Any] = {
        "table_description": table.get("description", ""),
        "columns": {},
    }

    for col in table.get("columns", []):
        col_name = col["name"]
        col_meta: dict[str, Any] = {}
        for field in COLUMN_FIELDS:
            if field in col:
                col_meta[field] = col[field]
        result["columns"][col_name] = col_meta

    return result


# ---------------------------------------------------------------------------
# YAML value formatting helpers
# ---------------------------------------------------------------------------


def _indent(text: str, spaces: int) -> str:
    """Indent every line of text by the given number of spaces."""
    prefix = " " * spaces
    return "\n".join(prefix + line if line else line for line in text.split("\n"))


def _needs_yaml_quoting(s: str) -> bool:
    """Check if a YAML scalar value needs quoting to avoid parse ambiguity."""
    # Colons followed by space can be interpreted as key-value pairs
    if ": " in s or s.endswith(":"):
        return True
    # Hash can start a comment
    if " #" in s or s.startswith("#"):
        return True
    # Curly braces / square brackets are flow syntax
    if s.startswith("{") or s.startswith("["):
        return True
    # Ampersand and asterisk are anchors/aliases
    if s.startswith("&") or s.startswith("*"):
        return True
    # Exclamation mark is a tag
    if s.startswith("!"):
        return True
    # Percent is a directive
    if s.startswith("%"):
        return True
    # Pipe and greater-than are block scalars
    return bool(s.startswith("|") or s.startswith(">"))


def format_yaml_value(key: str, value: Any, indent_level: int) -> list[str]:
    """Format a single YAML key-value pair into lines, matching the OMX style.

    indent_level is the number of spaces before the key.
    """
    prefix = " " * indent_level
    lines: list[str] = []

    if key == "description":
        if not value or value == "":
            lines.append(f'{prefix}description: ""')
        else:
            # Use multi-line block scalar for long descriptions
            # Match the OMX style: description on first line, continuation indented
            desc_str = str(value).strip()
            if "\n" in desc_str or len(desc_str) > 80:
                # Multi-line: first chunk on same line as key, rest indented
                words = desc_str.split()
                current_line = f"{prefix}description: {words[0]}"
                continuation_prefix = " " * (indent_level + 2)
                for word in words[1:]:
                    if len(current_line) + 1 + len(word) <= 120:
                        current_line += " " + word
                    else:
                        lines.append(current_line)
                        current_line = continuation_prefix + word
                lines.append(current_line)
            elif _needs_yaml_quoting(desc_str):
                # Short description with YAML-sensitive characters -- quote it
                # Use single quotes (escape internal single quotes by doubling them)
                escaped = desc_str.replace("'", "''")
                lines.append(f"{prefix}description: '{escaped}'")
            else:
                lines.append(f"{prefix}description: {desc_str}")

    elif key == "source":
        if value:
            src_str = str(value)
            # Always double-quote source values for consistency with OMX style
            # Escape any internal double-quotes
            escaped = src_str.replace('"', '\\"')
            lines.append(f'{prefix}source: "{escaped}"')

    elif key == "synonyms":
        if value is None or value == []:
            lines.append(f"{prefix}synonyms: []")
        elif isinstance(value, list) and len(value) > 0:
            lines.append(f"{prefix}synonyms:")
            for syn in value:
                lines.append(f"{prefix}- {syn}")
        else:
            lines.append(f"{prefix}synonyms: []")

    elif key == "business_rules":
        if value:
            br_str = str(value).strip()
            if len(br_str) > 80:
                lines.append(f"{prefix}business_rules: >-")
                words = br_str.split()
                continuation_prefix = " " * (indent_level + 2)
                current_line = continuation_prefix + words[0]
                for word in words[1:]:
                    if len(current_line) + 1 + len(word) <= 120:
                        current_line += " " + word
                    else:
                        lines.append(current_line)
                        current_line = continuation_prefix + word
                lines.append(current_line)
            elif _needs_yaml_quoting(br_str):
                escaped = br_str.replace("'", "''")
                lines.append(f"{prefix}business_rules: '{escaped}'")
            else:
                lines.append(f"{prefix}business_rules: {br_str}")

    elif key == "category":
        if value:
            lines.append(f"{prefix}category: {value}")

    elif key == "typical_aggregation":
        if value:
            lines.append(f"{prefix}typical_aggregation: {value}")

    elif key == "filterable":
        if value is not None:
            lines.append(f"{prefix}filterable: {str(value).lower()}")

    elif key == "example_values":
        if value and isinstance(value, list) and len(value) > 0:
            lines.append(f"{prefix}example_values:")
            for ev in value:
                # Preserve the OMX style: values as quoted strings or bare
                ev_str = str(ev)
                # If already has quotes, keep as-is
                if ev_str.startswith("'") or ev_str.startswith('"'):
                    lines.append(f"{prefix}- {ev_str}")
                else:
                    lines.append(f"{prefix}- '{ev_str}'")

    elif (
        key == "related_columns"
        and value
        and isinstance(value, list)
        and len(value) > 0
    ):
        lines.append(f"{prefix}related_columns:")
        for rc in value:
            lines.append(f"{prefix}- {rc}")

    return lines


# ---------------------------------------------------------------------------
# Surgical skeleton editing
# ---------------------------------------------------------------------------


def enrich_skeleton(skeleton_lines: list[str], omx_meta: dict[str, Any]) -> list[str]:
    """Enrich a skeleton YAML's lines with metadata from the OMX source.

    Strategy:
    1. Replace the table-level `description: ""` with the OMX description.
    2. For each column block (starting with `- name: xxx`), if that column
       exists in the OMX metadata, replace `description: ""` and add fields
       after it.
    """
    output: list[str] = []
    i = 0
    n = len(skeleton_lines)

    # Step 1: Find and replace table-level description
    # The table-level description is the one that appears before `columns:` and
    # is indented at 2 spaces (i.e., `  description: ""`)
    columns_seen = False

    while i < n:
        line = skeleton_lines[i]

        # Detect if we've reached the columns section
        if re.match(r"^\s+columns:\s*$", line):
            columns_seen = True
            output.append(line)
            i += 1
            continue

        # Table-level description replacement (before columns section)
        if not columns_seen and re.match(r'^(\s+)description:\s*""?\s*$', line):
            indent_match = re.match(r"^(\s+)", line)
            indent_str = indent_match.group(1) if indent_match else "  "
            table_desc = omx_meta.get("table_description", "")
            if table_desc:
                desc_lines = format_yaml_value(
                    "description", table_desc, len(indent_str)
                )
                for dl in desc_lines:
                    output.append(dl)
            else:
                output.append(line)
            i += 1
            continue

        # Column block detection (inside columns section)
        if columns_seen:
            col_match = re.match(r"^(\s+)-\s+name:\s+(\S+)\s*$", line)
            if col_match:
                col_indent_base = col_match.group(1)
                col_name = col_match.group(2)
                # The field indent is the base indent + "  " (for the "- " prefix -> "  ")
                # In the skeleton: "    - name: xxx" means fields at "      type: ..."
                field_indent = len(col_indent_base) + 2

                omx_col = omx_meta["columns"].get(col_name)

                # Emit the `- name:` line
                output.append(line)
                i += 1

                # Consume the `type:` line
                if i < n:
                    output.append(skeleton_lines[i])
                    i += 1

                # Consume the existing `description: ""` line
                if i < n and re.match(r'^\s+description:\s*""?\s*$', skeleton_lines[i]):
                    if omx_col:
                        # Replace with enriched fields
                        for field in COLUMN_FIELDS:
                            if field in omx_col:
                                field_lines = format_yaml_value(
                                    field, omx_col[field], field_indent
                                )
                                output.extend(field_lines)
                    else:
                        # No OMX match -- keep the empty description
                        output.append(skeleton_lines[i])
                    i += 1
                continue

        output.append(line)
        i += 1

    return output


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 60)
    print("Port OMX Metadata to Market Skeletons")
    print("=" * 60)
    print()

    # Load all OMX rich metadata
    omx_tables: dict[str, dict[str, Any]] = {}
    for table_name in RICH_TABLES:
        path = OMX_DIR / f"{table_name}.yaml"
        if not path.exists():
            print(f"  WARNING: OMX source not found: {path}")
            continue
        omx_tables[table_name] = load_omx_table(table_name)
        col_count = len(omx_tables[table_name]["columns"])
        print(f"  Loaded OMX {table_name}: {col_count} columns with metadata")

    print()

    # Process each market
    total_files = 0
    total_columns_enriched = 0

    for market in TARGET_MARKETS:
        market_dir = CATALOG_DIR / market
        if not market_dir.exists():
            print(f"  SKIP: {market} directory not found")
            continue

        print(f"--- {market} ---")

        for table_name, omx_meta in omx_tables.items():
            skeleton_path = market_dir / f"{table_name}.yaml"
            if not skeleton_path.exists():
                print(f"  SKIP: {market}/{table_name}.yaml not found")
                continue

            # Read skeleton
            with open(skeleton_path) as f:
                skeleton_text = f.read()
            skeleton_lines = skeleton_text.split("\n")

            # Remove trailing newline artifact
            if skeleton_lines and skeleton_lines[-1] == "":
                skeleton_lines = skeleton_lines[:-1]

            # Count columns that will be enriched
            matched_columns = 0
            for line in skeleton_lines:
                col_match = re.match(r"^\s+-\s+name:\s+(\S+)\s*$", line)
                if col_match:
                    col_name = col_match.group(1)
                    if col_name in omx_meta["columns"]:
                        matched_columns += 1

            # Enrich
            enriched_lines = enrich_skeleton(skeleton_lines, omx_meta)

            # Write back
            with open(skeleton_path, "w") as f:
                f.write("\n".join(enriched_lines) + "\n")

            total_files += 1
            total_columns_enriched += matched_columns
            total_cols_in_skeleton = sum(
                1 for line in skeleton_lines if re.match(r"^\s+-\s+name:\s+\S+", line)
            )
            print(
                f"  {table_name}: enriched {matched_columns}/{total_cols_in_skeleton} columns"
            )

        print()

    print("=" * 60)
    print(
        f"Done. Enriched {total_files} files, {total_columns_enriched} columns total."
    )
    print("=" * 60)


if __name__ == "__main__":
    main()
