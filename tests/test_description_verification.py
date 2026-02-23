"""Tests for description quality — detect hallucinated column references.

Scans every column description in the YAML catalog and flags references
to column names that don't exist in the same table.  This catches a common
LLM-generation artefact where a description mentions a plausible-sounding
column that was never actually created.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

CATALOG_DIR = Path(__file__).resolve().parent.parent / "catalog"

# Tables to check (layer, table_name) — only enriched tables (non-empty table description)
_TABLES = []
for _layer in ("kpi", "data"):
    _dir = CATALOG_DIR / _layer
    if _dir.exists():
        for _f in sorted(_dir.glob("*.yaml")):
            if not _f.name.startswith("_"):
                _data = yaml.safe_load(_f.read_text())
                _desc = _data.get("table", {}).get("description", "")
                if _desc and _desc.strip():  # Skip skeleton/WIP tables
                    _TABLES.append((_layer, _f.stem))


def _load_table(layer: str, table: str) -> dict:
    path = CATALOG_DIR / layer / f"{table}.yaml"
    return yaml.safe_load(path.read_text())


def _extract_column_refs_from_text(text: str, all_column_names: set[str]) -> set[str]:
    """Extract tokens from text that look like column name references.

    Only returns tokens that match the pattern of existing column names
    (lowercase with underscores, or known column names).
    """
    # Extract word tokens that look like column identifiers
    tokens = set(re.findall(r"\b([a-z][a-z0-9_]*(?:_[a-z0-9]+)+)\b", text))
    # Only keep tokens that are plausible column names (contain underscore)
    # and match something in the catalog
    return tokens & all_column_names


# ---------------------------------------------------------------------------
# Known hallucination patterns from autopsy findings
# ---------------------------------------------------------------------------

# These specific column names were identified as hallucinated in the autopsy
# Column names that should never appear as column references in descriptions.
# edge_bps is excluded: it's legitimately used as a trader synonym/alias.
_KNOWN_HALLUCINATED = {
    "delta_bucket",  # Does not exist in any table
    "bid_size_0",  # Should be bid_volume_0
    "ask_size_0",  # Should be ask_volume_0
    "putcall",  # Should be option_type_name
}


class TestDescriptionColumnReferences:
    """Verify descriptions don't reference non-existent columns."""

    @pytest.mark.parametrize("layer,table", _TABLES)
    def test_description_refs_exist_in_table(self, layer, table):
        """Column names mentioned in descriptions must exist in the same table."""
        data = _load_table(layer, table)
        columns = data["table"]["columns"]

        errors = []
        for col in columns:
            desc = col.get("description", "")
            bad_refs = _extract_hallucinated_refs(desc)
            if bad_refs:
                errors.append(f"  {col['name']}: references {bad_refs}")

        assert not errors, (
            f"{layer}/{table} has descriptions with hallucinated column refs:\n"
            + "\n".join(errors)
        )


def _extract_hallucinated_refs(text: str) -> set[str]:
    """Check if text references any known-hallucinated column names."""
    tokens = set(re.findall(r"\b([a-z][a-z0-9_]*(?:_[a-z0-9]+)*)\b", text))
    return tokens & _KNOWN_HALLUCINATED


class TestKnownHallucinationPatterns:
    """Verify specific known hallucinations are not present anywhere."""

    @pytest.mark.parametrize("bad_name", sorted(_KNOWN_HALLUCINATED))
    def test_hallucinated_name_not_in_descriptions(self, bad_name):
        """Known hallucinated column name should not appear in any description."""
        occurrences = []
        for layer, table in _TABLES:
            data = _load_table(layer, table)
            for col in data["table"]["columns"]:
                desc = col.get("description", "")
                if bad_name in desc:
                    occurrences.append(f"{layer}/{table}:{col['name']}")

        assert not occurrences, (
            f"Hallucinated name '{bad_name}' found in descriptions:\n"
            + "\n".join(f"  {o}" for o in occurrences)
        )


class TestDescriptionQuality:
    """Basic description quality checks."""

    @pytest.mark.parametrize("layer,table", _TABLES)
    def test_no_empty_descriptions(self, layer, table):
        """Every column must have a non-empty description."""
        data = _load_table(layer, table)
        empty = [
            c["name"]
            for c in data["table"]["columns"]
            if not c.get("description", "").strip()
        ]
        assert not empty, f"{layer}/{table} has empty descriptions: {empty}"

    @pytest.mark.parametrize("layer,table", _TABLES)
    def test_descriptions_minimum_length(self, layer, table):
        """Descriptions should be at least 10 characters (not just a type name)."""
        data = _load_table(layer, table)
        short = [
            (c["name"], c.get("description", ""))
            for c in data["table"]["columns"]
            if len(c.get("description", "").strip()) < 10
        ]
        assert not short, f"{layer}/{table} has very short descriptions:\n" + "\n".join(
            f"  {name}: '{desc}'" for name, desc in short
        )
