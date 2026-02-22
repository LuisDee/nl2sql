"""Tests for metadata/field_lineage.yaml (Track 23, Phase 3).

Validates schema, content, and cross-references of the field lineage map
that traces key columns from proto -> silver (data-loader) -> gold (KPI).
"""

import re
from pathlib import Path

import pytest
import yaml

METADATA_DIR = Path(__file__).parent.parent / "metadata"

# Key columns that MUST have lineage entries
# Note: interval-templated columns use {interval} pattern (e.g. delta_slippage_{interval})
REQUIRED_LINEAGE_COLUMNS = {
    "instant_edge",
    "instant_pnl",
    "instant_pnl_w_fees",
    "delta_slippage_{interval}",
    "trade_date",
    "event_timestamp_ns",
    "trade_price",
    "trade_size",
    "contract_size",
}

# Known TradableInstrument subtypes in proto
INSTRUMENT_SUBTYPES = {
    "TradableInstrument",
    "Instrument",
    "Option",
    "Future",
    "Stock",
    "Bond",
    "Rate",
    "Commodity",
    "Cash",
    "Index",
    "Combo",
}


def _parse_composite_messages(msg: str) -> list[str]:
    """Parse composite proto message references like 'MarketTrade + VtCommon'."""
    # Split on ' + ' or ' / '
    return [m.strip() for m in re.split(r"\s*[+/]\s*", msg)]


def _is_list_column(col: str) -> bool:
    """Check if a silver column value is a list of input columns (for computed fields)."""
    return col.startswith("[") or col.startswith("(") or "," in col


class TestFieldLineageYaml:
    """Validate metadata/field_lineage.yaml schema and content."""

    @pytest.fixture(autouse=True)
    def load_yaml(self):
        path = METADATA_DIR / "field_lineage.yaml"
        assert path.exists(), f"Missing {path}"
        with open(path) as f:
            self.data = yaml.safe_load(f)

    def test_top_level_is_dict_with_lineages_key(self):
        assert isinstance(self.data, dict)
        assert "lineages" in self.data

    def test_lineages_is_list(self):
        assert isinstance(self.data["lineages"], list)
        assert len(self.data["lineages"]) > 0

    def test_each_lineage_has_required_keys(self):
        for entry in self.data["lineages"]:
            assert "column" in entry, f"Lineage entry missing 'column': {entry}"
            assert "layers" in entry, (
                f"Lineage entry {entry.get('column')} missing 'layers'"
            )

    def test_each_lineage_has_proto_layer(self):
        """Every lineage should trace back to a proto source."""
        for entry in self.data["lineages"]:
            layers = entry["layers"]
            assert isinstance(layers, dict), (
                f"Lineage {entry['column']}: 'layers' should be a dict"
            )
            assert "proto" in layers, f"Lineage {entry['column']} missing 'proto' layer"

    def test_proto_layer_has_required_keys(self):
        for entry in self.data["lineages"]:
            proto = entry["layers"].get("proto", {})
            assert "field" in proto, (
                f"Lineage {entry['column']}: proto layer missing 'field'"
            )
            assert "message" in proto, (
                f"Lineage {entry['column']}: proto layer missing 'message'"
            )

    def test_silver_layer_has_required_keys(self):
        """Silver layer entries should have column name and table."""
        for entry in self.data["lineages"]:
            silver = entry["layers"].get("silver")
            if silver is None:
                continue
            assert "column" in silver, (
                f"Lineage {entry['column']}: silver layer missing 'column'"
            )
            assert "table" in silver or "tables" in silver, (
                f"Lineage {entry['column']}: silver layer missing 'table' or 'tables'"
            )

    def test_gold_layer_has_required_keys(self):
        """Gold layer entries should have column name and formula or note."""
        for entry in self.data["lineages"]:
            gold = entry["layers"].get("gold")
            if gold is None:
                continue
            assert "column" in gold, (
                f"Lineage {entry['column']}: gold layer missing 'column'"
            )

    def test_required_columns_have_lineage(self):
        """All key columns must have a lineage entry."""
        lineage_columns = {e["column"] for e in self.data["lineages"]}
        missing = REQUIRED_LINEAGE_COLUMNS - lineage_columns
        assert not missing, f"Required columns missing lineage: {missing}"

    def test_minimum_lineage_count(self):
        """Should have at least 9 lineage entries (the required columns)."""
        assert len(self.data["lineages"]) >= 9, (
            f"Expected 9+ lineages, got {len(self.data['lineages'])}"
        )

    def test_trade_price_traces_to_proto(self):
        """trade_price should trace to a proto field like tradePrice."""
        tp = next(
            (e for e in self.data["lineages"] if e["column"] == "trade_price"),
            None,
        )
        assert tp is not None, "trade_price lineage not found"
        proto_field = tp["layers"]["proto"]["field"].lower()
        assert "price" in proto_field, (
            f"trade_price proto field should contain 'price', got: {proto_field}"
        )

    def test_contract_size_traces_to_instrument_proto(self):
        """contract_size should trace to a TradableInstrument subtype proto."""
        cs = next(
            (e for e in self.data["lineages"] if e["column"] == "contract_size"),
            None,
        )
        assert cs is not None, "contract_size lineage not found"
        proto_msg = cs["layers"]["proto"]["message"]
        # Accept any TradableInstrument subtype (Option, Future, Stock, etc.)
        messages = _parse_composite_messages(proto_msg)
        has_instrument = any(m in INSTRUMENT_SUBTYPES for m in messages)
        assert has_instrument, (
            f"contract_size proto message should reference a TradableInstrument type, got: {proto_msg}"
        )


class TestFieldLineageCrossReferences:
    """Validate that lineage entries cross-reference correctly against other metadata files."""

    @pytest.fixture(autouse=True)
    def load_all_yaml(self):
        lineage_path = METADATA_DIR / "field_lineage.yaml"
        proto_path = METADATA_DIR / "proto_fields.yaml"
        transforms_path = METADATA_DIR / "data_loader_transforms.yaml"

        assert lineage_path.exists(), f"Missing {lineage_path}"
        assert proto_path.exists(), f"Missing {proto_path}"
        assert transforms_path.exists(), f"Missing {transforms_path}"

        with open(lineage_path) as f:
            self.lineage = yaml.safe_load(f)
        with open(proto_path) as f:
            self.proto = yaml.safe_load(f)
        with open(transforms_path) as f:
            self.transforms = yaml.safe_load(f)

    def _get_proto_messages(self) -> set[str]:
        return {m["name"] for m in self.proto["messages"]}

    def _get_transform_columns(self, table: str) -> set[str]:
        for t in self.transforms["tables"]:
            if t["name"] == table:
                return {c["name"] for c in t["columns"]}
        return set()

    def test_proto_messages_exist_in_proto_fields(self):
        """Every proto message referenced in lineage should exist in proto_fields.yaml."""
        proto_messages = self._get_proto_messages() | INSTRUMENT_SUBTYPES
        errors = []
        for entry in self.lineage["lineages"]:
            msg = entry["layers"]["proto"]["message"]
            # Parse composite message references (e.g. "MarketTrade + VtCommon")
            components = _parse_composite_messages(msg)
            for component in components:
                if component not in proto_messages:
                    errors.append(
                        f"{entry['column']}: proto message '{component}' not in proto_fields.yaml"
                    )
        assert not errors, "\n".join(errors)

    def test_silver_columns_exist_in_transforms(self):
        """Every simple silver column referenced in lineage should exist in data_loader_transforms.yaml.

        Computed KPI columns have list-valued silver columns (multiple input fields)
        which are validated structurally but not cross-referenced individually.
        """
        errors = []
        for entry in self.lineage["lineages"]:
            silver = entry["layers"].get("silver")
            if silver is None:
                continue
            col = silver["column"]
            # Skip list-valued columns (computed fields list their input columns)
            if _is_list_column(col):
                continue
            # Skip descriptive columns (contain spaces or slashes)
            if " " in col or "/" in col:
                continue
            tables = silver.get(
                "tables", [silver["table"]] if "table" in silver else []
            )
            # Skip tables not in our transforms (e.g. 'instruments' from enrichment)
            data_loader_tables = {t["name"] for t in self.transforms["tables"]}
            relevant_tables = [t for t in tables if t in data_loader_tables]
            if not relevant_tables:
                continue
            # Check if column exists in at least one referenced table
            found = any(
                col in self._get_transform_columns(table) for table in relevant_tables
            )
            if not found:
                errors.append(
                    f"{entry['column']}: silver column '{col}' not found in transforms for tables {relevant_tables}"
                )
        assert not errors, "\n".join(errors)
