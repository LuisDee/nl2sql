"""Tests for the category assignment script.

Validates that scripts/enrich_categories.py correctly assigns one of
four categories (time, identifier, dimension, measure) to every column
using deterministic heuristic rules.
"""

from __future__ import annotations

import copy

import pytest
from scripts.enrich_categories import categorize_column, enrich_table_categories

# ---------------------------------------------------------------------------
# Test: Individual categorization rules
# ---------------------------------------------------------------------------


class TestCategorizeColumn:
    """Verify the categorize_column heuristic returns correct categories."""

    # -- time columns --
    @pytest.mark.parametrize(
        "name,col_type",
        [
            ("trade_date", "DATE"),
            ("event_date", "DATE"),
            ("exchange_date", "DATE"),
            ("transaction_timestamp", "TIMESTAMP"),
            ("exchange_timestamp", "TIMESTAMP"),
            ("hw_nic_rx_timestamp", "TIMESTAMP"),
            ("event_timestamp_ns", "INTEGER"),
            ("transaction_timestamp_ns", "INTEGER"),
            ("revision_timestamp_ns", "INTEGER"),
            ("theo_compute_timestamp_start", "TIMESTAMP"),
            ("theo_compute_timestamp_start_ns", "INTEGER"),
        ],
    )
    def test_time_columns(self, name, col_type):
        result = categorize_column(name, col_type, has_formula=False)
        assert result == "time", f"{name} ({col_type}) should be time, got {result}"

    # -- identifier columns --
    @pytest.mark.parametrize(
        "name,col_type",
        [
            ("instrument_hash", "STRING"),
            ("ref_curve_id", "INTEGER"),
            ("ref_carry_id", "INTEGER"),
            ("curve_id", "INTEGER"),
            ("carry_id", "INTEGER"),
            ("stream_id", "INTEGER"),
            ("kafka_partition", "INTEGER"),
            ("kafka_offset", "INTEGER"),
            ("sequence_number", "INTEGER"),
            ("order_id", "STRING"),
            ("trade_id", "STRING"),
            ("parent_trade_id", "STRING"),
        ],
    )
    def test_identifier_columns(self, name, col_type):
        result = categorize_column(name, col_type, has_formula=False)
        assert result == "identifier", (
            f"{name} ({col_type}) should be identifier, got {result}"
        )

    # -- dimension columns --
    @pytest.mark.parametrize(
        "name,col_type",
        [
            ("symbol", "STRING"),
            ("portfolio", "STRING"),
            ("algo", "STRING"),
            ("currency", "STRING"),
            ("exchange", "STRING"),
            ("trade_aggressor_side", "STRING"),
            ("trade_side", "STRING"),
            ("option_type_name", "STRING"),
            ("inst_type_name", "STRING"),
            ("side", "STRING"),
            ("event_type", "STRING"),
            ("is_parent", "BOOLEAN"),
            ("parent_is_combo", "BOOLEAN"),
            ("keyframe", "BOOLEAN"),
            ("is_active_instrument", "BOOLEAN"),
        ],
    )
    def test_dimension_columns(self, name, col_type):
        result = categorize_column(name, col_type, has_formula=False)
        assert result == "dimension", (
            f"{name} ({col_type}) should be dimension, got {result}"
        )

    # -- measure columns (with formula) --
    @pytest.mark.parametrize(
        "name,col_type",
        [
            ("instant_edge", "FLOAT"),
            ("instant_pnl", "FLOAT"),
            ("buy_sell_multiplier", "INTEGER"),
            ("delta_slippage_1s", "FLOAT"),
            ("vol_slippage_1s_per_unit", "FLOAT"),
            ("tv_change_buysell_1s", "FLOAT"),
        ],
    )
    def test_measure_columns_with_formula(self, name, col_type):
        result = categorize_column(name, col_type, has_formula=True)
        assert result == "measure", (
            f"{name} ({col_type}) should be measure, got {result}"
        )

    # -- measure columns (no formula, numeric non-identifier) --
    @pytest.mark.parametrize(
        "name,col_type",
        [
            ("price", "FLOAT"),
            ("tv", "FLOAT"),
            ("delta", "FLOAT"),
            ("gamma", "FLOAT"),
            ("vega", "FLOAT"),
            ("trade_price", "FLOAT"),
            ("trade_size", "INTEGER"),
            ("strike", "FLOAT"),
            ("contract_size", "FLOAT"),
            ("bid_volume_0", "INTEGER"),
            ("ask_price_0", "FLOAT"),
        ],
    )
    def test_measure_columns_numeric(self, name, col_type):
        result = categorize_column(name, col_type, has_formula=False)
        assert result == "measure", (
            f"{name} ({col_type}) should be measure, got {result}"
        )

    # -- string columns without identifier patterns → dimension --
    def test_expiry_timestamp_string_is_dimension(self):
        """expiry_timestamp is STRING type, not TIMESTAMP → dimension."""
        result = categorize_column("expiry_timestamp", "STRING", has_formula=False)
        assert result == "dimension"


# ---------------------------------------------------------------------------
# Test: Table-level enrichment
# ---------------------------------------------------------------------------

SAMPLE_TABLE = {
    "table": {
        "name": "test_table",
        "dataset": "{kpi_dataset}",
        "fqn": "{project}.{kpi_dataset}.test_table",
        "layer": "kpi",
        "description": "Test",
        "partition_field": "trade_date",
        "columns": [
            {"name": "trade_date", "type": "DATE", "description": "Date"},
            {"name": "symbol", "type": "STRING", "description": "Ticker"},
            {"name": "instrument_hash", "type": "STRING", "description": "Hash"},
            {
                "name": "instant_edge",
                "type": "FLOAT",
                "description": "Edge",
                "formula": "...",
            },
            {"name": "price", "type": "FLOAT", "description": "Price"},
            {"name": "is_parent", "type": "BOOLEAN", "description": "Flag"},
        ],
    }
}


class TestEnrichTableCategories:
    """Verify table-level category enrichment."""

    def test_all_columns_get_categories(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result = enrich_table_categories(data)
        for col in result["table"]["columns"]:
            assert "category" in col, f"Column {col['name']} missing category"

    def test_preserves_existing_category(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        data["table"]["columns"][0]["category"] = "time"
        result, stats = enrich_table_categories(data, return_stats=True)
        assert stats["preserved"] >= 1

    def test_does_not_overwrite_existing_category(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        data["table"]["columns"][4]["category"] = (
            "dimension"  # price as dimension (override)
        )
        result = enrich_table_categories(data)
        price_col = next(c for c in result["table"]["columns"] if c["name"] == "price")
        assert price_col["category"] == "dimension"

    def test_returns_stats(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result, stats = enrich_table_categories(data, return_stats=True)
        assert stats["assigned"] == 6
        assert stats["preserved"] == 0
        assert "time" in stats["by_category"]
        assert "measure" in stats["by_category"]

    def test_correct_categories_assigned(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result = enrich_table_categories(data)
        cols = {c["name"]: c["category"] for c in result["table"]["columns"]}
        assert cols["trade_date"] == "time"
        assert cols["symbol"] == "dimension"
        assert cols["instrument_hash"] == "identifier"
        assert cols["instant_edge"] == "measure"
        assert cols["price"] == "measure"
        assert cols["is_parent"] == "dimension"

    def test_idempotent(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result1 = enrich_table_categories(data)
        result2 = enrich_table_categories(copy.deepcopy(result1))
        assert result1 == result2
