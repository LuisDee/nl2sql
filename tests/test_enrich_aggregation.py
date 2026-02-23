"""Tests for the aggregation and filterable enrichment script.

Validates that scripts/enrich_aggregation.py correctly:
- Assigns typical_aggregation to measure columns based on name patterns
- Assigns filterable flag to dimension/identifier/time columns
- Preserves existing values
- Is idempotent
"""

from __future__ import annotations

import copy

import pytest
from scripts.enrich_aggregation import (
    assign_aggregation,
    assign_filterable,
    enrich_table_aggregation,
)

# ---------------------------------------------------------------------------
# Test: Aggregation assignment
# ---------------------------------------------------------------------------


class TestAssignAggregation:
    """Verify aggregation heuristics for measure columns."""

    # SUM: PnL, edge, slippage, fees
    @pytest.mark.parametrize(
        "name",
        [
            "instant_pnl",
            "instant_pnl_w_fees",
            "instant_edge",
            "delta_slippage_1s",
            "vol_slippage_5s",
            "roll_slippage_1s",
            "other_slippage_1s",
            "fees",
            "total_slippage_1s",
        ],
    )
    def test_sum_for_pnl_edge_slippage(self, name):
        assert assign_aggregation(name) == "SUM"

    # SUM: size/volume
    @pytest.mark.parametrize(
        "name",
        [
            "trade_size",
            "trade_volume",
            "traded_size",
            "abs_traded_size",
            "routed_size",
            "bid_volume_0",
            "ask_volume_0",
        ],
    )
    def test_sum_for_size_volume(self, name):
        assert assign_aggregation(name) == "SUM"

    # AVG: price, TV, greeks
    @pytest.mark.parametrize(
        "name",
        [
            "price",
            "trade_price",
            "avg_trade_price",
            "tv",
            "mid_tv",
            "ref_tv",
            "delta",
            "gamma",
            "vega",
            "theta",
            "rho",
            "strike",
            "ref_delta",
            "ref_gamma",
        ],
    )
    def test_avg_for_price_tv_greeks(self, name):
        assert assign_aggregation(name) == "AVG"

    # AVG: ratios and multipliers
    @pytest.mark.parametrize(
        "name",
        [
            "buy_sell_multiplier",
            "market_mako_multiplier",
            "contract_size",
            "leg_ratio",
        ],
    )
    def test_avg_for_multipliers_ratios(self, name):
        assert assign_aggregation(name) == "AVG"

    # AVG: per-unit metrics
    @pytest.mark.parametrize(
        "name",
        [
            "delta_slippage_1s_per_unit",
            "vol_slippage_5s_per_unit",
            "roll_slippage_1s_per_unit",
            "other_slippage_1s_per_unit",
        ],
    )
    def test_avg_for_per_unit(self, name):
        assert assign_aggregation(name) == "AVG"

    # AVG: base values, adjustments (intermediates)
    @pytest.mark.parametrize(
        "name",
        [
            "mid_base_val",
            "raw_bv_bid",
            "raw_bv_ask",
            "base_adjustment",
            "odr_adjustment",
            "layered_base_adjustment",
            "calculated_base_val_1s",
            "vol_path_estimate_1s",
        ],
    )
    def test_avg_for_intermediates(self, name):
        assert assign_aggregation(name) == "AVG"

    def test_returns_none_for_unknown(self):
        """Unknown measure columns should still get an aggregation."""
        result = assign_aggregation("some_unknown_metric")
        assert result in ("SUM", "AVG")


# ---------------------------------------------------------------------------
# Test: Filterable assignment
# ---------------------------------------------------------------------------


class TestAssignFilterable:
    """Verify filterable flag assignment."""

    @pytest.mark.parametrize(
        "name,category",
        [
            ("symbol", "dimension"),
            ("portfolio", "dimension"),
            ("algo", "dimension"),
            ("currency", "dimension"),
            ("trade_aggressor_side", "dimension"),
            ("option_type_name", "dimension"),
            ("is_parent", "dimension"),
            ("instrument_hash", "identifier"),
            ("trade_date", "time"),
        ],
    )
    def test_filterable_true(self, name, category):
        assert assign_filterable(name, category) is True

    @pytest.mark.parametrize(
        "name,category",
        [
            ("instant_edge", "measure"),
            ("delta_slippage_1s", "measure"),
            ("price", "measure"),
        ],
    )
    def test_measures_not_filterable(self, name, category):
        assert assign_filterable(name, category) is False

    def test_non_partition_time_not_filterable(self):
        """Timestamps other than trade_date are not typically filtered on."""
        assert assign_filterable("exchange_timestamp", "time") is False


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
            {
                "name": "trade_date",
                "type": "DATE",
                "description": "Date",
                "category": "time",
            },
            {
                "name": "symbol",
                "type": "STRING",
                "description": "Ticker",
                "category": "dimension",
            },
            {
                "name": "instrument_hash",
                "type": "STRING",
                "description": "Hash",
                "category": "identifier",
            },
            {
                "name": "instant_pnl",
                "type": "FLOAT",
                "description": "PnL",
                "category": "measure",
                "formula": "...",
            },
            {
                "name": "price",
                "type": "FLOAT",
                "description": "Price",
                "category": "measure",
            },
        ],
    }
}


class TestEnrichTableAggregation:
    """Verify table-level aggregation + filterable enrichment."""

    def test_measures_get_aggregation(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result = enrich_table_aggregation(data)
        pnl = next(c for c in result["table"]["columns"] if c["name"] == "instant_pnl")
        assert pnl["typical_aggregation"] == "SUM"
        price = next(c for c in result["table"]["columns"] if c["name"] == "price")
        assert price["typical_aggregation"] == "AVG"

    def test_non_measures_no_aggregation(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result = enrich_table_aggregation(data)
        sym = next(c for c in result["table"]["columns"] if c["name"] == "symbol")
        assert "typical_aggregation" not in sym

    def test_dimensions_get_filterable(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result = enrich_table_aggregation(data)
        sym = next(c for c in result["table"]["columns"] if c["name"] == "symbol")
        assert sym["filterable"] is True

    def test_measures_not_filterable(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result = enrich_table_aggregation(data)
        pnl = next(c for c in result["table"]["columns"] if c["name"] == "instant_pnl")
        assert "filterable" not in pnl or pnl.get("filterable") is False

    def test_preserves_existing(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        data["table"]["columns"][3]["typical_aggregation"] = "AVG"  # override PnL
        result, stats = enrich_table_aggregation(data, return_stats=True)
        pnl = next(c for c in result["table"]["columns"] if c["name"] == "instant_pnl")
        assert pnl["typical_aggregation"] == "AVG"
        assert stats["agg_preserved"] >= 1

    def test_returns_stats(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result, stats = enrich_table_aggregation(data, return_stats=True)
        assert stats["agg_assigned"] == 2
        assert stats["filterable_assigned"] >= 2

    def test_idempotent(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result1 = enrich_table_aggregation(data)
        result2 = enrich_table_aggregation(copy.deepcopy(result1))
        assert result1 == result2
