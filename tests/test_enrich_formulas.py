"""Tests for the formula enrichment script.

Validates that scripts/enrich_formulas.py correctly:
- Builds a formula index from kpi_computations.yaml
- Expands per-interval metrics to concrete column names
- Resolves shared_formulas references
- Adds missing formulas to YAML columns
- Updates mismatched formulas
- Preserves correct formulas
- Is idempotent
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
from scripts.enrich_formulas import (
    build_formula_index,
    enrich_table_yaml,
    get_all_intervals,
    load_kpi_computations,
)

METADATA_DIR = Path(__file__).resolve().parent.parent / "metadata"
KPI_COMPUTATIONS_PATH = METADATA_DIR / "kpi_computations.yaml"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def computations() -> dict:
    """Load the real kpi_computations.yaml once for all tests."""
    return load_kpi_computations(KPI_COMPUTATIONS_PATH)


@pytest.fixture(scope="module")
def intervals(computations) -> list[str]:
    """All interval names from kpi_computations."""
    return get_all_intervals(computations)


@pytest.fixture(scope="module")
def markettrade_index(computations, intervals) -> dict[str, str]:
    """Formula index for the markettrade trade type."""
    return build_formula_index(computations, "markettrade", intervals)


@pytest.fixture(scope="module")
def quotertrade_index(computations, intervals) -> dict[str, str]:
    """Formula index for the quotertrade trade type."""
    return build_formula_index(computations, "quotertrade", intervals)


@pytest.fixture(scope="module")
def otoswing_index(computations, intervals) -> dict[str, str]:
    """Formula index for the otoswing trade type."""
    return build_formula_index(computations, "otoswing", intervals)


@pytest.fixture(scope="module")
def brokertrade_index(computations, intervals) -> dict[str, str]:
    """Formula index for the brokertrade trade type."""
    return build_formula_index(computations, "brokertrade", intervals)


# ---------------------------------------------------------------------------
# Test: Interval extraction
# ---------------------------------------------------------------------------


class TestIntervalExtraction:
    """Verify interval names are correctly extracted from kpi_computations."""

    def test_intraday_intervals_present(self, intervals):
        for name in ["1s", "5s", "10s", "30s", "1m", "5m", "30m", "1h"]:
            assert name in intervals, f"Missing intraday interval: {name}"

    def test_multiday_intervals_present(self, intervals):
        for name in ["eod", "1D", "5D"]:
            assert name in intervals, f"Missing multiday interval: {name}"

    def test_trade_interval_present(self, intervals):
        assert "trade" in intervals

    def test_10m_interval_present(self, intervals):
        """10m is eod_only but still a valid interval."""
        assert "10m" in intervals

    def test_extended_multiday_present(self, intervals):
        """ICE/ARB children intervals."""
        for name in ["10D", "20D", "50D", "80D"]:
            assert name in intervals


# ---------------------------------------------------------------------------
# Test: Formula index building
# ---------------------------------------------------------------------------


class TestFormulaIndexBuilding:
    """Verify formula index is correctly built from kpi_computations."""

    def test_non_interval_metric_indexed(self, markettrade_index):
        """instant_edge should be in the index with its full CASE formula."""
        assert "instant_edge" in markettrade_index
        assert "CASE WHEN" in markettrade_index["instant_edge"]
        assert "trade_price" in markettrade_index["instant_edge"]

    def test_buy_sell_multiplier_indexed(self, markettrade_index):
        """buy_sell_multiplier should be in the index."""
        assert "buy_sell_multiplier" in markettrade_index
        assert "trade_aggressor_side" in markettrade_index["buy_sell_multiplier"]

    def test_interval_metric_expanded(self, markettrade_index):
        """delta_slippage_{interval} should expand to delta_slippage_1s, etc."""
        assert "delta_slippage_1s" in markettrade_index
        assert "delta_slippage_5s" in markettrade_index
        assert "delta_slippage_eod" in markettrade_index
        # Template placeholder should NOT remain
        assert "delta_slippage_{interval}" not in markettrade_index

    def test_interval_formula_expanded(self, markettrade_index):
        """Expanded formula should replace {interval} with actual interval."""
        formula = markettrade_index["delta_slippage_1s"]
        assert "calculated_base_val_1s" in formula
        assert "{interval}" not in formula

    def test_per_unit_interval_metric_expanded(self, markettrade_index):
        """delta_slippage_{interval}_per_unit should also expand."""
        assert "delta_slippage_1s_per_unit" in markettrade_index
        assert "delta_slippage_5m_per_unit" in markettrade_index

    def test_shared_formula_adjusted_tv_resolved(self, markettrade_index):
        """adjusted_tv_{interval} should resolve to actual formula, not 'See ...'"""
        assert "adjusted_tv_1s" in markettrade_index
        formula = markettrade_index["adjusted_tv_1s"]
        assert "See shared_formulas" not in formula
        assert "tv_1s" in formula
        assert "delta_1s" in formula

    def test_vol_path_estimate_resolved(self, markettrade_index):
        """vol_path_estimate_{interval} should resolve per trade type."""
        assert "vol_path_estimate_1s" in markettrade_index
        formula = markettrade_index["vol_path_estimate_1s"]
        # markettrade uses raw_delta_mid_bv (not raw_delta)
        assert "raw_delta_mid_bv" in formula

    def test_clicktrade_vol_path_uses_raw_delta(self, computations, intervals):
        """clicktrade vol_path_estimate uses raw_delta (not raw_delta_mid_bv)."""
        idx = build_formula_index(computations, "clicktrade", intervals)
        formula = idx["vol_path_estimate_1s"]
        assert "raw_delta_1s" in formula
        assert "raw_delta_mid_bv" not in formula

    def test_intermediate_calculations_indexed(self, markettrade_index):
        """Intermediate calculations like mid_base_val should be in the index."""
        assert "mid_base_val" in markettrade_index
        assert "raw_bv_bid" in markettrade_index["mid_base_val"]

    def test_intermediate_with_variants_uses_standard(self, markettrade_index):
        """Intermediates with standard/nse_nifty use the standard variant."""
        assert "raw_delta_mid_bv" in markettrade_index
        # Standard variant uses (under - roll)
        assert "under" in markettrade_index["raw_delta_mid_bv"]

    def test_quotertrade_has_mid_tv(self, quotertrade_index):
        """quotertrade-specific metric mid_tv should be indexed."""
        assert "mid_tv" in quotertrade_index

    def test_otoswing_has_swing_mid_tv(self, otoswing_index):
        """otoswing-specific metric swing_mid_tv should be indexed."""
        assert "swing_mid_tv" in otoswing_index

    def test_otoswing_has_fired_at_variants(self, otoswing_index):
        """otoswing should have fired_at variants for interval metrics."""
        assert "delta_slippage_1s_fired_at" in otoswing_index

    def test_brokertrade_has_market_mako_multiplier(self, brokertrade_index):
        """brokertrade should have market_mako_multiplier."""
        assert "market_mako_multiplier" in brokertrade_index

    def test_instant_pnl_formula(self, markettrade_index):
        """instant_pnl should have the full CASE formula, not simplified."""
        assert "instant_pnl" in markettrade_index
        formula = markettrade_index["instant_pnl"]
        assert "CASE WHEN" in formula
        assert "contract_size" in formula


# ---------------------------------------------------------------------------
# Test: YAML enrichment
# ---------------------------------------------------------------------------

# Small fixture YAML for testing enrichment logic
SAMPLE_YAML = {
    "table": {
        "name": "markettrade",
        "dataset": "{kpi_dataset}",
        "fqn": "{project}.{kpi_dataset}.markettrade",
        "layer": "kpi",
        "description": "Test table",
        "partition_field": "trade_date",
        "columns": [
            {
                "name": "instant_edge",
                "type": "FLOAT",
                "description": "At-trade edge",
                # No formula — should be added
            },
            {
                "name": "instant_pnl",
                "type": "FLOAT",
                "description": "At-trade PnL",
                "formula": "instant_edge * trade_size * contract_size",
                # Wrong formula — should be updated
            },
            {
                "name": "buy_sell_multiplier",
                "type": "INTEGER",
                "description": "Sign convention",
                "formula": "CASE WHEN trade_aggressor_side IN ('BUY', '66') THEN -1 WHEN trade_aggressor_side IN ('SELL', '83') THEN 1 ELSE 0 END",
                # Correct formula — should be preserved
            },
            {
                "name": "delta_slippage_1s",
                "type": "FLOAT",
                "description": "Delta slippage at 1s",
                # No formula — should be added with expanded interval
            },
            {
                "name": "unrelated_column",
                "type": "STRING",
                "description": "A column not in kpi_computations",
                # No formula — should NOT be touched
            },
        ],
    }
}


class TestYamlEnrichment:
    """Verify formula enrichment of YAML column data."""

    def test_adds_missing_formula(self, markettrade_index):
        """Column without formula gets one from the index."""
        data = copy.deepcopy(SAMPLE_YAML)
        result = enrich_table_yaml(data, markettrade_index)
        ie_col = next(
            c for c in result["table"]["columns"] if c["name"] == "instant_edge"
        )
        assert "formula" in ie_col
        assert "CASE WHEN" in ie_col["formula"]

    def test_updates_mismatched_formula(self, markettrade_index):
        """Column with wrong formula gets updated."""
        data = copy.deepcopy(SAMPLE_YAML)
        result = enrich_table_yaml(data, markettrade_index)
        pnl_col = next(
            c for c in result["table"]["columns"] if c["name"] == "instant_pnl"
        )
        assert "CASE WHEN" in pnl_col["formula"]
        assert pnl_col["formula"] != "instant_edge * trade_size * contract_size"

    def test_preserves_matching_formula(self, markettrade_index):
        """Column with correct formula is not modified."""
        data = copy.deepcopy(SAMPLE_YAML)
        result = enrich_table_yaml(data, markettrade_index)
        bsm_col = next(
            c for c in result["table"]["columns"] if c["name"] == "buy_sell_multiplier"
        )
        assert bsm_col["formula"] == markettrade_index["buy_sell_multiplier"]

    def test_interval_formula_added_with_expansion(self, markettrade_index):
        """Per-interval column gets formula with {interval} replaced."""
        data = copy.deepcopy(SAMPLE_YAML)
        result = enrich_table_yaml(data, markettrade_index)
        ds_col = next(
            c for c in result["table"]["columns"] if c["name"] == "delta_slippage_1s"
        )
        assert "formula" in ds_col
        assert "calculated_base_val_1s" in ds_col["formula"]
        assert "{interval}" not in ds_col["formula"]

    def test_unrelated_column_unchanged(self, markettrade_index):
        """Columns not in the formula index are not modified."""
        data = copy.deepcopy(SAMPLE_YAML)
        result = enrich_table_yaml(data, markettrade_index)
        unrel_col = next(
            c for c in result["table"]["columns"] if c["name"] == "unrelated_column"
        )
        assert "formula" not in unrel_col

    def test_idempotent(self, markettrade_index):
        """Running enrichment twice produces the same result."""
        data = copy.deepcopy(SAMPLE_YAML)
        result1 = enrich_table_yaml(data, markettrade_index)
        result2 = enrich_table_yaml(copy.deepcopy(result1), markettrade_index)
        assert result1 == result2

    def test_returns_stats(self, markettrade_index):
        """enrich_table_yaml returns counts of added/updated/verified."""
        data = copy.deepcopy(SAMPLE_YAML)
        result, stats = enrich_table_yaml(data, markettrade_index, return_stats=True)
        assert stats["added"] >= 1  # instant_edge, delta_slippage_1s
        assert stats["updated"] >= 1  # instant_pnl
        assert stats["verified"] >= 1  # buy_sell_multiplier
        assert stats["total_in_source"] >= 3


# ---------------------------------------------------------------------------
# Test: Full integration with real kpi_computations.yaml
# ---------------------------------------------------------------------------


class TestFormulaIndexCompleteness:
    """Verify the formula index covers all expected metrics."""

    def test_markettrade_has_all_core_metrics(self, markettrade_index):
        """All core markettrade metrics should be indexed."""
        expected = [
            "buy_sell_multiplier",
            "instant_edge",
            "instant_pnl",
            "instant_pnl_w_fees",
        ]
        for name in expected:
            assert name in markettrade_index, f"Missing metric: {name}"

    def test_markettrade_has_all_slippage_types(self, markettrade_index):
        """All 4 slippage types should be expanded for at least 1s."""
        for prefix in [
            "delta_slippage",
            "roll_slippage",
            "vol_slippage",
            "other_slippage",
        ]:
            key = f"{prefix}_1s"
            assert key in markettrade_index, f"Missing: {key}"
            key_pu = f"{prefix}_1s_per_unit"
            assert key_pu in markettrade_index, f"Missing: {key_pu}"

    def test_markettrade_has_tv_change(self, markettrade_index):
        """tv_change and tv_change_buysell should be expanded."""
        assert "tv_change_1s" in markettrade_index
        assert "tv_change_buysell_1s" in markettrade_index

    def test_all_trade_types_have_indexes(self, computations, intervals):
        """All 5 trade types should produce formula indexes."""
        for tt in [
            "markettrade",
            "quotertrade",
            "otoswing",
            "clicktrade",
            "brokertrade",
        ]:
            idx = build_formula_index(computations, tt, intervals)
            assert len(idx) > 0, f"Empty index for {tt}"
            assert "instant_edge" in idx, f"Missing instant_edge for {tt}"

    def test_formula_count_reasonable(self, markettrade_index):
        """markettrade should have 100+ formulas (metrics + intermediates * intervals)."""
        # 6 intermediates + ~20 non-interval metrics + ~12 interval metrics * 17 intervals
        assert len(markettrade_index) > 100
