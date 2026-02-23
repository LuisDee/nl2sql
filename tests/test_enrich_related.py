"""Tests for the related columns enrichment script.

Validates that scripts/enrich_related.py correctly:
- Extracts column references from formula text
- Filters to columns that exist in the same table
- Caps at 5 related columns
- Preserves existing related_columns
- Is idempotent
"""

from __future__ import annotations

import copy

from scripts.enrich_related import (
    enrich_table_related,
    extract_formula_references,
)

# ---------------------------------------------------------------------------
# Test: Formula reference extraction
# ---------------------------------------------------------------------------


class TestExtractFormulaReferences:
    """Verify column name extraction from formula text."""

    def test_simple_formula(self):
        refs = extract_formula_references("trade_price - tv")
        assert "trade_price" in refs
        assert "tv" in refs

    def test_case_formula(self):
        formula = "CASE WHEN trade_aggressor_side = 'BUY' THEN (trade_price - tv) * trade_size * contract_size ELSE 0 END"
        refs = extract_formula_references(formula)
        assert "trade_price" in refs
        assert "tv" in refs
        assert "trade_size" in refs
        assert "contract_size" in refs
        assert "trade_aggressor_side" in refs

    def test_excludes_sql_keywords(self):
        formula = "CASE WHEN trade_side IN ('BUY', 'SELL') THEN 1 ELSE 0 END"
        refs = extract_formula_references(formula)
        assert "CASE" not in refs
        assert "WHEN" not in refs
        assert "THEN" not in refs
        assert "ELSE" not in refs
        assert "END" not in refs
        assert "IN" not in refs
        assert "trade_side" in refs

    def test_excludes_string_literals(self):
        formula = "CASE WHEN side = 'BUYSELL_BUY' THEN 1 END"
        refs = extract_formula_references(formula)
        assert "BUYSELL_BUY" not in refs
        assert "side" in refs

    def test_excludes_numbers(self):
        formula = "delta * 0.5 + gamma * 2"
        refs = extract_formula_references(formula)
        assert "delta" in refs
        assert "gamma" in refs
        assert "0" not in refs

    def test_interval_expanded_formula(self):
        formula = "((calculated_base_val_1s - mid_base_val) * delta) * trade_size * contract_size * buy_sell_multiplier"
        refs = extract_formula_references(formula)
        assert "calculated_base_val_1s" in refs
        assert "mid_base_val" in refs
        assert "delta" in refs
        assert "buy_sell_multiplier" in refs

    def test_function_calls_excluded(self):
        formula = "COALESCE(ABS(leg_ratio), 1) * contract_size"
        refs = extract_formula_references(formula)
        assert "COALESCE" not in refs
        assert "ABS" not in refs
        assert "leg_ratio" in refs
        assert "contract_size" in refs

    def test_pow_function(self):
        formula = "0.5 * ref_gamma * POW((mid_base_val - delta_adjusted_base_price), 2)"
        refs = extract_formula_references(formula)
        assert "POW" not in refs
        assert "ref_gamma" in refs
        assert "mid_base_val" in refs
        assert "delta_adjusted_base_price" in refs


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
            {"name": "trade_price", "type": "FLOAT", "description": "Price"},
            {"name": "tv", "type": "FLOAT", "description": "Theo value"},
            {"name": "trade_size", "type": "INTEGER", "description": "Size"},
            {"name": "contract_size", "type": "FLOAT", "description": "Contract"},
            {
                "name": "instant_edge",
                "type": "FLOAT",
                "description": "Edge",
                "formula": "CASE WHEN side = 'BUY' THEN (trade_price - tv) ELSE (tv - trade_price) END",
            },
            {
                "name": "instant_pnl",
                "type": "FLOAT",
                "description": "PnL",
                "formula": "instant_edge * trade_size * contract_size",
            },
            {"name": "unrelated", "type": "STRING", "description": "Other"},
        ],
    }
}


class TestEnrichTableRelated:
    """Verify table-level related columns enrichment."""

    def test_formula_columns_get_related(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result = enrich_table_related(data)
        edge = next(
            c for c in result["table"]["columns"] if c["name"] == "instant_edge"
        )
        assert "related_columns" in edge
        assert "trade_price" in edge["related_columns"]
        assert "tv" in edge["related_columns"]

    def test_non_formula_columns_no_related(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result = enrich_table_related(data)
        unrel = next(c for c in result["table"]["columns"] if c["name"] == "unrelated")
        assert "related_columns" not in unrel

    def test_related_filtered_to_table_columns(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result = enrich_table_related(data)
        edge = next(
            c for c in result["table"]["columns"] if c["name"] == "instant_edge"
        )
        table_cols = {c["name"] for c in data["table"]["columns"]}
        for rel in edge["related_columns"]:
            assert rel in table_cols

    def test_self_reference_excluded(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result = enrich_table_related(data)
        pnl = next(c for c in result["table"]["columns"] if c["name"] == "instant_pnl")
        assert "instant_pnl" not in pnl.get("related_columns", [])

    def test_capped_at_5(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        # Add a formula referencing many columns
        data["table"]["columns"].append(
            {
                "name": "complex_metric",
                "type": "FLOAT",
                "description": "Complex",
                "formula": "trade_price + tv + trade_size + contract_size + instant_edge + instant_pnl + unrelated",
            }
        )
        result = enrich_table_related(data)
        metric = next(
            c for c in result["table"]["columns"] if c["name"] == "complex_metric"
        )
        assert len(metric["related_columns"]) <= 5

    def test_preserves_existing(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        data["table"]["columns"][4]["related_columns"] = ["custom_col"]
        result, stats = enrich_table_related(data, return_stats=True)
        edge = next(
            c for c in result["table"]["columns"] if c["name"] == "instant_edge"
        )
        assert edge["related_columns"] == ["custom_col"]
        assert stats["preserved"] >= 1

    def test_returns_stats(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result, stats = enrich_table_related(data, return_stats=True)
        assert stats["assigned"] >= 1

    def test_idempotent(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        result1 = enrich_table_related(data)
        result2 = enrich_table_related(copy.deepcopy(result1))
        assert result1 == result2
