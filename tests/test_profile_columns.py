"""Tests for the BQ data profiling script.

Validates that scripts/profile_columns.py correctly:
- Classifies cardinality into tiers (comprehensive, illustrative, skip)
- Builds correct BQ profiling SQL
- Transforms BQ results into example_values + comprehensive flag
- Preserves existing example_values
- Caps example_values at 25 (schema.py constraint)
"""

from __future__ import annotations

import copy

from scripts.profile_columns import (
    Cardinality,
    build_profiling_sql,
    classify_cardinality,
    enrich_table_example_values,
    transform_profiling_results,
)

# ---------------------------------------------------------------------------
# Cardinality classification
# ---------------------------------------------------------------------------


class TestClassifyCardinality:
    """Verify cardinality tier classification."""

    def test_low_cardinality_comprehensive(self):
        assert classify_cardinality(5) == Cardinality.COMPREHENSIVE

    def test_boundary_24_comprehensive(self):
        assert classify_cardinality(24) == Cardinality.COMPREHENSIVE

    def test_boundary_25_illustrative(self):
        assert classify_cardinality(25) == Cardinality.ILLUSTRATIVE

    def test_mid_cardinality_illustrative(self):
        assert classify_cardinality(100) == Cardinality.ILLUSTRATIVE

    def test_boundary_250_illustrative(self):
        assert classify_cardinality(250) == Cardinality.ILLUSTRATIVE

    def test_boundary_251_skip(self):
        assert classify_cardinality(251) == Cardinality.SKIP

    def test_high_cardinality_skip(self):
        assert classify_cardinality(10000) == Cardinality.SKIP

    def test_zero_skip(self):
        assert classify_cardinality(0) == Cardinality.SKIP

    def test_one_comprehensive(self):
        assert classify_cardinality(1) == Cardinality.COMPREHENSIVE


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------


class TestBuildProfilingSql:
    """Verify profiling SQL generation."""

    def test_generates_valid_sql(self):
        sql = build_profiling_sql(
            project="my-project",
            dataset="my_dataset",
            table="my_table",
            columns=["portfolio", "currency"],
            trade_date="2026-02-17",
        )
        assert "my-project.my_dataset.my_table" in sql
        assert "portfolio" in sql
        assert "currency" in sql
        assert "2026-02-17" in sql

    def test_includes_approx_functions(self):
        sql = build_profiling_sql(
            project="p",
            dataset="d",
            table="t",
            columns=["col1"],
            trade_date="2026-02-17",
        )
        assert "APPROX_COUNT_DISTINCT" in sql
        assert "APPROX_TOP_COUNT" in sql

    def test_empty_columns_returns_none(self):
        result = build_profiling_sql(
            project="p",
            dataset="d",
            table="t",
            columns=[],
            trade_date="2026-02-17",
        )
        assert result is None

    def test_top_count_limit(self):
        """Should request enough top values for comprehensive tier (up to 25)."""
        sql = build_profiling_sql(
            project="p",
            dataset="d",
            table="t",
            columns=["col1"],
            trade_date="2026-02-17",
        )
        assert "25" in sql


# ---------------------------------------------------------------------------
# Result transformation
# ---------------------------------------------------------------------------


class TestTransformProfilingResults:
    """Verify BQ result rows → example_values mapping."""

    def test_comprehensive_tier(self):
        """Low cardinality → store all values, comprehensive=True."""
        rows = [
            {
                "column_name": "option_type",
                "approx_distinct": 2,
                "top_values": [
                    {"value": "Call", "count": 100},
                    {"value": "Put", "count": 50},
                ],
            }
        ]
        result = transform_profiling_results(rows)
        assert result["option_type"]["example_values"] == ["Call", "Put"]
        assert result["option_type"]["comprehensive"] is True

    def test_illustrative_tier(self):
        """Medium cardinality → store top 10, comprehensive=False."""
        top = [{"value": f"val_{i}", "count": 100 - i} for i in range(50)]
        rows = [
            {
                "column_name": "portfolio",
                "approx_distinct": 50,
                "top_values": top,
            }
        ]
        result = transform_profiling_results(rows)
        assert len(result["portfolio"]["example_values"]) == 10
        assert result["portfolio"]["comprehensive"] is False

    def test_skip_tier(self):
        """High cardinality → not included in results."""
        rows = [
            {
                "column_name": "instrument_hash",
                "approx_distinct": 5000,
                "top_values": [{"value": "abc", "count": 1}],
            }
        ]
        result = transform_profiling_results(rows)
        assert "instrument_hash" not in result

    def test_null_values_excluded(self):
        """NULL values from BQ should be filtered out."""
        rows = [
            {
                "column_name": "side",
                "approx_distinct": 3,
                "top_values": [
                    {"value": "BUY", "count": 100},
                    {"value": None, "count": 50},
                    {"value": "SELL", "count": 30},
                ],
            }
        ]
        result = transform_profiling_results(rows)
        assert None not in result["side"]["example_values"]
        assert result["side"]["example_values"] == ["BUY", "SELL"]

    def test_values_sorted_by_count(self):
        """Values should be ordered by frequency (most common first)."""
        rows = [
            {
                "column_name": "exchange",
                "approx_distinct": 5,
                "top_values": [
                    {"value": "CBOE", "count": 500},
                    {"value": "ISE", "count": 300},
                    {"value": "PHLX", "count": 100},
                    {"value": "AMEX", "count": 50},
                    {"value": "NASDAQ", "count": 10},
                ],
            }
        ]
        result = transform_profiling_results(rows)
        assert result["exchange"]["example_values"] == [
            "CBOE",
            "ISE",
            "PHLX",
            "AMEX",
            "NASDAQ",
        ]


# ---------------------------------------------------------------------------
# Table-level enrichment
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
                "name": "portfolio",
                "type": "STRING",
                "description": "Portfolio",
                "category": "dimension",
            },
            {
                "name": "exchange",
                "type": "STRING",
                "description": "Exchange",
                "category": "dimension",
            },
            {
                "name": "instrument_hash",
                "type": "INTEGER",
                "description": "Hash",
                "category": "identifier",
            },
            {
                "name": "trade_price",
                "type": "FLOAT",
                "description": "Price",
                "category": "measure",
            },
            {
                "name": "option_type",
                "type": "STRING",
                "description": "Type",
                "category": "dimension",
                "example_values": ["Call", "Put"],
                "comprehensive": True,
            },
        ],
    }
}


class TestEnrichTableExampleValues:
    """Verify table-level example_values enrichment."""

    def test_applies_profiling_results(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        profiling = {
            "portfolio": {
                "example_values": ["MON", "WHITE", "RED"],
                "comprehensive": True,
            },
            "exchange": {"example_values": ["CBOE", "ISE"], "comprehensive": True},
        }
        result, stats = enrich_table_example_values(data, profiling, return_stats=True)
        port = next(c for c in result["table"]["columns"] if c["name"] == "portfolio")
        assert port["example_values"] == ["MON", "WHITE", "RED"]
        assert port["comprehensive"] is True
        assert stats["assigned"] == 2

    def test_preserves_existing(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        profiling = {
            "option_type": {"example_values": ["C", "P"], "comprehensive": True},
        }
        result, stats = enrich_table_example_values(data, profiling, return_stats=True)
        ot = next(c for c in result["table"]["columns"] if c["name"] == "option_type")
        assert ot["example_values"] == ["Call", "Put"]  # preserved, not overwritten
        assert stats["preserved"] == 1

    def test_skips_measures(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        profiling = {
            "trade_price": {"example_values": [1.5, 2.0], "comprehensive": False},
        }
        result, stats = enrich_table_example_values(data, profiling, return_stats=True)
        price = next(
            c for c in result["table"]["columns"] if c["name"] == "trade_price"
        )
        assert "example_values" not in price

    def test_idempotent(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        profiling = {
            "portfolio": {"example_values": ["MON", "WHITE"], "comprehensive": True},
        }
        result1, _ = enrich_table_example_values(data, profiling, return_stats=True)
        result2, _ = enrich_table_example_values(
            copy.deepcopy(result1), profiling, return_stats=True
        )
        assert result1 == result2

    def test_returns_columns_to_profile(self):
        """get_columns_to_profile should return only dim/id STRING columns without existing example_values."""
        from scripts.profile_columns import get_columns_to_profile

        data = copy.deepcopy(SAMPLE_TABLE)
        cols = get_columns_to_profile(data)
        names = [c["name"] for c in cols]
        assert "portfolio" in names
        assert "exchange" in names
        assert "option_type" not in names  # already has example_values
        assert "trade_price" not in names  # measure
        assert "trade_date" not in names  # time
