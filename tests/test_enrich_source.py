"""Tests for the source field enrichment script.

Validates that scripts/enrich_source.py correctly:
- Builds proto field → BQ column name mapping
- Maps data table columns to proto message.field origins
- Maps KPI columns to kpi_computations references
- Preserves existing source fields
- Is idempotent
"""

from __future__ import annotations

import copy

from scripts.enrich_source import (
    build_kpi_source_map,
    build_proto_column_map,
    enrich_table_source,
)

# ---------------------------------------------------------------------------
# Proto mapping
# ---------------------------------------------------------------------------


class TestBuildProtoColumnMap:
    """Verify proto field → BQ column name mapping."""

    def test_basic_mapping(self):
        transforms = {
            "tables": [
                {
                    "name": "markettrade",
                    "columns": [
                        {
                            "name": "trade_price",
                            "source_field": "props.tradePrice",
                            "transformation": "unnest",
                        },
                        {
                            "name": "trade_size",
                            "source_field": "tradeSize",
                            "transformation": "rename",
                        },
                    ],
                }
            ]
        }
        proto_to_bq = {
            "markettrade": {
                "message": "MarketTrade",
                "file": "data/MarketTrade.proto",
            }
        }
        result = build_proto_column_map("markettrade", transforms, proto_to_bq)
        assert (
            result["trade_price"] == "MarketTrade.tradePrice (data/MarketTrade.proto)"
        )
        assert result["trade_size"] == "MarketTrade.tradeSize (data/MarketTrade.proto)"

    def test_direct_transform(self):
        transforms = {
            "tables": [
                {
                    "name": "markettrade",
                    "columns": [
                        {
                            "name": "kafka_partition",
                            "source_field": "kafka_partition",
                            "transformation": "direct",
                        },
                    ],
                }
            ]
        }
        proto_to_bq = {
            "markettrade": {
                "message": "MarketTrade",
                "file": "data/MarketTrade.proto",
            }
        }
        result = build_proto_column_map("markettrade", transforms, proto_to_bq)
        assert result["kafka_partition"] == "kafka infrastructure"

    def test_derive_transform(self):
        transforms = {
            "tables": [
                {
                    "name": "markettrade",
                    "columns": [
                        {
                            "name": "trade_date",
                            "source_field": "derived",
                            "transformation": "derive",
                        },
                    ],
                }
            ]
        }
        proto_to_bq = {
            "markettrade": {"message": "MarketTrade", "file": "data/MarketTrade.proto"}
        }
        result = build_proto_column_map("markettrade", transforms, proto_to_bq)
        assert result["trade_date"] == "derived (data-loader)"

    def test_enrichment_columns(self):
        transforms = {
            "tables": [
                {
                    "name": "markettrade",
                    "columns": [],
                    "enrichment_columns_from_instruments": [
                        "mako_symbol",
                        "currency",
                    ],
                }
            ]
        }
        proto_to_bq = {
            "markettrade": {"message": "MarketTrade", "file": "data/MarketTrade.proto"}
        }
        result = build_proto_column_map("markettrade", transforms, proto_to_bq)
        assert result["mako_symbol"] == "instrument enrichment (data-loader)"
        assert result["currency"] == "instrument enrichment (data-loader)"

    def test_unknown_table(self):
        result = build_proto_column_map("nonexistent", {"tables": []}, {})
        assert result == {}


# ---------------------------------------------------------------------------
# KPI source mapping
# ---------------------------------------------------------------------------


class TestBuildKpiSourceMap:
    """Verify KPI computation source mapping."""

    def test_formula_columns(self):
        kpi_yaml = {
            "trade_types": {
                "markettrade": {
                    "metrics": {
                        "instant_edge": {
                            "formula": "CASE WHEN side = 'BUY' THEN ...",
                        }
                    }
                }
            }
        }
        result = build_kpi_source_map("markettrade", kpi_yaml)
        assert result["instant_edge"] == "KPI computation (markettrade.instant_edge)"

    def test_intermediate_calculations(self):
        kpi_yaml = {
            "trade_types": {
                "markettrade": {
                    "intermediate_calculations": {
                        "mid_base_val": {"formula": "(bid + ask) / 2"}
                    },
                    "metrics": {},
                }
            }
        }
        result = build_kpi_source_map("markettrade", kpi_yaml)
        assert result["mid_base_val"] == "KPI intermediate (markettrade.mid_base_val)"

    def test_shared_formulas(self):
        kpi_yaml = {
            "shared_formulas": {"buy_sell_multiplier": {"formula": "CASE ..."}},
            "trade_types": {"markettrade": {"metrics": {}}},
        }
        result = build_kpi_source_map("markettrade", kpi_yaml)
        assert (
            result["buy_sell_multiplier"] == "KPI shared formula (buy_sell_multiplier)"
        )


# ---------------------------------------------------------------------------
# Table enrichment
# ---------------------------------------------------------------------------

SAMPLE_TABLE = {
    "table": {
        "name": "test_table",
        "dataset": "{data_dataset}",
        "fqn": "{project}.{data_dataset}.test_table",
        "layer": "data",
        "description": "Test",
        "partition_field": "trade_date",
        "columns": [
            {"name": "trade_price", "type": "FLOAT", "description": "Price"},
            {"name": "trade_size", "type": "INTEGER", "description": "Size"},
            {
                "name": "portfolio",
                "type": "STRING",
                "description": "Portfolio",
                "source": "existing_source",
            },
            {"name": "unknown_col", "type": "STRING", "description": "Unknown"},
        ],
    }
}


class TestEnrichTableSource:
    """Verify table-level source enrichment."""

    def test_applies_source_mapping(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        source_map = {
            "trade_price": "MarketTrade.tradePrice (data/MarketTrade.proto)",
            "trade_size": "MarketTrade.tradeSize (data/MarketTrade.proto)",
        }
        result, stats = enrich_table_source(data, source_map, return_stats=True)
        tp = next(c for c in result["table"]["columns"] if c["name"] == "trade_price")
        assert tp["source"] == "MarketTrade.tradePrice (data/MarketTrade.proto)"
        assert stats["assigned"] >= 1

    def test_preserves_existing(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        source_map = {"portfolio": "something_else"}
        result, stats = enrich_table_source(data, source_map, return_stats=True)
        port = next(c for c in result["table"]["columns"] if c["name"] == "portfolio")
        assert port["source"] == "existing_source"
        assert stats["preserved"] == 1

    def test_skips_unknown_columns(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        source_map = {"trade_price": "MarketTrade.tradePrice (data/MarketTrade.proto)"}
        result, stats = enrich_table_source(data, source_map, return_stats=True)
        unk = next(c for c in result["table"]["columns"] if c["name"] == "unknown_col")
        assert "source" not in unk

    def test_idempotent(self):
        data = copy.deepcopy(SAMPLE_TABLE)
        source_map = {"trade_price": "MarketTrade.tradePrice (data/MarketTrade.proto)"}
        result1, _ = enrich_table_source(data, source_map, return_stats=True)
        result2, _ = enrich_table_source(
            copy.deepcopy(result1), source_map, return_stats=True
        )
        assert result1 == result2
