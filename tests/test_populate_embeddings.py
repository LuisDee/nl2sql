"""Tests for populate_embeddings.py — enriched embedding text generation."""

import pytest

from scripts.populate_embeddings import build_embedding_text


class TestBuildEmbeddingText:
    """Tests for the build_embedding_text() helper."""

    def test_includes_table_and_column_name(self):
        """Embedding text should start with table.column format."""
        result = build_embedding_text(
            table_name="markettrade",
            column_name="instant_edge",
            column_type="FLOAT64",
            layer="kpi",
            description="Instantaneous edge at the moment of trade",
            synonyms=["edge", "trading edge"],
        )
        assert "markettrade.instant_edge" in result

    def test_includes_column_type(self):
        """Embedding text should include the column type."""
        result = build_embedding_text(
            table_name="theodata",
            column_name="vol",
            column_type="FLOAT64",
            layer="data",
            description="Implied volatility",
            synonyms=["implied vol", "IV"],
        )
        assert "FLOAT64" in result

    def test_includes_layer(self):
        """Embedding text should include the layer (kpi or data)."""
        result = build_embedding_text(
            table_name="markettrade",
            column_name="instant_pnl",
            column_type="FLOAT64",
            layer="kpi",
            description="Immediate P&L at execution",
            synonyms=["pnl", "profit"],
        )
        assert "kpi" in result

    def test_includes_description(self):
        """Embedding text should include the full description."""
        desc = "Instantaneous edge at the moment of trade execution"
        result = build_embedding_text(
            table_name="markettrade",
            column_name="instant_edge",
            column_type="FLOAT64",
            layer="kpi",
            description=desc,
            synonyms=["edge"],
        )
        assert desc in result

    def test_includes_synonyms(self):
        """Embedding text should include synonym list."""
        result = build_embedding_text(
            table_name="markettrade",
            column_name="instant_edge",
            column_type="FLOAT64",
            layer="kpi",
            description="Edge at trade",
            synonyms=["edge", "trading edge", "edge_bps", "capture"],
        )
        assert "Also known as:" in result
        assert "edge" in result
        assert "trading edge" in result
        assert "edge_bps" in result
        assert "capture" in result

    def test_handles_no_synonyms(self):
        """Columns with no synonyms should not have 'Also known as:' suffix."""
        result = build_embedding_text(
            table_name="markettrade",
            column_name="trade_date",
            column_type="DATE",
            layer="kpi",
            description="The date of the trade",
            synonyms=[],
        )
        assert "Also known as:" not in result
        assert "markettrade.trade_date" in result
        assert "DATE" in result

    def test_handles_none_synonyms(self):
        """Columns with None synonyms should not crash."""
        result = build_embedding_text(
            table_name="markettrade",
            column_name="trade_date",
            column_type="DATE",
            layer="kpi",
            description="The date of the trade",
            synonyms=None,
        )
        assert "Also known as:" not in result

    def test_handles_empty_description(self):
        """Columns with empty description should still produce valid text."""
        result = build_embedding_text(
            table_name="marketdata",
            column_name="bid_price_0",
            column_type="FLOAT64",
            layer="data",
            description="",
            synonyms=["best bid"],
        )
        assert "marketdata.bid_price_0" in result
        assert "FLOAT64" in result
        assert "Also known as:" in result

    def test_full_format(self):
        """Verify the complete format of the embedding text."""
        result = build_embedding_text(
            table_name="markettrade",
            column_name="instant_edge",
            column_type="FLOAT64",
            layer="kpi",
            description="Instantaneous edge at the moment of trade",
            synonyms=["edge", "trading edge"],
        )
        # Should follow: "{table}.{col} ({type}, {layer}): {desc}. Also known as: {synonyms}"
        assert result.startswith("markettrade.instant_edge (FLOAT64, kpi)")
        assert "Instantaneous edge at the moment of trade" in result
        assert "Also known as: edge, trading edge" in result
