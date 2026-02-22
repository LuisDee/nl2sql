"""Unit tests for scripts/populate_glossary.py."""

from unittest.mock import MagicMock

from nl2sql_agent.config import settings


class TestBuildGlossaryEmbeddingText:
    """Tests for glossary embedding text builder."""

    def test_basic_format(self):
        """Embedding text follows the template: {name}: {definition}. Also known as: {synonyms}."""
        from scripts.populate_glossary import build_glossary_embedding_text

        result = build_glossary_embedding_text(
            name="total PnL",
            definition="Sum of instant_pnl across all trades.",
            synonyms=["total profit", "aggregate pnl"],
        )
        assert result == (
            "total PnL: Sum of instant_pnl across all trades. "
            "Also known as: total profit, aggregate pnl"
        )

    def test_empty_synonyms(self):
        """With no synonyms, 'Also known as' section is omitted."""
        from scripts.populate_glossary import build_glossary_embedding_text

        result = build_glossary_embedding_text(
            name="test concept",
            definition="A test definition.",
            synonyms=[],
        )
        assert result == "test concept: A test definition."
        assert "Also known as" not in result

    def test_single_synonym(self):
        """Single synonym is included without a trailing comma."""
        from scripts.populate_glossary import build_glossary_embedding_text

        result = build_glossary_embedding_text(
            name="edge",
            definition="Instantaneous edge per unit.",
            synonyms=["trading edge"],
        )
        assert result == (
            "edge: Instantaneous edge per unit. Also known as: trading edge"
        )


class TestPopulateGlossaryEmbeddings:
    """Tests for populate_glossary_embeddings() MERGE SQL generation."""

    def _make_glossary_entries(self):
        return [
            {
                "name": "total PnL",
                "definition": "Sum of instant_pnl across all trades.",
                "synonyms": ["total profit", "aggregate pnl"],
                "related_columns": ["markettrade.instant_pnl"],
                "category": "performance",
                "sql_pattern": "SUM(instant_pnl)",
            },
            {
                "name": "edge",
                "definition": "Instantaneous edge per unit at trade execution.",
                "synonyms": ["trading edge", "capture"],
                "related_columns": ["markettrade.instant_edge"],
                "category": "performance",
                "sql_pattern": "AVG(instant_edge)",
            },
        ]

    def test_generates_merge_sql(self):
        """populate_glossary_embeddings() generates MERGE SQL."""
        from scripts.populate_glossary import populate_glossary_embeddings

        mock_bq = MagicMock()
        entries = self._make_glossary_entries()

        count = populate_glossary_embeddings(mock_bq, entries, settings)

        assert count == 2
        mock_bq.execute_query.assert_called_once()
        sql = mock_bq.execute_query.call_args[0][0]
        assert "MERGE" in sql
        assert "glossary_embeddings" in sql

    def test_merge_on_name(self):
        """MERGE uses ON target.name = source.name."""
        from scripts.populate_glossary import populate_glossary_embeddings

        mock_bq = MagicMock()
        entries = self._make_glossary_entries()

        populate_glossary_embeddings(mock_bq, entries, settings)

        sql = mock_bq.execute_query.call_args[0][0]
        assert "target.name = source.name" in sql

    def test_includes_embedding_text(self):
        """MERGE SQL includes the computed embedding_text."""
        from scripts.populate_glossary import populate_glossary_embeddings

        mock_bq = MagicMock()
        entries = self._make_glossary_entries()

        populate_glossary_embeddings(mock_bq, entries, settings)

        sql = mock_bq.execute_query.call_args[0][0]
        assert "embedding_text" in sql
        assert "total PnL:" in sql

    def test_includes_payload_columns(self):
        """MERGE SQL includes category, sql_pattern, related_columns as payload."""
        from scripts.populate_glossary import populate_glossary_embeddings

        mock_bq = MagicMock()
        entries = self._make_glossary_entries()

        populate_glossary_embeddings(mock_bq, entries, settings)

        sql = mock_bq.execute_query.call_args[0][0]
        assert "category" in sql
        assert "sql_pattern" in sql
        assert "related_columns" in sql

    def test_nulls_embedding_on_update(self):
        """MERGE UPDATE SET nulls the embedding so generate-embeddings picks it up."""
        from scripts.populate_glossary import populate_glossary_embeddings

        mock_bq = MagicMock()
        entries = self._make_glossary_entries()

        populate_glossary_embeddings(mock_bq, entries, settings)

        sql = mock_bq.execute_query.call_args[0][0]
        assert "embedding = NULL" in sql

    def test_handles_optional_fields_as_null(self):
        """Entries without category or sql_pattern get CAST(NULL AS STRING)."""
        from scripts.populate_glossary import populate_glossary_embeddings

        mock_bq = MagicMock()
        entries = [
            {
                "name": "test",
                "definition": "A test concept.",
                "synonyms": [],
                "related_columns": ["markettrade.col1"],
            },
        ]

        populate_glossary_embeddings(mock_bq, entries, settings)

        sql = mock_bq.execute_query.call_args[0][0]
        assert "CAST(NULL AS STRING)" in sql

    def test_returns_entry_count(self):
        """Returns the number of entries processed."""
        from scripts.populate_glossary import populate_glossary_embeddings

        mock_bq = MagicMock()
        entries = self._make_glossary_entries()

        count = populate_glossary_embeddings(mock_bq, entries, settings)
        assert count == 2
