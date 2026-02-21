"""Unit tests for scripts/populate_embeddings.py."""

from unittest.mock import MagicMock

from nl2sql_agent.config import settings


class TestEscapeSqlString:
    def test_escapes_single_quotes(self):
        from scripts.populate_embeddings import _escape_sql_string

        assert _escape_sql_string("it's") == "it\\'s"

    def test_escapes_backslashes(self):
        from scripts.populate_embeddings import _escape_sql_string

        assert _escape_sql_string("a\\b") == "a\\\\b"

    def test_escapes_newlines(self):
        from scripts.populate_embeddings import _escape_sql_string

        assert _escape_sql_string("a\nb") == "a\\nb"

    def test_escapes_carriage_returns(self):
        from scripts.populate_embeddings import _escape_sql_string

        assert _escape_sql_string("a\rb") == "a\\rb"

    def test_no_special_chars_unchanged(self):
        from scripts.populate_embeddings import _escape_sql_string

        assert _escape_sql_string("hello world") == "hello world"

    def test_combined_escapes(self):
        from scripts.populate_embeddings import _escape_sql_string

        result = _escape_sql_string("it's a\\b\nand\rmore")
        assert "\\'" in result
        assert "\\\\" in result
        assert "\\n" in result
        assert "\\r" in result

    def test_empty_string(self):
        from scripts.populate_embeddings import _escape_sql_string

        assert _escape_sql_string("") == ""


class TestBatched:
    def test_splits_evenly(self):
        from scripts.populate_embeddings import _batched

        result = list(_batched([1, 2, 3, 4], 2))
        assert result == [[1, 2], [3, 4]]

    def test_handles_remainder(self):
        from scripts.populate_embeddings import _batched

        result = list(_batched([1, 2, 3, 4, 5], 2))
        assert result == [[1, 2], [3, 4], [5]]

    def test_empty_list(self):
        from scripts.populate_embeddings import _batched

        result = list(_batched([], 2))
        assert result == []

    def test_batch_larger_than_list(self):
        from scripts.populate_embeddings import _batched

        result = list(_batched([1, 2], 5))
        assert result == [[1, 2]]

    def test_single_element_batches(self):
        from scripts.populate_embeddings import _batched

        result = list(_batched([1, 2, 3], 1))
        assert result == [[1], [2], [3]]

    def test_batch_equals_list_size(self):
        from scripts.populate_embeddings import _batched

        result = list(_batched([1, 2, 3], 3))
        assert result == [[1, 2, 3]]


class TestBuildEmbeddingText:
    def test_basic_format(self):
        from scripts.populate_embeddings import build_embedding_text

        result = build_embedding_text(
            "markettrade", "symbol", "STRING", "kpi", "The traded symbol", None
        )
        assert "markettrade.symbol" in result
        assert "STRING" in result
        assert "kpi" in result
        assert "The traded symbol" in result

    def test_includes_synonyms(self):
        from scripts.populate_embeddings import build_embedding_text

        result = build_embedding_text(
            "markettrade",
            "instant_edge",
            "FLOAT64",
            "kpi",
            "Trading edge",
            ["edge", "alpha"],
        )
        assert "Also known as: edge, alpha" in result

    def test_no_synonyms_omits_section(self):
        from scripts.populate_embeddings import build_embedding_text

        result = build_embedding_text(
            "markettrade", "symbol", "STRING", "kpi", "desc", None
        )
        assert "Also known as" not in result

    def test_empty_synonyms_list_omits_section(self):
        from scripts.populate_embeddings import build_embedding_text

        result = build_embedding_text(
            "markettrade", "symbol", "STRING", "kpi", "desc", []
        )
        assert "Also known as" not in result

    def test_empty_description(self):
        from scripts.populate_embeddings import build_embedding_text

        result = build_embedding_text("t", "c", "STRING", "kpi", "", None)
        assert "t.c" in result
        assert "STRING" in result

    def test_header_only_when_no_description_or_synonyms(self):
        from scripts.populate_embeddings import build_embedding_text

        result = build_embedding_text("t", "c", "STRING", "kpi", "", None)
        assert result == "t.c (STRING, kpi)"

    def test_full_format(self):
        from scripts.populate_embeddings import build_embedding_text

        result = build_embedding_text(
            "markettrade",
            "instant_edge",
            "FLOAT64",
            "kpi",
            "Instantaneous edge at the moment of trade",
            ["edge", "trading edge"],
        )
        assert result.startswith("markettrade.instant_edge (FLOAT64, kpi)")
        assert "Instantaneous edge at the moment of trade" in result
        assert "Also known as: edge, trading edge" in result

    def test_description_with_synonyms_uses_colon_separator(self):
        from scripts.populate_embeddings import build_embedding_text

        result = build_embedding_text(
            "markettrade", "symbol", "STRING", "kpi", "Ticker", ["sym"]
        )
        assert ": " in result

    def test_none_synonyms_same_as_empty(self):
        from scripts.populate_embeddings import build_embedding_text

        result_none = build_embedding_text("t", "c", "STRING", "kpi", "desc", None)
        result_empty = build_embedding_text("t", "c", "STRING", "kpi", "desc", [])
        assert result_none == result_empty


class TestPopulateColumnEmbeddings:
    def _make_tables(self):
        """Create minimal table YAML structure."""
        return [
            {
                "table": {
                    "name": "markettrade",
                    "dataset": "{kpi_dataset}",
                    "layer": "kpi",
                    "columns": [
                        {
                            "name": "symbol",
                            "type": "STRING",
                            "description": "Traded symbol",
                        },
                        {
                            "name": "trade_date",
                            "type": "DATE",
                            "description": "Date of trade",
                            "synonyms": ["date"],
                        },
                    ],
                }
            },
            {
                "table": {
                    "name": "theodata",
                    "dataset": "{data_dataset}",
                    "layer": "data",
                    "columns": [
                        {
                            "name": "vol",
                            "type": "FLOAT64",
                            "description": "Implied volatility",
                            "synonyms": ["IV", "sigma"],
                        },
                    ],
                }
            },
        ]

    def test_returns_total_column_count(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        tables = self._make_tables()
        count = populate_column_embeddings(bq, tables, settings)
        assert count == 3  # 2 from markettrade + 1 from theodata

    def test_calls_bq_execute_query(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        tables = self._make_tables()
        populate_column_embeddings(bq, tables, settings)
        assert bq.execute_query.called

    def test_sql_contains_merge_into_column_embeddings(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        tables = self._make_tables()
        populate_column_embeddings(bq, tables, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        assert "MERGE" in sql
        assert "column_embeddings" in sql

    def test_sql_contains_column_names(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        tables = self._make_tables()
        populate_column_embeddings(bq, tables, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        assert "symbol" in sql
        assert "trade_date" in sql
        assert "vol" in sql

    def test_synonyms_included_in_sql(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        tables = self._make_tables()
        populate_column_embeddings(bq, tables, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        assert "IV" in sql
        assert "sigma" in sql

    def test_sql_uses_metadata_dataset(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        tables = self._make_tables()
        populate_column_embeddings(bq, tables, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        fqn = f"{settings.gcp_project}.{settings.metadata_dataset}"
        assert fqn in sql

    def test_resolves_dataset_placeholders(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        tables = self._make_tables()
        populate_column_embeddings(bq, tables, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        assert settings.kpi_dataset in sql
        assert settings.data_dataset in sql

    def test_empty_tables_returns_zero(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        count = populate_column_embeddings(bq, [], settings)
        assert count == 0
        bq.execute_query.assert_not_called()

    def test_table_with_no_columns_skipped(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        tables = [
            {
                "table": {
                    "name": "empty_table",
                    "dataset": "{kpi_dataset}",
                    "layer": "kpi",
                    "columns": [],
                }
            }
        ]
        count = populate_column_embeddings(bq, tables, settings)
        assert count == 0
        bq.execute_query.assert_not_called()

    def test_single_batch_for_small_input(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        tables = self._make_tables()
        populate_column_embeddings(bq, tables, settings)
        # 3 columns fit in a single batch (BATCH_SIZE=500)
        assert bq.execute_query.call_count == 1

    def test_sql_contains_unnest(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        tables = self._make_tables()
        populate_column_embeddings(bq, tables, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        assert "UNNEST" in sql

    def test_sql_contains_struct_for_each_column(self):
        from scripts.populate_embeddings import populate_column_embeddings

        bq = MagicMock()
        tables = self._make_tables()
        populate_column_embeddings(bq, tables, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        assert sql.count("STRUCT(") == 3


class TestPopulateQueryMemory:
    def _make_examples(self):
        return [
            {
                "question": "What was the edge today?",
                "sql": (
                    "SELECT instant_edge FROM"
                    " `{project}.{kpi_dataset}.markettrade`"
                    " WHERE trade_date = CURRENT_DATE()"
                ),
                "tables_used": ["markettrade"],
                "dataset": "{kpi_dataset}",
                "complexity": "simple",
                "routing_signal": "edge -> kpi",
                "validated_by": "human",
            },
        ]

    def test_returns_example_count(self):
        from scripts.populate_embeddings import populate_query_memory

        bq = MagicMock()
        examples = self._make_examples()
        count = populate_query_memory(bq, examples, settings)
        assert count == 1

    def test_sql_contains_merge_into_query_memory(self):
        from scripts.populate_embeddings import populate_query_memory

        bq = MagicMock()
        examples = self._make_examples()
        populate_query_memory(bq, examples, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        assert "MERGE" in sql
        assert "query_memory" in sql

    def test_resolves_project_placeholder(self):
        from scripts.populate_embeddings import populate_query_memory

        bq = MagicMock()
        examples = self._make_examples()
        populate_query_memory(bq, examples, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        assert "{project}" not in sql
        assert settings.gcp_project in sql

    def test_resolves_dataset_placeholder(self):
        from scripts.populate_embeddings import populate_query_memory

        bq = MagicMock()
        examples = self._make_examples()
        populate_query_memory(bq, examples, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        assert "{kpi_dataset}" not in sql
        assert settings.kpi_dataset in sql

    def test_sql_contains_question_text(self):
        from scripts.populate_embeddings import populate_query_memory

        bq = MagicMock()
        examples = self._make_examples()
        populate_query_memory(bq, examples, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        assert "What was the edge today?" in sql

    def test_sql_contains_tables_used(self):
        from scripts.populate_embeddings import populate_query_memory

        bq = MagicMock()
        examples = self._make_examples()
        populate_query_memory(bq, examples, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        assert "markettrade" in sql

    def test_empty_examples_returns_zero(self):
        from scripts.populate_embeddings import populate_query_memory

        bq = MagicMock()
        count = populate_query_memory(bq, [], settings)
        assert count == 0
        bq.execute_query.assert_not_called()

    def test_sql_uses_metadata_dataset(self):
        from scripts.populate_embeddings import populate_query_memory

        bq = MagicMock()
        examples = self._make_examples()
        populate_query_memory(bq, examples, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        fqn = f"{settings.gcp_project}.{settings.metadata_dataset}"
        assert fqn in sql

    def test_multiple_examples(self):
        from scripts.populate_embeddings import populate_query_memory

        bq = MagicMock()
        examples = [
            *self._make_examples(),
            {
                "question": "Show all symbols",
                "sql": (
                    "SELECT DISTINCT symbol FROM `{project}.{kpi_dataset}.markettrade`"
                ),
                "tables_used": ["markettrade"],
                "dataset": "{kpi_dataset}",
                "complexity": "simple",
                "routing_signal": "symbol -> kpi",
                "validated_by": "human",
            },
        ]
        count = populate_query_memory(bq, examples, settings)
        assert count == 2

    def test_escapes_special_chars_in_question(self):
        from scripts.populate_embeddings import populate_query_memory

        bq = MagicMock()
        examples = [
            {
                "question": "What's the edge?",
                "sql": "SELECT 1",
                "tables_used": ["markettrade"],
                "dataset": "{kpi_dataset}",
                "complexity": "simple",
                "routing_signal": "",
                "validated_by": "human",
            },
        ]
        populate_query_memory(bq, examples, settings)
        sql = bq.execute_query.call_args_list[0][0][0]
        # The single quote in "What's" should be escaped
        assert "What\\'s the edge?" in sql
