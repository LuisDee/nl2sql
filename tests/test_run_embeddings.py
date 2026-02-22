"""Unit tests for the embedding pipeline (scripts/run_embeddings.py).

Tests all public functions by mocking the BigQuery client and capturing
the SQL strings passed to bq.execute_query. Asserts structure (keywords,
table names, placeholders), not exact SQL strings.
"""

from unittest.mock import MagicMock, mock_open, patch

from scripts.run_embeddings import (
    _build_routing_descriptions,
    _build_table_descriptions,
    _escape_bq_string,
    create_embedding_tables,
    create_metadata_dataset,
    create_vector_indexes,
    generate_embeddings,
    migrate_payload_columns,
    populate_schema_embeddings,
    populate_symbols,
    verify_embedding_model,
)

from nl2sql_agent.config import settings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_bq() -> MagicMock:
    """Create a MagicMock satisfying BigQueryProtocol."""
    bq = MagicMock()
    bq.execute_query.return_value = MagicMock()
    return bq


def _sql_calls(bq: MagicMock) -> list[str]:
    """Extract all SQL strings passed to bq.execute_query."""
    return [c[0][0] for c in bq.execute_query.call_args_list]


# ---------------------------------------------------------------------------
# TestCreateMetadataDataset
# ---------------------------------------------------------------------------


class TestCreateMetadataDataset:
    def test_creates_schema_with_if_not_exists(self):
        bq = _make_bq()
        create_metadata_dataset(bq, settings)

        bq.execute_query.assert_called_once()
        sql = bq.execute_query.call_args[0][0]
        assert "CREATE SCHEMA IF NOT EXISTS" in sql

    def test_includes_project_and_dataset(self):
        bq = _make_bq()
        create_metadata_dataset(bq, settings)

        sql = bq.execute_query.call_args[0][0]
        assert settings.gcp_project in sql
        assert settings.metadata_dataset in sql

    def test_includes_location(self):
        bq = _make_bq()
        create_metadata_dataset(bq, settings)

        sql = bq.execute_query.call_args[0][0]
        assert settings.bq_location in sql


# ---------------------------------------------------------------------------
# TestVerifyEmbeddingModel
# ---------------------------------------------------------------------------


class TestVerifyEmbeddingModel:
    def test_queries_information_schema(self):
        bq = _make_bq()
        verify_embedding_model(bq, settings)

        sql = bq.execute_query.call_args[0][0]
        assert "INFORMATION_SCHEMA.MODELS" in sql

    def test_includes_model_name(self):
        bq = _make_bq()
        verify_embedding_model(bq, settings)

        # The model name is the last part of the embedding_model_ref
        model_name = settings.embedding_model_ref.split(".")[-1]
        sql = bq.execute_query.call_args[0][0]
        assert model_name in sql


# ---------------------------------------------------------------------------
# TestCreateEmbeddingTables
# ---------------------------------------------------------------------------


class TestCreateEmbeddingTables:
    def test_creates_four_tables(self):
        bq = _make_bq()
        create_embedding_tables(bq, settings)

        assert bq.execute_query.call_count == 4

    def test_default_uses_if_not_exists(self):
        bq = _make_bq()
        create_embedding_tables(bq, settings, force=False)

        sqls = _sql_calls(bq)
        for sql in sqls:
            assert "CREATE OR REPLACE TABLE" not in sql
        # First three should have IF NOT EXISTS; fourth always uses it
        for sql in sqls:
            assert "IF NOT EXISTS" in sql

    def test_force_uses_create_or_replace(self):
        bq = _make_bq()
        create_embedding_tables(bq, settings, force=True)

        sqls = _sql_calls(bq)
        # First three tables use CREATE OR REPLACE
        for sql in sqls[:3]:
            assert "CREATE OR REPLACE TABLE" in sql
        # symbol_exchange_map always uses IF NOT EXISTS (hardcoded)
        assert "IF NOT EXISTS" in sqls[3]

    def test_creates_schema_embeddings_table(self):
        bq = _make_bq()
        create_embedding_tables(bq, settings)

        sqls = _sql_calls(bq)
        schema_sql = sqls[0]
        assert "schema_embeddings" in schema_sql
        assert "source_type STRING" in schema_sql
        assert "embedding ARRAY<FLOAT64>" in schema_sql

    def test_creates_column_embeddings_table(self):
        bq = _make_bq()
        create_embedding_tables(bq, settings)

        sqls = _sql_calls(bq)
        col_sql = sqls[1]
        assert "column_embeddings" in col_sql
        assert "column_name STRING" in col_sql

    def test_creates_query_memory_table(self):
        bq = _make_bq()
        create_embedding_tables(bq, settings)

        sqls = _sql_calls(bq)
        qm_sql = sqls[2]
        assert "query_memory" in qm_sql
        assert "question STRING" in qm_sql
        assert "sql_query STRING" in qm_sql

    def test_creates_symbol_exchange_map_table(self):
        bq = _make_bq()
        create_embedding_tables(bq, settings)

        sqls = _sql_calls(bq)
        sym_sql = sqls[3]
        assert "symbol_exchange_map" in sym_sql
        assert "symbol STRING" in sym_sql
        assert "exchange STRING" in sym_sql
        assert "portfolio STRING" in sym_sql

    def test_uses_fully_qualified_names(self):
        bq = _make_bq()
        create_embedding_tables(bq, settings)

        fqn = f"{settings.gcp_project}.{settings.metadata_dataset}"
        sqls = _sql_calls(bq)
        for sql in sqls:
            assert fqn in sql

    def test_column_embeddings_has_payload_columns(self):
        bq = _make_bq()
        create_embedding_tables(bq, settings)

        sqls = _sql_calls(bq)
        col_sql = sqls[1]
        assert "category STRING" in col_sql
        assert "formula STRING" in col_sql
        assert "typical_aggregation STRING" in col_sql
        assert "filterable BOOL" in col_sql
        assert "example_values ARRAY<STRING>" in col_sql
        assert "related_columns ARRAY<STRING>" in col_sql


class TestMigratePayloadColumns:
    def test_migrate_adds_payload_columns(self):
        bq = _make_bq()
        migrate_payload_columns(bq, settings)

        sqls = _sql_calls(bq)
        # Should issue ALTER TABLE ADD COLUMN for each payload column
        assert any("ALTER TABLE" in sql and "category" in sql for sql in sqls)

    def test_migrate_is_idempotent(self):
        """IF NOT EXISTS prevents errors when columns already exist."""

        bq = _make_bq()
        migrate_payload_columns(bq, settings)

        sqls = _sql_calls(bq)
        for sql in sqls:
            assert "IF NOT EXISTS" in sql


# ---------------------------------------------------------------------------
# TestPopulateSymbols
# ---------------------------------------------------------------------------


class TestPopulateSymbols:
    def test_reads_csv_and_merges(self):
        bq = _make_bq()
        csv_content = "symbol,exchange,portfolio\nABC,eurex,MON\nDEF,eurex,WHITE\n"

        with patch("scripts.run_embeddings.Path") as mock_path_cls:
            # Set up the path chain: Path(__file__).parent.parent / "data" / ...
            csv_path_mock = MagicMock()
            csv_path_mock.exists.return_value = True
            mock_path_cls.return_value.parent.parent.__truediv__.return_value.__truediv__.return_value = csv_path_mock

            with patch("builtins.open", mock_open(read_data=csv_content)):
                populate_symbols(bq, settings)

        bq.execute_query.assert_called_once()
        sql = bq.execute_query.call_args[0][0]
        assert "MERGE" in sql
        assert "symbol_exchange_map" in sql
        assert "ABC" in sql
        assert "DEF" in sql

    def test_batches_at_500_rows(self):
        bq = _make_bq()
        # Create 1001 rows -> should produce 3 batches (500 + 500 + 1)
        rows = [f"SYM{i},eurex,MON" for i in range(1001)]
        csv_content = "symbol,exchange,portfolio\n" + "\n".join(rows) + "\n"

        with patch("scripts.run_embeddings.Path") as mock_path_cls:
            csv_path_mock = MagicMock()
            csv_path_mock.exists.return_value = True
            mock_path_cls.return_value.parent.parent.__truediv__.return_value.__truediv__.return_value = csv_path_mock

            with patch("builtins.open", mock_open(read_data=csv_content)):
                populate_symbols(bq, settings)

        assert bq.execute_query.call_count == 3

    def test_skips_when_csv_missing(self):
        bq = _make_bq()

        with patch("scripts.run_embeddings.Path") as mock_path_cls:
            csv_path_mock = MagicMock()
            csv_path_mock.exists.return_value = False
            mock_path_cls.return_value.parent.parent.__truediv__.return_value.__truediv__.return_value = csv_path_mock

            populate_symbols(bq, settings)

        bq.execute_query.assert_not_called()

    def test_merge_sql_structure(self):
        bq = _make_bq()
        csv_content = "symbol,exchange,portfolio\nABC,eurex,MON\n"

        with patch("scripts.run_embeddings.Path") as mock_path_cls:
            csv_path_mock = MagicMock()
            csv_path_mock.exists.return_value = True
            mock_path_cls.return_value.parent.parent.__truediv__.return_value.__truediv__.return_value = csv_path_mock

            with patch("builtins.open", mock_open(read_data=csv_content)):
                populate_symbols(bq, settings)

        sql = bq.execute_query.call_args[0][0]
        assert "MERGE" in sql
        assert "USING" in sql
        assert "UNNEST" in sql
        assert "WHEN NOT MATCHED THEN" in sql
        assert "INSERT" in sql


# ---------------------------------------------------------------------------
# TestPopulateSchemaEmbeddings
# ---------------------------------------------------------------------------


class TestPopulateSchemaEmbeddings:
    def test_generates_merge_for_kpi_and_data(self):
        bq = _make_bq()
        populate_schema_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        # At minimum: kpi merge + data merge + routing merge + cleanup = 4
        assert len(sqls) >= 4

    def test_all_kpi_tables_in_sql(self):
        bq = _make_bq()
        populate_schema_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        all_sql = " ".join(sqls)
        kpi_tables = [
            "markettrade",
            "quotertrade",
            "brokertrade",
            "clicktrade",
            "otoswing",
        ]
        for table in kpi_tables:
            assert table in all_sql, f"KPI table '{table}' not found in SQL"

    def test_all_data_tables_in_sql(self):
        bq = _make_bq()
        populate_schema_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        all_sql = " ".join(sqls)
        data_tables = ["theodata", "marketdata", "marketdepth", "swingdata"]
        for table in data_tables:
            assert table in all_sql, f"Data table '{table}' not found in SQL"

    def test_routing_descriptions_included(self):
        bq = _make_bq()
        populate_schema_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        all_sql = " ".join(sqls)
        assert "'routing'" in all_sql

    def test_merge_uses_coalesce_for_match(self):
        bq = _make_bq()
        populate_schema_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        # The kpi and data MERGEs use COALESCE in the ON clause
        kpi_or_data_merges = [s for s in sqls if "COALESCE" in s]
        assert len(kpi_or_data_merges) >= 2

    def test_routing_merge_uses_on_false(self):
        bq = _make_bq()
        populate_schema_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        routing_sql = [s for s in sqls if "'routing'" in s and "MERGE" in s]
        assert len(routing_sql) >= 1
        assert "ON FALSE" in routing_sql[0]

    def test_routing_cleanup_deletes_duplicates(self):
        bq = _make_bq()
        populate_schema_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        cleanup_sqls = [s for s in sqls if "DELETE FROM" in s]
        assert len(cleanup_sqls) >= 1
        assert "source_type = 'routing'" in cleanup_sqls[0]

    def test_uses_schema_embeddings_table(self):
        bq = _make_bq()
        populate_schema_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        for sql in sqls:
            if "MERGE" in sql:
                assert "schema_embeddings" in sql


# ---------------------------------------------------------------------------
# TestGenerateEmbeddings
# ---------------------------------------------------------------------------


class TestGenerateEmbeddings:
    def test_generates_three_update_sqls(self):
        bq = _make_bq()
        generate_embeddings(bq, settings)

        assert bq.execute_query.call_count == 3

    def test_uses_array_length_check(self):
        bq = _make_bq()
        generate_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        for sql in sqls:
            assert "ARRAY_LENGTH" in sql
            assert "embedding IS NULL" in sql

    def test_uses_retrieval_document_task_type(self):
        bq = _make_bq()
        generate_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        # schema_embeddings and column_embeddings use RETRIEVAL_DOCUMENT
        assert "RETRIEVAL_DOCUMENT" in sqls[0]
        assert "RETRIEVAL_DOCUMENT" in sqls[1]
        # query_memory uses RETRIEVAL_QUERY
        assert "RETRIEVAL_QUERY" in sqls[2]

    def test_updates_correct_tables(self):
        bq = _make_bq()
        generate_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        assert "schema_embeddings" in sqls[0]
        assert "column_embeddings" in sqls[1]
        assert "query_memory" in sqls[2]

    def test_uses_ml_generate_embedding(self):
        bq = _make_bq()
        generate_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        for sql in sqls:
            assert "ML.GENERATE_EMBEDDING" in sql

    def test_references_embedding_model(self):
        bq = _make_bq()
        generate_embeddings(bq, settings)

        sqls = _sql_calls(bq)
        for sql in sqls:
            assert settings.embedding_model_ref in sql

    def test_skips_when_autonomous_embeddings_enabled(self):
        bq = _make_bq()
        # Create a settings-like object with use_autonomous_embeddings=True
        patched = MagicMock()
        patched.use_autonomous_embeddings = True

        generate_embeddings(bq, patched)

        bq.execute_query.assert_not_called()


# ---------------------------------------------------------------------------
# TestCreateVectorIndexes
# ---------------------------------------------------------------------------


class TestCreateVectorIndexes:
    def test_creates_three_indexes(self):
        bq = _make_bq()
        create_vector_indexes(bq, settings)

        assert bq.execute_query.call_count == 3

    def test_uses_if_not_exists(self):
        bq = _make_bq()
        create_vector_indexes(bq, settings)

        sqls = _sql_calls(bq)
        for sql in sqls:
            assert "CREATE VECTOR INDEX IF NOT EXISTS" in sql

    def test_uses_tree_ah_index_type(self):
        bq = _make_bq()
        create_vector_indexes(bq, settings)

        sqls = _sql_calls(bq)
        for sql in sqls:
            assert "TREE_AH" in sql

    def test_uses_cosine_distance(self):
        bq = _make_bq()
        create_vector_indexes(bq, settings)

        sqls = _sql_calls(bq)
        for sql in sqls:
            assert "COSINE" in sql

    def test_indexes_correct_tables(self):
        bq = _make_bq()
        create_vector_indexes(bq, settings)

        sqls = _sql_calls(bq)
        all_sql = " ".join(sqls)
        assert "schema_embeddings" in all_sql
        assert "column_embeddings" in all_sql
        assert "query_memory" in all_sql

    def test_indexes_embedding_column(self):
        bq = _make_bq()
        create_vector_indexes(bq, settings)

        sqls = _sql_calls(bq)
        for sql in sqls:
            assert "(embedding)" in sql


# ---------------------------------------------------------------------------
# TestBuildTableDescriptions
# ---------------------------------------------------------------------------


class TestBuildTableDescriptions:
    def test_returns_kpi_and_data_descriptions(self):
        descs = _build_table_descriptions(settings)

        layers = {d["layer"] for d in descs}
        assert "kpi" in layers
        assert "data" in layers

    def test_includes_dataset_level_rows(self):
        descs = _build_table_descriptions(settings)

        dataset_rows = [d for d in descs if d["source_type"] == "dataset"]
        assert len(dataset_rows) >= 2  # one for kpi, one for data

    def test_includes_table_level_rows(self):
        descs = _build_table_descriptions(settings)

        table_rows = [d for d in descs if d["source_type"] == "table"]
        assert len(table_rows) >= 10  # 5 KPI + at least 5 data tables

    def test_kpi_tables_present(self):
        descs = _build_table_descriptions(settings)

        kpi_tables = {
            d["table_name"]
            for d in descs
            if d["layer"] == "kpi" and d["source_type"] == "table"
        }
        expected = {
            "markettrade",
            "quotertrade",
            "brokertrade",
            "clicktrade",
            "otoswing",
        }
        assert expected.issubset(kpi_tables)

    def test_data_tables_present(self):
        descs = _build_table_descriptions(settings)

        data_tables = {
            d["table_name"]
            for d in descs
            if d["layer"] == "data" and d["source_type"] == "table"
        }
        expected = {"theodata", "marketdata", "marketdepth", "swingdata"}
        assert expected.issubset(data_tables)

    def test_row_structure(self):
        descs = _build_table_descriptions(settings)

        required_keys = {
            "source_type",
            "layer",
            "dataset_name",
            "table_name",
            "description",
        }
        for desc in descs:
            assert required_keys.issubset(desc.keys()), f"Missing keys in {desc}"

    def test_kpi_dataset_name_matches_settings(self):
        descs = _build_table_descriptions(settings)

        kpi_rows = [d for d in descs if d["layer"] == "kpi"]
        for row in kpi_rows:
            assert row["dataset_name"] == settings.kpi_dataset

    def test_data_dataset_name_matches_settings(self):
        descs = _build_table_descriptions(settings)

        data_rows = [d for d in descs if d["layer"] == "data"]
        for row in data_rows:
            assert row["dataset_name"] == settings.data_dataset

    def test_descriptions_are_nonempty(self):
        descs = _build_table_descriptions(settings)

        for desc in descs:
            assert desc["description"], f"Empty description for {desc}"


# ---------------------------------------------------------------------------
# TestBuildRoutingDescriptions
# ---------------------------------------------------------------------------


class TestBuildRoutingDescriptions:
    def test_returns_at_least_three_descriptions(self):
        descs = _build_routing_descriptions()

        # _routing.yaml has kpi_vs_data_general, theodata_routing, kpi_table_selection
        assert len(descs) >= 3

    def test_descriptions_are_strings(self):
        descs = _build_routing_descriptions()

        for desc in descs:
            assert isinstance(desc, str)

    def test_descriptions_are_nonempty(self):
        descs = _build_routing_descriptions()

        for desc in descs:
            assert len(desc.strip()) > 0


# ---------------------------------------------------------------------------
# TestEscapeBqString
# ---------------------------------------------------------------------------


class TestEscapeBqString:
    def test_escapes_single_quotes(self):
        result = _escape_bq_string("it's a test")
        assert "\\'" in result
        assert "'" not in result.replace("\\'", "")

    def test_collapses_whitespace(self):
        result = _escape_bq_string("hello   world\n\tfoo")
        assert result == "hello world foo"

    def test_handles_empty_string(self):
        result = _escape_bq_string("")
        assert result == ""

    def test_handles_no_special_characters(self):
        result = _escape_bq_string("hello world")
        assert result == "hello world"

    def test_combined_escaping_and_whitespace(self):
        result = _escape_bq_string("it's   a\n'test'")
        assert result == "it\\'s a \\'test\\'"
