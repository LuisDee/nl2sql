"""Tests for vector_search_columns — column-level semantic search."""

from nl2sql_agent.tools._deps import clear_vector_cache, get_cached_vector_result
from nl2sql_agent.tools.vector_search import vector_search_columns


def _make_column_row(
    dataset="nl2sql_omx_kpi",
    table="markettrade",
    column="instant_edge",
    column_type="FLOAT64",
    description="Edge at trade",
    synonyms="edge, trading edge",
    distance=0.10,
):
    """Helper to build a mock column search result row."""
    return {
        "search_type": "column_search",
        "dataset_name": dataset,
        "table_name": table,
        "best_column_distance": distance,
        "matching_columns": 1,
        "top_columns": [
            {
                "column_name": column,
                "column_type": column_type,
                "description": description,
                "synonyms": synonyms,
                "distance": distance,
            }
        ],
    }


def _make_example_row(
    question="what was edge yesterday?",
    sql_query="SELECT instant_edge FROM ...",
    tables_used=["markettrade"],
    dataset="nl2sql_omx_kpi",
    complexity="simple",
    routing_signal="kpi",
    distance=0.05,
):
    """Helper to build a mock example result row."""
    return {
        "search_type": "example",
        "past_question": question,
        "sql_query": sql_query,
        "tables_used": tables_used,
        "past_dataset": dataset,
        "complexity": complexity,
        "routing_signal": routing_signal,
        "distance": distance,
    }


class TestColumnSearch:
    """Tests for vector_search_columns tool."""

    def test_returns_tables_with_columns(self, mock_bq):
        """Column search returns table-level results with nested columns."""
        mock_bq.set_query_response("column_search", [
            _make_column_row(table="markettrade", column="instant_edge", distance=0.08),
        ])

        result = vector_search_columns("what was the edge?")

        assert result["status"] == "success"
        assert len(result["tables"]) == 1
        assert result["tables"][0]["table_name"] == "markettrade"
        assert result["tables"][0]["top_columns"][0]["column_name"] == "instant_edge"

    def test_tables_ranked_by_best_distance(self, mock_bq):
        """Tables should be ordered by their best column match distance."""
        mock_bq.set_query_response("column_search", [
            _make_column_row(table="theodata", distance=0.15),
            _make_column_row(table="markettrade", distance=0.05),
        ])

        result = vector_search_columns("what was the edge?")

        assert result["status"] == "success"
        assert len(result["tables"]) == 2
        # markettrade should come first (lower distance)
        assert result["tables"][0]["table_name"] == "markettrade"
        assert result["tables"][1]["table_name"] == "theodata"

    def test_caches_examples_for_fetch_few_shot(self, mock_bq):
        """Column search should cache examples for later fetch_few_shot_examples."""
        mock_bq.set_query_response("column_search", [
            _make_column_row(table="markettrade", distance=0.10),
        ])
        mock_bq.set_query_response("example", [
            _make_example_row(question="what was edge?", sql_query="SELECT instant_edge FROM ..."),
        ])

        vector_search_columns("what was the edge?")

        cached = get_cached_vector_result("what was the edge?")
        assert cached is not None
        assert "examples" in cached

    def test_falls_back_to_schema_search(self, mock_bq):
        """If column search fails, fall back to table-level schema search."""
        call_count = 0
        original_method = mock_bq.query_with_params

        def failing_then_succeeding(sql, params=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("column_embeddings table not found")
            return [
                {
                    "search_type": "schema",
                    "source_type": "table",
                    "layer": "kpi",
                    "dataset_name": "nl2sql_omx_kpi",
                    "table_name": "markettrade",
                    "description": "KPI metrics",
                    "distance": 0.12,
                },
            ]

        mock_bq.query_with_params = failing_then_succeeding

        result = vector_search_columns("what was the edge?")

        assert result["status"] == "success"
        assert result.get("fallback") is True

        mock_bq.query_with_params = original_method

    def test_top_columns_limited_per_table(self, mock_bq):
        """Verify max_per_table limit is used in SQL template."""
        mock_bq.set_query_response("column_search", [
            _make_column_row(table="markettrade", distance=0.10),
        ])

        vector_search_columns("what was the edge?")

        sql = mock_bq.last_query
        assert "LIMIT 15" in sql or "max_per_table" in sql.lower() or "15" in sql

    def test_uses_question_as_parameter(self, mock_bq):
        """The question should be passed as a @question parameter, not interpolated."""
        mock_bq.set_query_response("column_search", [
            _make_column_row(table="markettrade", distance=0.10),
        ])

        vector_search_columns("what was the edge today?")

        # Check params were used (SQL injection safe)
        assert mock_bq.last_params is not None
        param_names = [p["name"] for p in mock_bq.last_params]
        assert "question" in param_names

    def test_single_embedding_call(self, mock_bq):
        """The combined SQL should only generate one embedding."""
        mock_bq.set_query_response("column_search", [
            _make_column_row(table="markettrade", distance=0.10),
        ])

        vector_search_columns("any question")

        sql = mock_bq.last_query
        embedding_count = sql.count("ML.GENERATE_EMBEDDING")
        assert embedding_count == 1, f"Expected 1 embedding call, got {embedding_count}"

    def test_error_returns_status_error(self, mock_bq):
        """If both column and fallback fail, return error status."""
        def always_fail(sql, params=None):
            raise RuntimeError("everything broken")

        mock_bq.query_with_params = always_fail

        result = vector_search_columns("anything")

        assert result["status"] == "error"
        assert "error_message" in result

        # Restore
        mock_bq.query_with_params = MockBigQueryService.query_with_params.__get__(mock_bq)


# Need to import for the restore in the last test
from tests.conftest import MockBigQueryService
