"""Tests for combined vector search + internal caching."""

from nl2sql_agent.tools._deps import clear_vector_cache, get_cached_vector_result
from nl2sql_agent.tools.vector_search import (
    fetch_few_shot_examples,
    vector_search_tables,
)


class TestCombinedVectorSearch:
    """vector_search_tables() runs a combined CTE and caches examples."""

    def test_combined_query_returns_schema_results(self, mock_bq):
        mock_bq.set_query_response(
            "schema_results",
            [
                {
                    "search_type": "schema",
                    "source_type": "table",
                    "layer": "kpi",
                    "dataset_name": "nl2sql_omx_kpi",
                    "table_name": "markettrade",
                    "description": "KPI metrics for market trades",
                    "distance": 0.1234,
                },
            ],
        )

        result = vector_search_tables("what was the edge?")

        assert result["status"] == "success"
        assert len(result["results"]) == 1
        assert result["results"][0]["table_name"] == "markettrade"

    def test_combined_query_caches_examples(self, mock_bq):
        mock_bq.set_query_response(
            "question_embedding",
            [
                {
                    "search_type": "schema",
                    "source_type": "table",
                    "layer": "kpi",
                    "dataset_name": "nl2sql_omx_kpi",
                    "table_name": "markettrade",
                    "description": "KPI metrics",
                    "distance": 0.12,
                },
                {
                    "search_type": "example",
                    "source_type": "",
                    "layer": "",
                    "dataset_name": "nl2sql_omx_kpi",
                    "table_name": "what was edge yesterday?",
                    "description": "SELECT edge_bps FROM ...",
                    "distance": 0.05,
                },
            ],
        )

        vector_search_tables("what was the edge?")

        cached = get_cached_vector_result("what was the edge?")
        assert cached is not None
        assert len(cached["examples"]) == 1
        assert cached["examples"][0]["past_question"] == "what was edge yesterday?"

    def test_fetch_few_shot_uses_cache_no_extra_bq_call(self, mock_bq):
        mock_bq.set_query_response(
            "question_embedding",
            [
                {
                    "search_type": "schema",
                    "source_type": "table",
                    "layer": "kpi",
                    "dataset_name": "nl2sql_omx_kpi",
                    "table_name": "markettrade",
                    "description": "KPI metrics",
                    "distance": 0.12,
                },
                {
                    "search_type": "example",
                    "source_type": "",
                    "layer": "",
                    "dataset_name": "nl2sql_omx_kpi",
                    "table_name": "what was edge yesterday?",
                    "description": "SELECT edge_bps FROM ...",
                    "distance": 0.05,
                },
            ],
        )

        vector_search_tables("what was the edge?")
        calls_after_tables = mock_bq.query_call_count

        result = fetch_few_shot_examples("what was the edge?")

        assert result["status"] == "success"
        assert len(result["examples"]) == 1
        # No additional BQ call — served from cache
        assert mock_bq.query_call_count == calls_after_tables

    def test_different_question_triggers_fresh_bq_call(self, mock_bq):
        mock_bq.set_query_response(
            "question_embedding",
            [
                {
                    "search_type": "schema",
                    "source_type": "table",
                    "layer": "kpi",
                    "dataset_name": "nl2sql_omx_kpi",
                    "table_name": "markettrade",
                    "description": "KPI metrics",
                    "distance": 0.12,
                },
            ],
        )

        vector_search_tables("what was the edge?")
        calls_after_tables = mock_bq.query_call_count

        # Different question — cache miss, must call BQ
        fetch_few_shot_examples("how did vol change?")

        assert mock_bq.query_call_count == calls_after_tables + 1

    def test_cache_cleared_between_sessions(self, mock_bq):
        mock_bq.set_query_response(
            "question_embedding",
            [
                {
                    "search_type": "example",
                    "source_type": "",
                    "layer": "",
                    "dataset_name": "nl2sql_omx_kpi",
                    "table_name": "cached question",
                    "description": "SELECT 1",
                    "distance": 0.01,
                },
            ],
        )

        vector_search_tables("test")
        assert get_cached_vector_result("test") is not None

        clear_vector_cache()
        assert get_cached_vector_result("test") is None

    def test_fallback_on_combined_query_failure(self, mock_bq):
        """If the combined query fails, fall back to schema-only search."""
        call_count = 0
        original_method = mock_bq.query_with_params

        def failing_then_succeeding(sql, params=None):
            nonlocal call_count
            call_count += 1
            # First call (combined) fails, second call (fallback) succeeds
            if call_count == 1:
                raise RuntimeError("combined query failed")
            return [
                {
                    "source_type": "table",
                    "layer": "kpi",
                    "dataset_name": "nl2sql_omx_kpi",
                    "table_name": "markettrade",
                    "description": "KPI metrics",
                    "distance": 0.12,
                }
            ]

        mock_bq.query_with_params = failing_then_succeeding

        result = vector_search_tables("test question")

        assert result["status"] == "success"
        assert len(result["results"]) == 1

        mock_bq.query_with_params = original_method

    def test_combined_query_uses_single_embedding(self, mock_bq):
        """The combined SQL should only contain one ML.GENERATE_EMBEDDING call."""
        vector_search_tables("any question")

        sql = mock_bq.last_query
        embedding_count = sql.count("ML.GENERATE_EMBEDDING")
        assert embedding_count == 1, f"Expected 1 embedding call, got {embedding_count}"


class TestCombinedSearchSqlTemplate:
    """The combined SQL template must include example metadata columns."""

    def test_combined_sql_includes_tables_used(self):
        """example_results CTE must SELECT tables_used for routing."""
        from nl2sql_agent.tools.vector_search import _COMBINED_SEARCH_SQL

        assert "tables_used" in _COMBINED_SEARCH_SQL, (
            "_COMBINED_SEARCH_SQL drops tables_used — examples lose routing info"
        )

    def test_combined_sql_includes_complexity(self):
        """example_results CTE must SELECT complexity."""
        from nl2sql_agent.tools.vector_search import _COMBINED_SEARCH_SQL

        assert "complexity" in _COMBINED_SEARCH_SQL, (
            "_COMBINED_SEARCH_SQL drops complexity — examples lose metadata"
        )

    def test_combined_sql_includes_routing_signal(self):
        """example_results CTE must SELECT routing_signal."""
        from nl2sql_agent.tools.vector_search import _COMBINED_SEARCH_SQL

        assert "routing_signal" in _COMBINED_SEARCH_SQL, (
            "_COMBINED_SEARCH_SQL drops routing_signal — examples lose routing hints"
        )


class TestVectorCacheIsolation:
    """Cache is properly isolated per test via conftest mock_bq fixture."""

    def test_cache_starts_empty(self):
        """Each test starts with no cached data."""
        clear_vector_cache()
        assert get_cached_vector_result("anything") is None
