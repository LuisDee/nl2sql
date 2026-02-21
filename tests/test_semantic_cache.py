"""Tests for the semantic cache tool."""

from nl2sql_agent.tools.semantic_cache import _CACHE_SEARCH_SQL, check_semantic_cache


class TestSemanticCacheHit:
    def test_cache_hit_returns_cached_sql(self, mock_bq):
        mock_bq.set_query_response(
            "query_memory",
            [
                {
                    "cached_question": "What was total PnL today?",
                    "cached_sql": "SELECT SUM(pnl) FROM t",
                    "tables_used": ["markettrade"],
                    "cached_dataset": "nl2sql_omx_kpi",
                    "distance": 0.02,
                }
            ],
        )

        result = check_semantic_cache("What was total PnL today?")

        assert result["cache_hit"] is True
        assert result["cached_sql"] == "SELECT SUM(pnl) FROM t"
        assert result["distance"] == 0.02

    def test_cache_miss_returns_false(self, mock_bq):
        mock_bq.set_query_response(
            "query_memory",
            [
                {
                    "cached_question": "Unrelated question",
                    "cached_sql": "SELECT 1",
                    "tables_used": [],
                    "cached_dataset": "",
                    "distance": 0.5,
                }
            ],
        )

        result = check_semantic_cache("What was total PnL today?")

        assert result["cache_hit"] is False

    def test_cache_miss_on_empty_results(self, mock_bq):
        # Default response is empty list
        result = check_semantic_cache("What was total PnL today?")

        assert result["cache_hit"] is False

    def test_cache_error_degrades_to_miss(self, mock_bq):
        def raise_error(*args, **kwargs):
            raise Exception("BQ connection failed")

        mock_bq.query_with_params = raise_error

        result = check_semantic_cache("What was total PnL today?")

        assert result["cache_hit"] is False
        assert "error" in result["reason"]


class TestSemanticCacheSQL:
    def test_uses_cosine_distance(self):
        assert "COSINE" in _CACHE_SEARCH_SQL

    def test_uses_retrieval_query_task_type(self):
        assert "RETRIEVAL_QUERY" in _CACHE_SEARCH_SQL

    def test_top_k_is_one(self):
        assert "top_k => 1" in _CACHE_SEARCH_SQL


class TestSemanticCacheThreshold:
    def test_threshold_from_settings(self, mock_bq):
        """Cache should use settings.semantic_cache_threshold (0.10)."""
        mock_bq.set_query_response(
            "query_memory",
            [
                {
                    "cached_question": "Test",
                    "cached_sql": "SELECT 1",
                    "tables_used": [],
                    "cached_dataset": "",
                    "distance": 0.08,  # Below default 0.10 threshold
                }
            ],
        )

        result = check_semantic_cache("Test question")
        assert result["cache_hit"] is True

    def test_above_threshold_is_miss(self, mock_bq):
        mock_bq.set_query_response(
            "query_memory",
            [
                {
                    "cached_question": "Test",
                    "cached_sql": "SELECT 1",
                    "tables_used": [],
                    "cached_dataset": "",
                    "distance": 0.12,  # Above default 0.10 threshold
                }
            ],
        )

        result = check_semantic_cache("Test question")
        assert result["cache_hit"] is False


class TestSemanticCacheNoBqService:
    def test_no_bq_service_returns_miss(self):
        """If BQ service not initialized, should degrade gracefully."""
        import nl2sql_agent.tools._deps as deps

        original = deps._bq_service
        deps._bq_service = None
        try:
            result = check_semantic_cache("test")
            assert result["cache_hit"] is False
        finally:
            deps._bq_service = original
