"""Tests for exchange-aware semantic cache validation.

After Track 14 (Multi-Exchange Support), the semantic cache can return
cached SQL from a different exchange. A cached OMX query should NOT be
returned for a Brazil question if the datasets don't match.
"""

from nl2sql_agent.tools.semantic_cache import check_semantic_cache


class TestCacheExchangeMatch:
    """Cache hit where cached_dataset matches resolved exchange datasets → still a hit."""

    def test_cache_hit_matching_exchange(self, mock_bq):
        mock_bq.set_query_response(
            "query_memory",
            [
                {
                    "cached_question": "What was edge today?",
                    "cached_sql": "SELECT edge FROM t",
                    "tables_used": ["markettrade"],
                    "cached_dataset": "nl2sql_omx_kpi",
                    "distance": 0.03,
                }
            ],
        )

        result = check_semantic_cache(
            "What was edge today?",
            exchange_datasets="nl2sql_omx_kpi,nl2sql_omx_data",
        )

        assert result["cache_hit"] is True
        assert result["cached_sql"] == "SELECT edge FROM t"


class TestCacheExchangeMismatch:
    """Cache hit where cached_dataset doesn't match resolved exchange → treated as miss."""

    def test_cache_miss_on_exchange_mismatch(self, mock_bq):
        mock_bq.set_query_response(
            "query_memory",
            [
                {
                    "cached_question": "What was edge today?",
                    "cached_sql": "SELECT edge FROM omx_table",
                    "tables_used": ["markettrade"],
                    "cached_dataset": "nl2sql_omx_kpi",
                    "distance": 0.03,
                }
            ],
        )

        result = check_semantic_cache(
            "What was edge today?",
            exchange_datasets="nl2sql_brazil_kpi,nl2sql_brazil_data",
        )

        assert result["cache_hit"] is False
        assert "exchange" in result.get("reason", "").lower()


class TestCacheNoExchangeContext:
    """Cache hit with no exchange context (backward compatible)."""

    def test_cache_hit_without_exchange_param(self, mock_bq):
        mock_bq.set_query_response(
            "query_memory",
            [
                {
                    "cached_question": "What was edge today?",
                    "cached_sql": "SELECT edge FROM t",
                    "tables_used": ["markettrade"],
                    "cached_dataset": "nl2sql_omx_kpi",
                    "distance": 0.03,
                }
            ],
        )

        # No exchange_datasets param → should still be a hit
        result = check_semantic_cache("What was edge today?")

        assert result["cache_hit"] is True

    def test_cache_hit_with_empty_exchange_param(self, mock_bq):
        mock_bq.set_query_response(
            "query_memory",
            [
                {
                    "cached_question": "What was edge today?",
                    "cached_sql": "SELECT edge FROM t",
                    "tables_used": ["markettrade"],
                    "cached_dataset": "nl2sql_omx_kpi",
                    "distance": 0.03,
                }
            ],
        )

        # Empty string → should still be a hit (backward compatible)
        result = check_semantic_cache("What was edge today?", exchange_datasets="")

        assert result["cache_hit"] is True


class TestCacheMissUnchanged:
    """Regular cache miss behavior unchanged."""

    def test_cache_miss_still_miss_with_exchange(self, mock_bq):
        mock_bq.set_query_response(
            "query_memory",
            [
                {
                    "cached_question": "Something unrelated",
                    "cached_sql": "SELECT 1",
                    "tables_used": [],
                    "cached_dataset": "nl2sql_omx_kpi",
                    "distance": 0.5,
                }
            ],
        )

        result = check_semantic_cache(
            "What was edge today?",
            exchange_datasets="nl2sql_omx_kpi,nl2sql_omx_data",
        )

        assert result["cache_hit"] is False
