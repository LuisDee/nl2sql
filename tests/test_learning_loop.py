"""Tests for the learning loop (save validated query) tool."""

from nl2sql_agent.tools.learning_loop import save_validated_query


class TestSaveValidatedQuery:
    def test_successful_save(self, mock_bq):
        result = save_validated_query(
            question="what was edge yesterday?",
            sql_query="SELECT edge_bps FROM ...",
            tables_used="markettrade",
            dataset="nl2sql_omx_kpi",
            complexity="simple",
            routing_signal="mentions edge",
        )

        assert result["status"] == "success"
        assert "markettrade" in result["message"]

    def test_passes_question_as_parameter(self, mock_bq):
        save_validated_query(
            question="test question",
            sql_query="SELECT 1",
            tables_used="markettrade",
            dataset="nl2sql_omx_kpi",
            complexity="simple",
            routing_signal="test",
        )

        # Should have been called at least twice (insert + embed)
        assert mock_bq.query_call_count >= 2

    def test_insert_failure_returns_error(self, mock_bq):
        original_method = mock_bq.query_with_params

        def failing_insert(*args, **kwargs):
            raise RuntimeError("insert failed")

        mock_bq.query_with_params = failing_insert

        result = save_validated_query(
            question="test",
            sql_query="SELECT 1",
            tables_used="markettrade",
            dataset="nl2sql_omx_kpi",
            complexity="simple",
            routing_signal="test",
        )

        assert result["status"] == "error"
        assert "insert" in result["error_message"].lower()

        mock_bq.query_with_params = original_method

    def test_embed_failure_returns_partial_success(self, mock_bq):
        original_execute = mock_bq.execute_query

        def failing_embed(*args, **kwargs):
            raise RuntimeError("embed failed")

        mock_bq.execute_query = failing_embed

        result = save_validated_query(
            question="test",
            sql_query="SELECT 1",
            tables_used="markettrade",
            dataset="nl2sql_omx_kpi",
            complexity="simple",
            routing_signal="test",
        )

        assert result["status"] == "partial_success"

        mock_bq.execute_query = original_execute

    def test_uses_retrieval_document_for_embedding(self, mock_bq):
        save_validated_query(
            question="test",
            sql_query="SELECT 1",
            tables_used="markettrade",
            dataset="nl2sql_omx_kpi",
            complexity="simple",
            routing_signal="test",
        )

        # The embed query (second call via execute_query) should use RETRIEVAL_DOCUMENT
        assert mock_bq.query_call_count == 2

    def test_insert_sql_uses_split_for_tables(self, mock_bq):
        save_validated_query(
            question="test",
            sql_query="SELECT 1",
            tables_used="markettrade,brokertrade",
            dataset="nl2sql_omx_kpi",
            complexity="medium",
            routing_signal="test",
        )

        # The insert SQL should contain SPLIT for converting comma-separated to ARRAY
        # last_query will be the embed SQL (second call), so check params from insert
        assert mock_bq.last_params is not None
        tables_param = next(
            p for p in mock_bq.last_params if p["name"] == "tables_used"
        )
        assert tables_param["value"] == "markettrade,brokertrade"
