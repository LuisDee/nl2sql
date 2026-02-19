"""Tests for vector search tools."""

from nl2sql_agent.tools.vector_search import vector_search_tables, fetch_few_shot_examples


class TestVectorSearchTables:
    def test_returns_results_on_success(self, mock_bq):
        mock_bq.set_query_response("schema_embeddings", [
            {
                "source_type": "table",
                "layer": "kpi",
                "dataset_name": "nl2sql_omx_kpi",
                "table_name": "markettrade",
                "description": "KPI metrics for market trades",
                "distance": 0.1234,
            }
        ])

        result = vector_search_tables("what was the edge on our trade?")

        assert result["status"] == "success"
        assert len(result["results"]) == 1
        assert result["results"][0]["table_name"] == "markettrade"

    def test_passes_question_as_parameter(self, mock_bq):
        vector_search_tables("test question")

        assert mock_bq.last_params is not None
        assert mock_bq.last_params[0]["name"] == "question"
        assert mock_bq.last_params[0]["value"] == "test question"

    def test_uses_retrieval_query_task_type(self, mock_bq):
        vector_search_tables("any question")

        assert "RETRIEVAL_QUERY" in mock_bq.last_query
        assert "RETRIEVAL_DOCUMENT" not in mock_bq.last_query

    def test_uses_cosine_distance(self, mock_bq):
        vector_search_tables("any question")

        assert "COSINE" in mock_bq.last_query

    def test_returns_error_dict_on_exception(self, mock_bq):
        original_method = mock_bq.query_with_params

        def exploding_query(*args, **kwargs):
            raise RuntimeError("BQ connection failed")

        mock_bq.query_with_params = exploding_query

        result = vector_search_tables("test")

        assert result["status"] == "error"
        assert "BQ connection failed" in result["error_message"]

        mock_bq.query_with_params = original_method

    def test_sql_contains_settings_references(self, mock_bq):
        vector_search_tables("any question")

        # Should reference the metadata dataset from settings
        assert "nl2sql_metadata" in mock_bq.last_query


class TestFetchFewShotExamples:
    def test_returns_examples_on_success(self, mock_bq):
        mock_bq.set_query_response("query_memory", [
            {
                "past_question": "what was edge yesterday?",
                "sql_query": "SELECT edge_bps FROM ...",
                "tables_used": "markettrade",
                "past_dataset": "nl2sql_omx_kpi",
                "complexity": "simple",
                "routing_signal": "mentions edge",
                "distance": 0.0567,
            }
        ])

        result = fetch_few_shot_examples("what was the edge?")

        assert result["status"] == "success"
        assert len(result["examples"]) == 1
        assert "edge" in result["examples"][0]["past_question"]

    def test_returns_error_dict_on_exception(self, mock_bq):
        original_method = mock_bq.query_with_params

        def exploding_query(*args, **kwargs):
            raise RuntimeError("timeout")

        mock_bq.query_with_params = exploding_query

        result = fetch_few_shot_examples("test")

        assert result["status"] == "error"

        mock_bq.query_with_params = original_method
