import pytest
from unittest.mock import MagicMock, patch
from nl2sql_agent.tools.semantic_cache import check_semantic_cache
from nl2sql_agent.tools.learning_loop import save_validated_query
from nl2sql_agent.config import settings

class TestSemanticCache:
    @patch("nl2sql_agent.tools.semantic_cache.get_bq_service")
    def test_cache_hit(self, mock_get_bq):
        mock_bq = MagicMock()
        mock_get_bq.return_value = mock_bq
        
        # Setup cached result with low distance
        mock_bq.query_with_params.return_value = [{
            "cached_question": "cached question",
            "cached_sql": "SELECT 1",
            "distance": 0.05
        }]
        # Set threshold to 0.1
        with patch.object(settings, 'semantic_cache_threshold', 0.1):
            result = check_semantic_cache("test question")
            
        assert result["cache_hit"] is True
        assert result["cached_sql"] == "SELECT 1"

    @patch("nl2sql_agent.tools.semantic_cache.get_bq_service")
    def test_cache_miss_distance(self, mock_get_bq):
        mock_bq = MagicMock()
        mock_get_bq.return_value = mock_bq
        
        # Setup cached result with high distance
        mock_bq.query_with_params.return_value = [{
            "cached_question": "irrelevant",
            "cached_sql": "SELECT 1",
            "distance": 0.9
        }]
        
        with patch.object(settings, 'semantic_cache_threshold', 0.1):
            result = check_semantic_cache("test question")
            
        assert result["cache_hit"] is False
        assert "exceeds threshold" in result["reason"]

    @patch("nl2sql_agent.tools.semantic_cache.get_bq_service")
    def test_cache_miss_empty(self, mock_get_bq):
        mock_bq = MagicMock()
        mock_get_bq.return_value = mock_bq
        
        # Setup empty result
        mock_bq.query_with_params.return_value = []
        
        result = check_semantic_cache("test question")
        
        assert result["cache_hit"] is False
        assert "no matching" in result["reason"]


class TestLearningLoop:
    @patch("nl2sql_agent.tools.learning_loop.get_bq_service")
    def test_save_validated_query_success(self, mock_get_bq):
        mock_bq = MagicMock()
        mock_get_bq.return_value = mock_bq
        
        # Act
        result = save_validated_query(
            question="test q",
            sql_query="SELECT 1",
            tables_used="table1",
            dataset="ds",
            complexity="simple",
            routing_signal="signal"
        )
        
        # Assert
        assert result["status"] == "success"
        # Verify insert called
        mock_bq.query_with_params.assert_called_once()
        # Verify embedding update called
        mock_bq.execute_query.assert_called_once()

    @patch("nl2sql_agent.tools.learning_loop.get_bq_service")
    def test_save_validated_query_partial_failure(self, mock_get_bq):
        mock_bq = MagicMock()
        mock_get_bq.return_value = mock_bq
        
        # Mock embedding failure
        mock_bq.execute_query.side_effect = Exception("Embedding failed")
        
        # Act
        result = save_validated_query(
            question="test q",
            sql_query="SELECT 1",
            tables_used="table1",
            dataset="ds",
            complexity="simple",
            routing_signal="signal"
        )
        
        # Assert
        assert result["status"] == "partial_success"
        assert "Embedding failed" in result["message"]
