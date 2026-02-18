"""Tests that verify fake implementations satisfy their protocols,
and that the protocol pattern works correctly."""

import pandas as pd

from nl2sql_agent.protocols import BigQueryProtocol, EmbeddingProtocol
from tests.fakes import FakeBigQueryClient, FakeEmbeddingClient


class TestFakeBigQueryClient:
    """Verify FakeBigQueryClient satisfies BigQueryProtocol."""

    def test_satisfies_protocol(self):
        """FakeBigQueryClient must be a valid BigQueryProtocol implementation."""
        client = FakeBigQueryClient()
        assert isinstance(client, BigQueryProtocol)

    def test_execute_query_returns_registered_result(self):
        """execute_query should return the DataFrame registered via add_result."""
        client = FakeBigQueryClient()
        expected = pd.DataFrame({"col": [1, 2, 3]})
        client.add_result("SELECT col FROM t", expected)

        result = client.execute_query("SELECT col FROM t")
        pd.testing.assert_frame_equal(result, expected)

    def test_execute_query_tracks_calls(self):
        """executed_queries list should track all queries passed to execute_query."""
        client = FakeBigQueryClient()
        client.add_result("Q1", pd.DataFrame())
        client.add_result("Q2", pd.DataFrame())

        client.execute_query("Q1")
        client.execute_query("Q2")

        assert client.executed_queries == ["Q1", "Q2"]

    def test_execute_query_raises_on_unregistered(self):
        """execute_query should raise KeyError for queries not registered."""
        import pytest

        client = FakeBigQueryClient()
        with pytest.raises(KeyError, match="No result registered"):
            client.execute_query("SELECT * FROM unknown")

    def test_dry_run_returns_valid_by_default(self):
        """dry_run_query should return valid=True by default."""
        client = FakeBigQueryClient()
        result = client.dry_run_query("SELECT 1")
        assert result["valid"] is True
        assert result["error"] is None

    def test_get_table_schema_returns_registered_schema(self):
        """get_table_schema should return the schema registered via add_schema."""
        client = FakeBigQueryClient()
        schema = [{"name": "id", "type": "INT64", "mode": "REQUIRED", "description": ""}]
        client.add_schema("nl2sql_omx_data", "theodata", schema)

        result = client.get_table_schema("nl2sql_omx_data", "theodata")
        assert result == schema


class TestFakeEmbeddingClient:
    """Verify FakeEmbeddingClient satisfies EmbeddingProtocol."""

    def test_satisfies_protocol(self):
        """FakeEmbeddingClient must be a valid EmbeddingProtocol implementation."""
        client = FakeEmbeddingClient()
        assert isinstance(client, EmbeddingProtocol)

    def test_embed_text_returns_correct_dimension(self):
        """embed_text should return a vector of the configured dimension."""
        client = FakeEmbeddingClient(dimension=768)
        result = client.embed_text("hello")
        assert len(result) == 768
        assert all(isinstance(x, float) for x in result)

    def test_embed_batch_returns_one_vector_per_input(self):
        """embed_batch should return one vector per input text."""
        client = FakeEmbeddingClient(dimension=256)
        result = client.embed_batch(["a", "b", "c"])
        assert len(result) == 3
        assert all(len(v) == 256 for v in result)