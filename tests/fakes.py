"""Fake implementations of protocols for unit testing.

These replace real external services with in-memory implementations
that return predictable, controlled data.

Usage in tests:
    from tests.fakes import FakeBigQueryClient
    client = FakeBigQueryClient()
    client.add_result("SELECT 1", pd.DataFrame({"col": [1]}))
    result = client.execute_query("SELECT 1")
"""

from typing import Any

import pandas as pd


class FakeBigQueryClient:
    """In-memory fake BigQuery client. Implements BigQueryProtocol.

    Register expected queries and their results before use.
    Raises KeyError if an unregistered query is executed.
    """

    def __init__(self):
        self._results: dict[str, pd.DataFrame] = {}
        self._schemas: dict[str, list[dict]] = {}
        self._dry_run_responses: dict[str, dict] = {}
        self.executed_queries: list[str] = []  # Tracks what was called (for assertions)

    def add_result(self, sql: str, result: pd.DataFrame) -> None:
        """Register an expected query and its result.

        Args:
            sql: The exact SQL string that will be passed to execute_query.
            result: The DataFrame to return when this SQL is executed.
        """
        self._results[sql] = result

    def add_schema(self, dataset: str, table: str, schema: list[dict]) -> None:
        """Register a table schema.

        Args:
            dataset: Dataset name.
            table: Table name.
            schema: List of column dicts with keys: name, type, mode, description.
        """
        self._schemas[f"{dataset}.{table}"] = schema

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Return pre-registered result for the query."""
        self.executed_queries.append(sql)
        if sql not in self._results:
            raise KeyError(
                f"FakeBigQueryClient: No result registered for query: {sql[:100]}..."
            )
        return self._results[sql]

    def dry_run_query(self, sql: str) -> dict:
        """Return pre-registered dry run response, or default valid."""
        if sql in self._dry_run_responses:
            return self._dry_run_responses[sql]
        return {"valid": True, "total_bytes_processed": 1024, "error": None}

    def get_table_schema(self, dataset: str, table: str) -> list[dict]:
        """Return pre-registered schema."""
        key = f"{dataset}.{table}"
        if key not in self._schemas:
            raise KeyError(f"FakeBigQueryClient: No schema registered for {key}")
        return self._schemas[key]

    def query_with_params(
        self, sql: str, params: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        """Return pre-registered result for the parameterised query as list of dicts."""
        self.executed_queries.append(sql)
        if sql in self._results:
            return self._results[sql].to_dict(orient="records")
        return []


class FakeEmbeddingClient:
    """Fake embedding client that returns deterministic dummy vectors.

    Returns a fixed-length vector of 0.1 for every input.
    Useful for testing vector search pipeline without Vertex AI calls.
    """

    def __init__(self, dimension: int = 768):
        self._dimension = dimension

    def embed_text(self, text: str) -> list[float]:
        """Return a dummy embedding vector."""
        return [0.1] * self._dimension

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Return dummy embeddings for a batch."""
        return [[0.1] * self._dimension for _ in texts]