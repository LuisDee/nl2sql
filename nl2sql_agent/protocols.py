"""Protocols (interfaces) for external dependencies.

All external services are accessed through these protocols. Business logic
and agent tools depend on these abstractions, NEVER on concrete clients.

Unit tests provide mock implementations.
Integration tests provide real implementations.

Usage:
    # In agent tools or business logic:
    def my_function(bq: BigQueryProtocol) -> str:
        results = bq.execute_query("SELECT 1")
        return results.to_markdown()

    # In production (agent.py):
    from nl2sql_agent.clients import LiveBigQueryClient
    client = LiveBigQueryClient(project="my-project")
    my_function(client)

    # In tests:
    from tests.fakes import FakeBigQueryClient
    client = FakeBigQueryClient(mock_results={"SELECT 1": ...})
    my_function(client)
"""

from typing import Any, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class BigQueryProtocol(Protocol):
    """Interface for BigQuery operations.

    Concrete implementations:
    - LiveBigQueryClient (nl2sql_agent/clients.py) — real BigQuery
    - FakeBigQueryClient (tests/fakes.py) — in-memory mock for unit tests
    """

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame.

        Args:
            sql: BigQuery SQL query string. Must be a SELECT statement.

        Returns:
            Query results as a pandas DataFrame.

        Raises:
            BigQueryError: If the query fails.
        """
        ...

    def dry_run_query(self, sql: str) -> dict:
        """Validate a SQL query without executing it.

        Args:
            sql: BigQuery SQL query string.

        Returns:
            Dict with keys:
                - "valid" (bool): Whether the query is syntactically valid.
                - "total_bytes_processed" (int): Estimated bytes if valid.
                - "error" (str | None): Error message if invalid.
        """
        ...

    def get_table_schema(self, dataset: str, table: str) -> list[dict]:
        """Get schema for a table.

        Args:
            dataset: BigQuery dataset name.
            table: Table name.

        Returns:
            List of dicts with keys: name, type, mode, description.
        """
        ...

    def query_with_params(
        self, sql: str, params: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a parameterised SQL query and return results as list of dicts.

        Args:
            sql: BigQuery SQL query with @param placeholders.
            params: List of query parameter dicts, each with keys:
                    name (str), type (str), value (Any).

        Returns:
            List of dicts, one per row. Column names are keys.
        """
        ...


@runtime_checkable
class EmbeddingProtocol(Protocol):
    """Interface for text embedding generation.

    Used in Track 02+ for vector search. Defined here so the interface
    is established from the start.

    Concrete implementations:
    - LiveEmbeddingClient (nl2sql_agent/clients.py) — Vertex AI via BigQuery
    - FakeEmbeddingClient (tests/fakes.py) — returns dummy vectors for testing
    """

    def embed_text(self, text: str) -> list[float]:
        """Generate an embedding vector for a text string.

        Args:
            text: Text to embed.

        Returns:
            Embedding vector as a list of floats.
        """
        ...

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for multiple texts.

        Args:
            texts: List of texts to embed.

        Returns:
            List of embedding vectors, one per input text.
        """
        ...