"""Shared test fixtures for the nl2sql-agent test suite."""

import os
from typing import Any

import pandas as pd
import pytest

# Set env vars at module level so they're available during test collection.
# The nl2sql_agent package creates a Settings() singleton at import time,
# which happens before any fixtures run.
_TEST_ENV = {
    "LITELLM_API_KEY": "test-key-not-real",
    "LITELLM_API_BASE": "http://localhost:4000",
    "LITELLM_MODEL": "openai/gemini-3-flash-preview",
    "LITELLM_MODEL_COMPLEX": "openai/gemini-3-pro-preview",
    "GCP_PROJECT": "cloud-data-n-base-d4b3",
    "BQ_LOCATION": "europe-west2",
    "KPI_DATASET": "nl2sql_omx_kpi",
    "DATA_DATASET": "nl2sql_omx_data",
    "METADATA_DATASET": "nl2sql_metadata",
    "ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS": "true",
}

for _key, _val in _TEST_ENV.items():
    os.environ.setdefault(_key, _val)


@pytest.fixture(autouse=True)
def set_test_env(monkeypatch):
    """Set required environment variables for all tests.

    This ensures tests don't depend on the real .env file.
    Every env var that pydantic Settings requires MUST be set here.
    """
    for key, val in _TEST_ENV.items():
        monkeypatch.setenv(key, val)


class MockBigQueryService:
    """Mock BigQuery service implementing BigQueryProtocol for tests.

    Implements all protocol methods: execute_query, dry_run_query,
    query_with_params.
    """

    def __init__(self):
        self.query_responses: dict[str, list[dict[str, Any]]] = {}
        self.dry_run_responses: dict[str, dict[str, Any]] = {}
        self.last_query: str | None = None
        self.last_params: list | None = None
        self.query_call_count: int = 0
        self._default_query_response: list[dict[str, Any]] = []
        self._default_dry_run_response: dict[str, Any] = {
            "valid": True,
            "total_bytes_processed": 1024 * 1024,
            "error": None,
        }

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Mock execute_query — returns DataFrame (matching existing protocol)."""
        self.last_query = sql
        self.query_call_count += 1

        for keyword, response in self.query_responses.items():
            if keyword.lower() in sql.lower():
                return pd.DataFrame(response)

        return pd.DataFrame(self._default_query_response)

    def query_with_params(
        self, sql: str, params: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        """Mock query_with_params — returns list[dict] (new method for Track 03)."""
        self.last_query = sql
        self.last_params = params
        self.query_call_count += 1

        for keyword, response in self.query_responses.items():
            if keyword.lower() in sql.lower():
                return response

        return self._default_query_response

    def dry_run_query(self, sql: str) -> dict[str, Any]:
        """Mock dry_run_query — returns validation dict (matching existing protocol)."""
        self.last_query = sql

        for keyword, response in self.dry_run_responses.items():
            if keyword.lower() in sql.lower():
                return response

        return self._default_dry_run_response

    def set_query_response(self, keyword: str, rows: list[dict[str, Any]]) -> None:
        """Set a response for queries containing the given keyword."""
        self.query_responses[keyword] = rows

    def set_dry_run_response(self, keyword: str, response: dict[str, Any]) -> None:
        """Set a dry-run response for queries containing the given keyword."""
        self.dry_run_responses[keyword] = response


@pytest.fixture
def mock_bq():
    """Provide a MockBigQueryService and inject it into tools._deps."""
    mock = MockBigQueryService()

    from nl2sql_agent.tools._deps import clear_vector_cache, init_bq_service
    init_bq_service(mock)

    yield mock

    # Reset to None after test and clear vector cache
    import nl2sql_agent.tools._deps as deps
    deps._bq_service = None
    clear_vector_cache()