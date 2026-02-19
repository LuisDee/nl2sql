"""Shared test fixtures for the nl2sql-agent test suite."""

import os

import pytest

# Set env vars at module level so they're available during test collection.
# The nl2sql_agent package creates a Settings() singleton at import time,
# which happens before any fixtures run.
_TEST_ENV = {
    "LITELLM_API_KEY": "test-key-not-real",
    "LITELLM_API_BASE": "http://localhost:4000",
    "LITELLM_MODEL": "gemini-3-flash-preview",
    "LITELLM_MODEL_COMPLEX": "gemini-3-pro-preview",
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