"""Shared test fixtures for the nl2sql-agent test suite."""

import os

import pytest


@pytest.fixture(autouse=True)
def set_test_env(monkeypatch):
    """Set required environment variables for all tests.

    This ensures tests don't depend on the real .env file.
    Every env var that pydantic Settings requires MUST be set here.
    """
    monkeypatch.setenv("LITELLM_API_KEY", "test-key-not-real")
    monkeypatch.setenv("LITELLM_API_BASE", "http://localhost:4000")
    monkeypatch.setenv("LITELLM_MODEL", "gemini-3-flash-preview")
    monkeypatch.setenv("LITELLM_MODEL_COMPLEX", "gemini-3-pro-preview")
    monkeypatch.setenv("GCP_PROJECT", "cloud-data-n-base-d4b3")
    monkeypatch.setenv("BQ_LOCATION", "europe-west2")
    monkeypatch.setenv("KPI_DATASET", "nl2sql_omx_kpi")
    monkeypatch.setenv("DATA_DATASET", "nl2sql_omx_data")
    monkeypatch.setenv("METADATA_DATASET", "nl2sql_metadata")
    monkeypatch.setenv("ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS", "true")