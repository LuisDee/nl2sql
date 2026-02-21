"""Integration tests for the tool chain with real BigQuery.

Tests each tool function directly with a real BigQuery service,
verifying the complete pipeline from question to result.
"""

from contextlib import contextmanager

import pytest

from nl2sql_agent.config import settings
from nl2sql_agent.tools._deps import init_bq_service
from nl2sql_agent.tools.metadata_loader import load_yaml_metadata
from nl2sql_agent.tools.sql_executor import execute_sql
from nl2sql_agent.tools.sql_validator import dry_run_sql
from nl2sql_agent.tools.vector_search import (
    fetch_few_shot_examples,
    vector_search_tables,
)


@contextmanager
def _patch_settings(real_settings):
    """Temporarily patch the settings singleton with real values."""
    attrs = [
        "gcp_project",
        "kpi_dataset",
        "data_dataset",
        "metadata_dataset",
        "embedding_model_ref",
        "vertex_ai_connection",
        "vector_search_top_k",
    ]
    originals = {a: getattr(settings, a) for a in attrs}
    for a in attrs:
        object.__setattr__(settings, a, getattr(real_settings, a))
    try:
        yield
    finally:
        for a, v in originals.items():
            object.__setattr__(settings, a, v)


@pytest.fixture
def wire_bq(bq_client, real_settings):
    """Inject the real BigQuery client and patch settings singleton."""
    init_bq_service(bq_client)
    with _patch_settings(real_settings):
        yield
    import nl2sql_agent.tools._deps as deps

    deps._bq_service = None


class TestVectorSearch:
    def test_vector_search_tables_returns_results(self, wire_bq):
        """Semantic search must find relevant tables for a trading question."""
        result = vector_search_tables("what was the average edge today?")
        assert result["status"] == "success"
        assert len(result["results"]) > 0
        table_names = [r["table_name"] for r in result["results"]]
        assert any("trade" in t for t in table_names), (
            f"Expected a trade table, got: {table_names}"
        )

    def test_fetch_few_shot_examples_returns_results(self, wire_bq):
        """Few-shot retrieval must find at least one similar past query."""
        result = fetch_few_shot_examples("what was the total PnL yesterday?")
        assert result["status"] == "success"
        assert len(result["examples"]) > 0
        assert "sql_query" in result["examples"][0]


class TestMetadataLoader:
    """YAML metadata loading — no BigQuery required."""

    def test_load_yaml_metadata_markettrade(self):
        """Must load YAML metadata for kpi.markettrade."""
        result = load_yaml_metadata("markettrade", "nl2sql_omx_kpi")
        assert result["status"] == "success"
        assert "metadata" in result
        assert "trade_date" in result["metadata"]

    def test_load_yaml_metadata_theodata(self):
        """Must load YAML metadata for data.theodata."""
        result = load_yaml_metadata("theodata", "nl2sql_omx_data")
        assert result["status"] == "success"
        assert "metadata" in result
        assert (
            "vol" in result["metadata"].lower() or "delta" in result["metadata"].lower()
        )

    def test_load_yaml_metadata_unknown_table(self):
        """Unknown table must return error with known table list."""
        result = load_yaml_metadata("nonexistent_table", "")
        assert result["status"] == "error"
        assert "Known tables" in result["error_message"]


class TestSQLTools:
    def test_dry_run_sql_tool_valid(self, wire_bq, real_settings):
        """dry_run_sql tool must validate correct SQL."""
        sql = f"""
        SELECT trade_date, symbol
        FROM `{real_settings.gcp_project}.{real_settings.kpi_dataset}.markettrade`
        WHERE trade_date = CURRENT_DATE()
        LIMIT 10
        """
        result = dry_run_sql(sql)
        assert result["status"] == "valid"
        assert "estimated_mb" in result

    def test_dry_run_sql_tool_invalid(self, wire_bq):
        """dry_run_sql tool must reject invalid SQL."""
        result = dry_run_sql("SELECT bad_column FROM nonexistent_table")
        assert result["status"] == "invalid"
        assert "error_message" in result

    def test_execute_sql_tool_select_one(self, wire_bq):
        """execute_sql tool must execute a trivial query."""
        result = execute_sql("SELECT 1 AS n")
        assert result["status"] == "success"
        assert result["row_count"] == 1
        assert result["rows"][0]["n"] == 1
