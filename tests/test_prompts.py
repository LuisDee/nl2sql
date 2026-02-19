"""Tests for the NL2SQL system prompt."""

from datetime import date
from unittest.mock import MagicMock

from nl2sql_agent.prompts import build_nl2sql_instruction


class TestBuildInstruction:
    def _make_ctx(self):
        """Create a mock ReadonlyContext."""
        return MagicMock()

    def test_returns_string(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert isinstance(result, str)
        assert len(result) > 500

    def test_contains_today_date(self):
        result = build_nl2sql_instruction(self._make_ctx())
        today = date.today().isoformat()
        assert today in result

    def test_contains_project_id_from_settings(self):
        result = build_nl2sql_instruction(self._make_ctx())
        from nl2sql_agent.config import settings

        assert settings.gcp_project in result

    def test_contains_both_dataset_names(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "nl2sql_omx_kpi" in result
        assert "nl2sql_omx_data" in result

    def test_contains_all_kpi_table_names(self):
        result = build_nl2sql_instruction(self._make_ctx())
        for table in [
            "markettrade",
            "quotertrade",
            "brokertrade",
            "clicktrade",
            "otoswing",
        ]:
            assert table in result

    def test_contains_theodata_routing_rule(self):
        """theodata must be explicitly called out as data-layer only."""
        result = build_nl2sql_instruction(self._make_ctx())
        assert "theodata" in result
        lower = result.lower()
        assert "unique" in lower or "only" in lower

    def test_contains_tool_usage_order(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "vector_search_tables" in result
        assert "load_yaml_metadata" in result
        assert "fetch_few_shot_examples" in result
        assert "dry_run_sql" in result
        assert "execute_sql" in result

    def test_contains_sql_rules(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "ROUND" in result
        assert "trade_date" in result
        assert "LIMIT" in result

    def test_contains_union_all_pattern(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "UNION ALL" in result

    def test_contains_security_constraints(self):
        result = build_nl2sql_instruction(self._make_ctx())
        upper = result.upper()
        assert "NEVER" in upper
        assert "INSERT" in upper or "DELETE" in upper or "DROP" in upper

    def test_no_hardcoded_project_ids(self):
        """The prompt must not contain hardcoded project IDs."""
        result = build_nl2sql_instruction(self._make_ctx())
        assert "melodic-stone-437916-t3" not in result

    def test_contains_clarification_rules(self):
        result = build_nl2sql_instruction(self._make_ctx())
        lower = result.lower()
        assert "clarif" in lower

    def test_contains_retry_instruction(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "retry" in result.lower() or "3 times" in result or "3 failures" in result
