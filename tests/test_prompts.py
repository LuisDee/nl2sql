"""Tests for the NL2SQL system prompt."""

from datetime import date
from unittest.mock import MagicMock

from nl2sql_agent.prompts import build_nl2sql_instruction


class TestBuildInstruction:
    def _make_ctx(self, state=None):
        """Create a mock ReadonlyContext with optional state."""
        ctx = MagicMock()
        ctx.state = state or {}
        return ctx

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

    def test_contains_cache_step(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "check_semantic_cache" in result

    def test_cache_step_before_vector_search(self):
        result = build_nl2sql_instruction(self._make_ctx())
        cache_pos = result.find("check_semantic_cache")
        vector_pos = result.find("vector_search_tables")
        assert cache_pos < vector_pos


class TestFollowUpContext:
    def _make_ctx(self, state=None):
        ctx = MagicMock()
        ctx.state = state or {}
        return ctx

    def test_follow_up_context_when_state_present(self):
        state = {
            "last_query_sql": "SELECT symbol FROM `p.d.markettrade`",
            "last_results_summary": {"row_count": 42, "preview": []},
        }
        result = build_nl2sql_instruction(self._make_ctx(state))
        assert "FOLLOW-UP CONTEXT" in result
        assert "SELECT symbol FROM" in result
        assert "42 rows" in result

    def test_no_follow_up_when_state_empty(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "FOLLOW-UP CONTEXT" not in result

    def test_follow_up_sql_trimmed_to_500_chars(self):
        long_sql = "SELECT " + "x, " * 300 + "y FROM t"
        state = {
            "last_query_sql": long_sql,
            "last_results_summary": {"row_count": 1, "preview": []},
        }
        result = build_nl2sql_instruction(self._make_ctx(state))
        assert "FOLLOW-UP CONTEXT" in result
        # The full SQL should NOT appear (it's >500 chars)
        assert long_sql not in result
        # But a truncated version should
        assert "..." in result


class TestRetryGuidance:
    def _make_ctx(self, state=None):
        ctx = MagicMock()
        ctx.state = state or {}
        return ctx

    def test_retry_status_shown_when_attempts_exist(self):
        state = {"dry_run_attempts": 2}
        result = build_nl2sql_instruction(self._make_ctx(state))
        assert "RETRY STATUS" in result
        assert "failed 2 time(s)" in result

    def test_no_retry_status_when_zero_attempts(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "RETRY STATUS" not in result
