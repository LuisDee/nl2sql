"""Tests for routing logic embedded in the system prompt.

These verify that the prompt text covers critical routing scenarios.
Actual LLM behavior testing is in Track 05 (Eval & Hardening).
"""

from unittest.mock import MagicMock

from nl2sql_agent.prompts import build_nl2sql_instruction


def _get_prompt():
    """Get the built instruction text."""
    return build_nl2sql_instruction(MagicMock())


class TestKpiRouting:
    def test_markettrade_is_default(self):
        prompt = _get_prompt()
        assert "DEFAULT" in prompt or "default" in prompt
        assert "markettrade" in prompt

    def test_brokertrade_mentions_account_field(self):
        prompt = _get_prompt()
        lower = prompt.lower()
        assert "account" in lower
        assert "bgc" in lower or "broker" in lower

    def test_quotertrade_kpi_vs_data_distinguished(self):
        """The prompt must distinguish KPI quotertrade from data quotertrade."""
        prompt = _get_prompt()
        assert "quotertrade" in prompt
        assert "quoter" in prompt.lower()

    def test_union_all_for_total_pnl(self):
        prompt = _get_prompt()
        assert "UNION ALL" in prompt
        assert "5" in prompt or "five" in prompt.lower()


class TestDataRouting:
    def test_theodata_explicit_routing(self):
        """theodata must have explicit routing rules."""
        prompt = _get_prompt()
        lower = prompt.lower()
        assert "theodata" in lower
        assert "vol" in lower
        assert "delta" in lower

    def test_marketdata_vs_marketdepth(self):
        prompt = _get_prompt()
        lower = prompt.lower()
        assert "marketdata" in lower
        assert "marketdepth" in lower
        assert "order book" in lower or "depth" in lower

    def test_kpi_vs_data_disambiguation(self):
        """Prompt must explain when to use KPI vs data for same-name tables."""
        prompt = _get_prompt()
        lower = prompt.lower()
        assert "edge" in lower and "kpi" in lower
        assert "raw" in lower and "data" in lower


class TestSqlConstraints:
    def test_trade_date_partition_required(self):
        prompt = _get_prompt()
        assert "trade_date" in prompt
        assert "partition" in prompt.lower() or "ALWAYS" in prompt

    def test_fully_qualified_table_names_required(self):
        prompt = _get_prompt()
        from nl2sql_agent.config import settings

        assert settings.gcp_project in prompt

    def test_read_only_enforcement(self):
        prompt = _get_prompt()
        upper = prompt.upper()
        assert "NEVER" in upper
        blocked_keywords = ["INSERT", "UPDATE", "DELETE", "DROP"]
        assert any(kw in upper for kw in blocked_keywords)

    def test_round_function_required(self):
        prompt = _get_prompt()
        assert "ROUND" in prompt

    def test_limit_required(self):
        prompt = _get_prompt()
        assert "LIMIT" in prompt
