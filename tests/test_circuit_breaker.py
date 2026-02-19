"""Tests for hard circuit breaker + tool call limit."""

from unittest.mock import MagicMock

from nl2sql_agent.callbacks import before_tool_guard


class TestCircuitBreaker:
    """before_tool_guard blocks SQL tools when max_retries_reached is True."""

    def _make_tool(self, name="dry_run_sql"):
        tool = MagicMock()
        tool.name = name
        return tool

    def _make_context(self, state=None):
        ctx = MagicMock()
        ctx.state = state if state is not None else {}
        return ctx

    def test_blocks_dry_run_when_max_retries_reached(self):
        tool = self._make_tool("dry_run_sql")
        ctx = self._make_context({"max_retries_reached": True})

        result = before_tool_guard(tool, {"sql_query": "SELECT 1"}, ctx)

        assert result is not None
        assert result["status"] == "error"
        assert result["blocked_by"] == "circuit_breaker"

    def test_blocks_execute_sql_when_max_retries_reached(self):
        tool = self._make_tool("execute_sql")
        ctx = self._make_context({"max_retries_reached": True})

        result = before_tool_guard(tool, {"sql_query": "SELECT 1"}, ctx)

        assert result is not None
        assert result["blocked_by"] == "circuit_breaker"

    def test_allows_sql_when_max_retries_not_reached(self):
        tool = self._make_tool("dry_run_sql")
        ctx = self._make_context({"max_retries_reached": False})

        result = before_tool_guard(tool, {"sql_query": "SELECT 1"}, ctx)

        assert result is None

    def test_allows_sql_when_max_retries_absent(self):
        tool = self._make_tool("dry_run_sql")
        ctx = self._make_context({})

        result = before_tool_guard(tool, {"sql_query": "SELECT 1"}, ctx)

        assert result is None

    def test_non_sql_tools_not_blocked_by_retry_flag(self):
        tool = self._make_tool("vector_search_tables")
        ctx = self._make_context({"max_retries_reached": True})

        result = before_tool_guard(tool, {"question": "what?"}, ctx)

        assert result is None


class TestToolCallCounter:
    """Global tool call counter blocks after max_tool_calls_per_turn."""

    def _make_tool(self, name="vector_search_tables"):
        tool = MagicMock()
        tool.name = name
        return tool

    def _make_context(self, state=None):
        ctx = MagicMock()
        ctx.state = state if state is not None else {}
        return ctx

    def test_counter_increments(self):
        tool = self._make_tool()
        ctx = self._make_context()

        before_tool_guard(tool, {"question": "test"}, ctx)

        assert ctx.state["tool_call_count"] == 1

    def test_counter_increments_consecutively(self):
        tool = self._make_tool()
        ctx = self._make_context({"tool_call_count": 5})

        before_tool_guard(tool, {"question": "test"}, ctx)

        assert ctx.state["tool_call_count"] == 6

    def test_blocks_after_max_tool_calls(self):
        tool = self._make_tool()
        # Set count to 50 (default max) — next call will be 51 and exceed limit
        ctx = self._make_context({"tool_call_count": 50})

        result = before_tool_guard(tool, {"question": "test"}, ctx)

        assert result is not None
        assert result["status"] == "error"
        assert result["blocked_by"] == "max_tool_calls"

    def test_allows_at_max_tool_calls(self):
        tool = self._make_tool()
        # Count is 49 — next call will be 50, which equals the limit
        ctx = self._make_context({"tool_call_count": 49})

        result = before_tool_guard(tool, {"question": "test"}, ctx)

        assert result is None
        assert ctx.state["tool_call_count"] == 50

    def test_counter_resets_on_check_semantic_cache(self):
        """check_semantic_cache is the first tool per question — resets counter."""
        tool = self._make_tool("check_semantic_cache")
        ctx = self._make_context({"tool_call_count": 25})

        result = before_tool_guard(tool, {"question": "new question"}, ctx)

        # Counter should be reset to 0, then incremented to 1
        assert result is None
        assert ctx.state["tool_call_count"] == 1
