"""Tests for ADK callbacks."""

from unittest.mock import MagicMock

from nl2sql_agent.callbacks import (
    MAX_DRY_RUN_RETRIES,
    after_tool_log,
    before_tool_guard,
)


class TestBeforeToolGuard:
    def _make_tool(self, name="execute_sql"):
        tool = MagicMock()
        tool.name = name
        return tool

    def _make_context(self, state=None):
        ctx = MagicMock()
        ctx.state = state if state is not None else {}
        return ctx

    def test_allows_select_query(self):
        tool = self._make_tool()
        args = {"sql_query": "SELECT * FROM table"}

        result = before_tool_guard(tool, args, self._make_context())

        assert result is None

    def test_allows_with_cte_query(self):
        tool = self._make_tool()
        args = {"sql_query": "WITH cte AS (SELECT 1) SELECT * FROM cte"}

        result = before_tool_guard(tool, args, self._make_context())

        assert result is None

    def test_blocks_insert_query(self):
        tool = self._make_tool()
        args = {"sql_query": "INSERT INTO table VALUES (1)"}

        result = before_tool_guard(tool, args, self._make_context())

        assert result is not None
        assert result["status"] == "error"

    def test_blocks_drop_query(self):
        tool = self._make_tool("dry_run_sql")
        args = {"sql_query": "DROP TABLE important_table"}

        result = before_tool_guard(tool, args, self._make_context())

        assert result is not None
        assert "Blocked" in result["error_message"]

    def test_allows_non_sql_tools(self):
        """Non-SQL tools should always be allowed."""
        tool = self._make_tool("vector_search_tables")
        args = {"question": "what was the edge?"}

        result = before_tool_guard(tool, args, self._make_context())

        assert result is None

    def test_blocks_delete_query(self):
        tool = self._make_tool()
        args = {"sql_query": "DELETE FROM table WHERE 1=1"}

        result = before_tool_guard(tool, args, self._make_context())

        assert result is not None

    def test_blocks_update_query(self):
        tool = self._make_tool()
        args = {"sql_query": "UPDATE table SET x = 1"}

        result = before_tool_guard(tool, args, self._make_context())

        assert result is not None

    def test_allows_empty_sql(self):
        tool = self._make_tool()
        args = {"sql_query": ""}

        result = before_tool_guard(tool, args, self._make_context())

        assert result is None

    def test_blocks_alter_query(self):
        tool = self._make_tool()
        args = {"sql_query": "ALTER TABLE foo ADD COLUMN bar INT"}

        result = before_tool_guard(tool, args, self._make_context())

        assert result is not None


class TestAfterToolLog:
    def _make_tool(self, name="execute_sql"):
        tool = MagicMock()
        tool.name = name
        return tool

    def _make_context(self, state=None):
        ctx = MagicMock()
        ctx.state = state if state is not None else {}
        return ctx

    def test_returns_none_for_passthrough(self):
        tool = self._make_tool()
        tool_response = {"status": "success", "row_count": 5}

        result = after_tool_log(tool, {}, self._make_context(), tool_response)

        assert result is None

    def test_handles_error_response(self):
        tool = self._make_tool()
        tool_response = {"status": "error", "error_message": "timeout"}

        result = after_tool_log(tool, {}, self._make_context(), tool_response)

        assert result is None

    def test_handles_none_response(self):
        tool = self._make_tool()

        result = after_tool_log(tool, {}, self._make_context(), None)

        assert result is None


class TestRetryTracking:
    def _make_tool(self, name="dry_run_sql"):
        tool = MagicMock()
        tool.name = name
        return tool

    def _make_context(self, state=None):
        ctx = MagicMock()
        ctx.state = state if state is not None else {}
        return ctx

    def test_retry_counter_increments_on_dry_run_failure(self):
        tool = self._make_tool()
        ctx = self._make_context()
        response = {"status": "invalid", "error_message": "syntax error"}

        after_tool_log(tool, {}, ctx, response)

        assert ctx.state["dry_run_attempts"] == 1

    def test_retry_counter_increments_consecutively(self):
        tool = self._make_tool()
        ctx = self._make_context({"dry_run_attempts": 1})
        response = {"status": "invalid", "error_message": "another error"}

        after_tool_log(tool, {}, ctx, response)

        assert ctx.state["dry_run_attempts"] == 2

    def test_retry_counter_resets_on_success(self):
        tool = self._make_tool()
        ctx = self._make_context({"dry_run_attempts": 2})
        response = {"status": "valid", "estimated_bytes": 1024}

        after_tool_log(tool, {}, ctx, response)

        assert ctx.state["dry_run_attempts"] == 0

    def test_max_retries_adds_escalation_hint(self):
        tool = self._make_tool()
        ctx = self._make_context({"dry_run_attempts": MAX_DRY_RUN_RETRIES - 1})
        response = {"status": "invalid", "error_message": "persistent error"}

        result = after_tool_log(tool, {}, ctx, response)

        assert result is not None
        assert result["max_retries_reached"] is True
        assert "escalation_hint" in result
        assert "Stop retrying" in result["escalation_hint"]

    def test_retry_counter_resets_after_execute(self):
        tool = MagicMock()
        tool.name = "execute_sql"
        ctx = self._make_context({"dry_run_attempts": 2})
        response = {"status": "success", "row_count": 10, "rows": []}

        after_tool_log(tool, {"sql_query": "SELECT 1"}, ctx, response)

        assert ctx.state["dry_run_attempts"] == 0
        assert ctx.state["max_retries_reached"] is False


class TestSessionState:
    def _make_context(self, state=None):
        ctx = MagicMock()
        ctx.state = state if state is not None else {}
        return ctx

    def test_execute_success_persists_last_query(self):
        tool = MagicMock()
        tool.name = "execute_sql"
        ctx = self._make_context()
        sql = "SELECT symbol, COUNT(*) FROM t GROUP BY symbol"
        rows = [{"symbol": "A", "count": 10}, {"symbol": "B", "count": 5}]
        response = {"status": "success", "row_count": 2, "rows": rows}

        after_tool_log(tool, {"sql_query": sql}, ctx, response)

        assert ctx.state["last_query_sql"] == sql

    def test_execute_failure_does_not_persist(self):
        tool = MagicMock()
        tool.name = "execute_sql"
        ctx = self._make_context()
        response = {"status": "error", "error_message": "timeout"}

        after_tool_log(tool, {"sql_query": "SELECT 1"}, ctx, response)

        assert "last_query_sql" not in ctx.state

    def test_results_summary_limited_to_3_rows(self):
        tool = MagicMock()
        tool.name = "execute_sql"
        ctx = self._make_context()
        rows = [{"val": i} for i in range(20)]
        response = {"status": "success", "row_count": 20, "rows": rows}

        after_tool_log(tool, {"sql_query": "SELECT 1"}, ctx, response)

        summary = ctx.state["last_results_summary"]
        assert summary["row_count"] == 20
        assert len(summary["preview"]) == 3


class TestCircuitBreakerReset:
    """Circuit breaker state must reset when a new question arrives."""

    def _make_tool(self, name="check_semantic_cache"):
        tool = MagicMock()
        tool.name = name
        return tool

    def _make_context(self, state=None):
        ctx = MagicMock()
        ctx.state = state if state is not None else {}
        return ctx

    def test_new_question_resets_dry_run_attempts(self):
        """check_semantic_cache must reset dry_run_attempts to 0."""
        ctx = self._make_context({
            "dry_run_attempts": 3,
            "max_retries_reached": True,
            "tool_call_count": 5,
            "tool_call_history": ["abc", "def"],
        })
        tool = self._make_tool("check_semantic_cache")

        before_tool_guard(tool, {"question": "new question"}, ctx)

        assert ctx.state["dry_run_attempts"] == 0

    def test_new_question_resets_max_retries_reached(self):
        """check_semantic_cache must reset max_retries_reached to False."""
        ctx = self._make_context({
            "dry_run_attempts": 3,
            "max_retries_reached": True,
            "tool_call_count": 5,
            "tool_call_history": ["abc", "def"],
        })
        tool = self._make_tool("check_semantic_cache")

        before_tool_guard(tool, {"question": "new question"}, ctx)

        assert ctx.state["max_retries_reached"] is False

    def test_circuit_breaker_allows_sql_after_reset(self):
        """After reset, dry_run_sql should NOT be blocked by circuit breaker."""
        ctx = self._make_context({
            "dry_run_attempts": 3,
            "max_retries_reached": True,
            "tool_call_count": 5,
            "tool_call_history": ["abc", "def"],
        })

        # Step 1: New question resets state
        cache_tool = self._make_tool("check_semantic_cache")
        before_tool_guard(cache_tool, {"question": "new question"}, ctx)

        # Step 2: dry_run_sql should now be allowed
        dry_run_tool = self._make_tool("dry_run_sql")
        result = before_tool_guard(dry_run_tool, {"sql_query": "SELECT 1"}, ctx)

        assert result is None  # None means allowed

    def test_non_cache_tool_does_not_reset_circuit_breaker(self):
        """Only check_semantic_cache resets — other tools must not."""
        ctx = self._make_context({
            "dry_run_attempts": 3,
            "max_retries_reached": True,
        })
        tool = self._make_tool("vector_search_tables")

        before_tool_guard(tool, {"question": "test"}, ctx)

        assert ctx.state["dry_run_attempts"] == 3
        assert ctx.state["max_retries_reached"] is True
