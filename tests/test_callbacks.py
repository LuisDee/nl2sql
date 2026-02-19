"""Tests for ADK callbacks."""

from unittest.mock import MagicMock

from nl2sql_agent.callbacks import before_tool_guard, after_tool_log


class TestBeforeToolGuard:
    def _make_tool(self, name="execute_sql"):
        tool = MagicMock()
        tool.name = name
        return tool

    def test_allows_select_query(self):
        tool = self._make_tool()
        args = {"sql_query": "SELECT * FROM table"}

        result = before_tool_guard(tool, args, MagicMock())

        assert result is None

    def test_allows_with_cte_query(self):
        tool = self._make_tool()
        args = {"sql_query": "WITH cte AS (SELECT 1) SELECT * FROM cte"}

        result = before_tool_guard(tool, args, MagicMock())

        assert result is None

    def test_blocks_insert_query(self):
        tool = self._make_tool()
        args = {"sql_query": "INSERT INTO table VALUES (1)"}

        result = before_tool_guard(tool, args, MagicMock())

        assert result is not None
        assert result["status"] == "error"

    def test_blocks_drop_query(self):
        tool = self._make_tool("dry_run_sql")
        args = {"sql_query": "DROP TABLE important_table"}

        result = before_tool_guard(tool, args, MagicMock())

        assert result is not None
        assert "Blocked" in result["error_message"]

    def test_allows_non_sql_tools(self):
        """Non-SQL tools should always be allowed."""
        tool = self._make_tool("vector_search_tables")
        args = {"question": "what was the edge?"}

        result = before_tool_guard(tool, args, MagicMock())

        assert result is None

    def test_blocks_delete_query(self):
        tool = self._make_tool()
        args = {"sql_query": "DELETE FROM table WHERE 1=1"}

        result = before_tool_guard(tool, args, MagicMock())

        assert result is not None

    def test_blocks_update_query(self):
        tool = self._make_tool()
        args = {"sql_query": "UPDATE table SET x = 1"}

        result = before_tool_guard(tool, args, MagicMock())

        assert result is not None

    def test_allows_empty_sql(self):
        tool = self._make_tool()
        args = {"sql_query": ""}

        result = before_tool_guard(tool, args, MagicMock())

        assert result is None

    def test_blocks_alter_query(self):
        tool = self._make_tool()
        args = {"sql_query": "ALTER TABLE foo ADD COLUMN bar INT"}

        result = before_tool_guard(tool, args, MagicMock())

        assert result is not None


class TestAfterToolLog:
    def _make_tool(self, name="execute_sql"):
        tool = MagicMock()
        tool.name = name
        return tool

    def test_returns_none_for_passthrough(self):
        tool = self._make_tool()
        tool_response = {"status": "success", "row_count": 5}

        result = after_tool_log(tool, {}, MagicMock(), tool_response)

        assert result is None

    def test_handles_error_response(self):
        tool = self._make_tool()
        tool_response = {"status": "error", "error_message": "timeout"}

        result = after_tool_log(tool, {}, MagicMock(), tool_response)

        assert result is None

    def test_handles_none_response(self):
        tool = self._make_tool()

        result = after_tool_log(tool, {}, MagicMock(), None)

        assert result is None
