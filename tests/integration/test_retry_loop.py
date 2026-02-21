from unittest.mock import MagicMock

from google.adk.tools.base_tool import BaseTool
from google.adk.tools.tool_context import ToolContext

from nl2sql_agent.callbacks import MAX_DRY_RUN_RETRIES, after_tool_log


class TestRetryLoop:
    def test_dry_run_increment(self):
        """Verify retry counter increments on failure."""
        tool = MagicMock(spec=BaseTool)
        tool.name = "dry_run_sql"

        # Mock context.state as a real dict
        context = MagicMock(spec=ToolContext)
        context.state = {}

        # 1st Failure
        response = {"status": "invalid", "error": "Syntax error"}
        result = after_tool_log(tool, {}, context, response)

        assert context.state.get("dry_run_attempts") == 1
        assert result is None  # No modification yet

        # 2nd Failure
        result = after_tool_log(tool, {}, context, response)
        assert context.state["dry_run_attempts"] == 2

    def test_max_retries_reached(self):
        """Verify max retries triggers escalation hint."""
        tool = MagicMock(spec=BaseTool)
        tool.name = "dry_run_sql"

        context = MagicMock(spec=ToolContext)
        context.state = {"dry_run_attempts": MAX_DRY_RUN_RETRIES - 1}

        response = {"status": "invalid", "error": "Syntax error"}
        result = after_tool_log(tool, {}, context, response)

        assert context.state["dry_run_attempts"] == MAX_DRY_RUN_RETRIES
        assert context.state["max_retries_reached"] is True
        assert result is not None
        assert result["max_retries_reached"] is True
        assert "Stop retrying" in result["escalation_hint"]

    def test_reset_on_success(self):
        """Verify success resets the counter."""
        tool = MagicMock(spec=BaseTool)
        tool.name = "dry_run_sql"

        context = MagicMock(spec=ToolContext)
        context.state = {"dry_run_attempts": 2}

        response = {"status": "valid"}
        result = after_tool_log(tool, {}, context, response)

        assert context.state["dry_run_attempts"] == 0
        assert result is None
