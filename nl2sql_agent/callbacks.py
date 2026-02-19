"""ADK callbacks for the NL2SQL agent.

Provides before_tool_callback and after_tool_callback for guardrails,
structured logging, and state management.

ADK v1.20.0 callback signatures:
  before_tool_callback(tool, args, tool_context) -> Optional[dict]
  after_tool_callback(tool, args, tool_context, tool_response) -> Optional[dict]
"""

from typing import Any

from google.adk.tools.base_tool import BaseTool
from google.adk.tools.tool_context import ToolContext

from nl2sql_agent.logging_config import get_logger

logger = get_logger(__name__)


def before_tool_guard(
    tool: BaseTool, args: dict[str, Any], tool_context: ToolContext
) -> dict | None:
    """Validate tool inputs before execution.

    Returns None to allow the tool to proceed normally.
    Returns a dict to short-circuit with a custom response.
    """
    tool_name = tool.name

    logger.info(
        "tool_call_start",
        tool=tool_name,
        args_preview={k: str(v)[:100] for k, v in args.items()},
    )

    # Guard: reject SQL tool calls with obvious DML/DDL
    if tool_name in ("dry_run_sql", "execute_sql"):
        sql = args.get("sql_query", "")
        first_word = sql.strip().split()[0].upper() if sql.strip() else ""
        if first_word not in ("SELECT", "WITH", ""):
            logger.warning(
                "callback_blocked_dml", tool=tool_name, first_word=first_word
            )
            return {
                "status": "error",
                "error_message": (
                    f"Blocked: {first_word} queries are not allowed. "
                    "Only SELECT/WITH queries are permitted."
                ),
            }

    return None  # Allow tool to proceed


def after_tool_log(
    tool: BaseTool,
    args: dict[str, Any],
    tool_context: ToolContext,
    tool_response: dict,
) -> dict | None:
    """Log tool results after execution."""
    tool_name = tool.name
    status = tool_response.get("status", "unknown") if tool_response else "unknown"

    logger.info(
        "tool_call_complete",
        tool=tool_name,
        status=status,
        row_count=tool_response.get("row_count") if tool_response else None,
        result_count=(
            tool_response.get("result_count", len(tool_response.get("results", [])))
            if tool_response
            else 0
        ),
    )

    return None  # Don't modify the response
