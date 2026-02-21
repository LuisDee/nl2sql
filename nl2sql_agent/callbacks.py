"""ADK callbacks for the NL2SQL agent.

Provides before_tool_callback and after_tool_callback for guardrails,
structured logging, retry tracking, and state management.

ADK v1.20.0 callback signatures:
  before_tool_callback(tool, args, tool_context) -> Optional[dict]
  after_tool_callback(tool, args, tool_context, tool_response) -> Optional[dict]
"""

import hashlib
import json
from typing import Any

from google.adk.tools.base_tool import BaseTool
from google.adk.tools.tool_context import ToolContext

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.sql_guard import contains_dml

logger = get_logger(__name__)

MAX_DRY_RUN_RETRIES = 3


def _tool_call_hash(tool_name: str, args: dict) -> str:
    """Create a stable hash of tool name + args for repetition detection."""
    try:
        key = json.dumps({"tool": tool_name, "args": args}, sort_keys=True, default=str)
    except (TypeError, ValueError):
        key = f"{tool_name}:{str(args)}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


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

    # Guard: reject SQL tool calls with DML/DDL anywhere in the body
    if tool_name in ("dry_run_sql", "execute_sql"):
        sql = args.get("sql_query", "")
        is_blocked, reason = contains_dml(sql)
        if is_blocked:
            logger.warning("callback_blocked_dml", tool=tool_name, reason=reason)
            return {"status": "error", "error_message": reason}

    # Hard circuit breaker: block SQL tools after max retries
    if tool_name in ("dry_run_sql", "execute_sql"):
        if tool_context.state.get("max_retries_reached"):
            logger.warning("circuit_breaker_blocked", tool=tool_name)
            return {
                "status": "error",
                "error_message": (
                    "Max SQL retry attempts reached. "
                    "Explain the error to the user."
                ),
                "blocked_by": "circuit_breaker",
            }

    # Reset state on new question
    # (check_semantic_cache is always the first tool called per question)
    if tool_name == "check_semantic_cache":
        tool_context.state["tool_call_count"] = 0
        tool_context.state["tool_call_history"] = []
        tool_context.state["dry_run_attempts"] = 0
        tool_context.state["max_retries_reached"] = False

    # --- Repetition detection ---
    call_hash = _tool_call_hash(tool_name, args)
    history = tool_context.state.get("tool_call_history", [])
    history.append(call_hash)
    tool_context.state["tool_call_history"] = history

    # Count consecutive identical hashes at tail
    consecutive = 0
    for h in reversed(history):
        if h == call_hash:
            consecutive += 1
        else:
            break

    if consecutive >= settings.max_consecutive_repeats:
        logger.warning(
            "repetition_detected",
            tool=tool_name,
            consecutive=consecutive,
        )
        return {
            "status": "error",
            "error_message": (
                f"Loop detected: {tool_name} called {consecutive} times "
                f"with identical arguments. Stop retrying and explain the error."
            ),
            "blocked_by": "repetition_detector",
        }

    # Safety net: absolute cap (very high, should never hit in practice)
    call_count = tool_context.state.get("tool_call_count", 0) + 1
    tool_context.state["tool_call_count"] = call_count
    if call_count > settings.max_tool_calls_per_turn:
        logger.warning("max_tool_calls_exceeded", count=call_count)
        return {
            "status": "error",
            "error_message": f"Safety limit: {call_count} tool calls exceeded.",
            "blocked_by": "max_tool_calls",
        }

    return None  # Allow tool to proceed


def after_tool_log(
    tool: BaseTool,
    args: dict[str, Any],
    tool_context: ToolContext,
    tool_response: dict,
) -> dict | None:
    """Log tool results and manage retry/session state."""
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

    # --- Retry tracking for dry_run_sql ---
    if tool_name == "dry_run_sql":
        attempts = tool_context.state.get("dry_run_attempts", 0)
        if status == "valid":
            tool_context.state["dry_run_attempts"] = 0
            logger.info("dry_run_retry_reset", reason="valid_result")
        else:
            attempts += 1
            tool_context.state["dry_run_attempts"] = attempts
            logger.warning(
                "dry_run_retry_increment",
                attempt=attempts,
                max_retries=MAX_DRY_RUN_RETRIES,
            )
            if attempts >= MAX_DRY_RUN_RETRIES:
                tool_context.state["max_retries_reached"] = True
                logger.error(
                    "dry_run_max_retries_reached", attempts=attempts
                )
                # Augment the response with escalation hint
                return {
                    **(tool_response or {}),
                    "max_retries_reached": True,
                    "escalation_hint": (
                        f"SQL validation has failed {attempts} times. "
                        "Stop retrying and explain the error to the user. "
                        "Ask if they can rephrase the question or provide more context."
                    ),
                }

    # --- Reset retry counter on successful execute ---
    if tool_name == "execute_sql" and status == "success":
        tool_context.state["dry_run_attempts"] = 0
        tool_context.state["max_retries_reached"] = False

        # --- Session state: persist last query for follow-ups ---
        sql = args.get("sql_query", "")
        rows = tool_response.get("rows", [])
        tool_context.state["last_query_sql"] = sql
        tool_context.state["last_results_summary"] = {
            "row_count": tool_response.get("row_count", 0),
            "preview": rows[:3],
        }
        logger.info("session_state_persisted", sql_len=len(sql), row_count=len(rows))

    return None  # Don't modify the response
