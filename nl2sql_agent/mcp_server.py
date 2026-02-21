"""MCP server exposing the NL2SQL agent as a single tool.

Gemini CLI / Claude Code can connect to this server via stdio transport
and use the `ask_trading_data` tool to query Mako trading data.

CRITICAL: MCP stdio transport uses stdout for JSON-RPC. ALL application
logging MUST go to stderr. This module redirects structlog output to
stderr before importing the agent module.
"""

import logging
import sys

# --- Redirect ALL logging to stderr BEFORE any agent imports ---
# structlog's PrintLoggerFactory prints to stdout by default.
# We must reconfigure it before agent.py's module-level setup_logging() runs.
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import structlog  # noqa: E402

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    cache_logger_on_first_use=False,  # Allow reconfiguration
)

from google.adk.runners import InMemoryRunner  # noqa: E402
from google.genai.types import Content, Part  # noqa: E402
from mcp.server import FastMCP  # noqa: E402
from mcp.server.fastmcp import Context  # noqa: E402

from nl2sql_agent.agent import root_agent  # noqa: E402

# --- Progress message mapping ---
TOOL_PROGRESS_MESSAGES = {
    "check_semantic_cache": "Checking query cache...",
    "resolve_exchange": "Resolving exchange...",
    "vector_search_columns": "Searching tables and columns...",
    "vector_search_tables": "Searching for relevant tables...",
    "load_yaml_metadata": "Loading column metadata...",
    "fetch_few_shot_examples": "Finding similar past queries...",
    "dry_run_sql": "Validating SQL syntax...",
    "execute_sql": "Executing query against BigQuery...",
    "save_validated_query": "Saving validated query...",
}

# --- MCP server ---
mcp = FastMCP(
    "mako-trading",
    instructions=(
        "NL2SQL agent for Mako Group trading data. "
        "Routes natural language questions to the correct BigQuery table."
    ),
)

# --- ADK runner (shared across requests) ---
_runner = InMemoryRunner(
    agent=root_agent,
    app_name="mcp_nl2sql",
)


@mcp.tool()
async def ask_trading_data(question: str, ctx: Context) -> str:
    """Ask a natural-language question about Mako Group's trading data.

    Routes to the correct BigQuery table and returns results.
    Use for: PnL, edge, slippage, KPI metrics, theo/vol/delta/greeks,
    quoter activity, broker performance (BGC/MGN), market data,
    order book depth, trade counts, symbol analysis, portfolio breakdowns.
    Covers both KPI (gold layer) and raw data (silver layer) datasets.
    """
    try:
        # Fresh session per request (no cross-request state leakage)
        session = await _runner.session_service.create_session(
            app_name="mcp_nl2sql", user_id="mcp_user"
        )

        user_msg = Content(
            role="user",
            parts=[Part(text=question)],
        )

        final_text = ""
        step_count = 0

        async for event in _runner.run_async(
            user_id="mcp_user",
            session_id=session.id,
            new_message=user_msg,
        ):
            # Emit progress for tool calls (function calls are in content.parts)
            for fc in event.get_function_calls():
                tool_name = fc.name or "unknown"
                step_desc = TOOL_PROGRESS_MESSAGES.get(
                    tool_name, f"Running {tool_name}..."
                )
                await ctx.report_progress(
                    progress=step_count, total=8, message=step_desc
                )
                step_count += 1

            # Collect final response text
            if event.is_final_response():
                if event.content and event.content.parts:
                    final_text = "\n".join(
                        p.text
                        for p in event.content.parts
                        if hasattr(p, "text") and p.text
                    )
                break

        return final_text or "No response generated."

    except Exception as e:
        return f"Error processing question: {e}"


if __name__ == "__main__":
    mcp.run(transport="stdio")
