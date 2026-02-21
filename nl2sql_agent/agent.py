"""ADK agent definitions: root agent and NL2SQL sub-agent.

The root_agent variable is REQUIRED by ADK convention.
ADK discovers it automatically when running `adk run nl2sql_agent`.
"""

from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from google.genai.types import GenerateContentConfig

from nl2sql_agent.callbacks import after_tool_log, before_tool_guard
from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger, setup_logging
from nl2sql_agent.prompts import build_nl2sql_instruction
from nl2sql_agent.tools import (
    check_semantic_cache,
    dry_run_sql,
    execute_sql,
    fetch_few_shot_examples,
    init_bq_service,
    load_yaml_metadata,
    resolve_exchange,
    save_validated_query,
    vector_search_columns,
)

# --- Initialise logging ---
setup_logging()
logger = get_logger(__name__)


# --- Lazy BQ client initialization ---
# Deferred so importing this module doesn't require GCP credentials.
# The client is created on first tool call via ADK's lifecycle.
_bq_initialized = False


def _ensure_bq_initialized():
    """Create and register the BQ client if not already done."""
    global _bq_initialized
    if _bq_initialized:
        return
    from nl2sql_agent.clients import LiveBigQueryClient

    bq_client = LiveBigQueryClient(
        project=settings.gcp_project, location=settings.bq_location
    )
    init_bq_service(bq_client)
    _bq_initialized = True


def _lazy_before_tool_guard(tool, args, tool_context):
    """Wrapper that ensures BQ is initialized before the first tool call."""
    _ensure_bq_initialized()
    return before_tool_guard(tool, args, tool_context)


# --- Model instances ---
default_model = LiteLlm(
    model=settings.litellm_model,
    api_key=settings.litellm_api_key,
    api_base=settings.litellm_api_base,
)

# --- NL2SQL Sub-Agent ---
nl2sql_agent = LlmAgent(
    name="nl2sql_agent",
    model=default_model,
    description=(
        "Answers questions about Mako trading data by querying BigQuery. "
        "Handles theo/vol/delta analysis, KPI/PnL queries, quoter activity, "
        "broker performance, edge/slippage analysis across all trading desks. "
        "Routes to the correct table based on question context."
    ),
    instruction=build_nl2sql_instruction,
    generate_content_config=GenerateContentConfig(temperature=0.1),
    tools=[
        check_semantic_cache,
        resolve_exchange,
        vector_search_columns,
        fetch_few_shot_examples,
        load_yaml_metadata,
        dry_run_sql,
        execute_sql,
        save_validated_query,
    ],
    before_tool_callback=_lazy_before_tool_guard,
    after_tool_callback=after_tool_log,
)

# --- Root Agent ---
# IMPORTANT: This variable MUST be named `root_agent`. ADK looks for this exact name.
root_agent = LlmAgent(
    name="mako_assistant",
    model=default_model,
    description="Mako Group trading assistant.",
    instruction=(
        "You are a helpful assistant for Mako Group traders. "
        "You coordinate between specialised sub-agents.\n\n"
        "## Delegation Rules\n"
        "- For ANY question about trading data, performance, KPIs, PnL, edge, "
        "slippage, theo/vol/delta, greeks, quoter activity, broker performance, "
        "market data, order book depth, or anything that requires querying "
        "a database → delegate to nl2sql_agent.\n"
        "- For general questions, greetings, or clarifications → answer directly.\n"
        "- If the trader's question is ambiguous about whether it needs data → "
        "ask a clarifying question before delegating.\n"
        "- Do NOT attempt to write SQL yourself. Always delegate to nl2sql_agent.\n"
    ),
    sub_agents=[nl2sql_agent],
)

logger.info(
    "agents_initialised",
    root_agent=root_agent.name,
    sub_agents=[a.name for a in root_agent.sub_agents],
    model=settings.litellm_model,
    tool_count=len(nl2sql_agent.tools),
)
