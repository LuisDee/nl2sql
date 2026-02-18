"""ADK agent definitions: root agent and NL2SQL sub-agent.

The root_agent variable is REQUIRED by ADK convention.
ADK discovers it automatically when running `adk run nl2sql_agent`.
"""

import os

from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import setup_logging, get_logger

# --- Initialise logging ---
setup_logging()
logger = get_logger(__name__)

# --- Configure LiteLLM environment ---
# LiteLLM reads these environment variables directly.
# We set them here from our pydantic settings to ensure they're available.
os.environ["LITELLM_API_KEY"] = settings.litellm_api_key
os.environ["LITELLM_API_BASE"] = settings.litellm_api_base

# --- Model instances ---
default_model = LiteLlm(model=settings.litellm_model)

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
    instruction=(
        "You are a SQL expert for Mako Group, an options market-making firm. "
        "Your job is to answer natural language questions about trading data. "
        "For now, you have no tools — just acknowledge the question and explain "
        "that you will be able to query BigQuery once tools are connected. "
        "Mention which tables might be relevant based on the question. "
        "KPI tables (gold layer) are in nl2sql_omx_kpi dataset. "
        "Raw data tables (silver layer) are in nl2sql_omx_data dataset."
    ),
    # tools=[] — no tools in Track 01. Added in Track 03.
)

# --- Root Agent ---
# IMPORTANT: This variable MUST be named `root_agent`. ADK looks for this exact name.
root_agent = LlmAgent(
    name="mako_assistant",
    model=default_model,
    description="Mako Group trading assistant.",
    instruction=(
        "You are a helpful assistant for Mako Group traders. "
        "For any questions about trading data, performance, KPIs, "
        "theo/vol analysis, quoter activity, edge, slippage, PnL, "
        "or anything that requires querying a database, delegate to nl2sql_agent. "
        "For general questions, greetings, or clarifications, answer directly. "
        "If the trader's question is ambiguous, ask a clarifying question."
    ),
    sub_agents=[nl2sql_agent],
)

logger.info(
    "agents_initialised",
    root_agent=root_agent.name,
    sub_agents=[a.name for a in root_agent.sub_agents],
    model=settings.litellm_model,
)