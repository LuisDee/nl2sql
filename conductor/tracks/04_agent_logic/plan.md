# Track 04: Agent Logic (Phase D) — Implementation Plan

## Objective

Craft the comprehensive system prompts and agent logic that turn the NL2SQL agent from a set of disconnected tools into an intelligent, domain-expert SQL assistant. After this track, the agent can:

1. **Route** natural language questions to the correct table(s) across 2 datasets and 13 tables
2. **Generate** accurate BigQuery Standard SQL grounded in YAML metadata and few-shot examples
3. **Self-correct** via dry-run validation with up to 3 retry attempts
4. **Clarify** ambiguous questions before generating SQL
5. **Handle edge cases** like UNION ALL across KPI tables, KPI-vs-data disambiguation, and date partition enforcement

**Dependency**: Tracks 01-03 complete (agent skeleton, YAML catalog, embedding tables, 6 tools wired).

---

## Research-Informed Design Decisions

This plan incorporates findings from extensive research into current (2025-2026) best practices:

### 1. Single Comprehensive Prompt (BASELINE Method)

**Decision**: Use a single-stage comprehensive prompt, not multi-agent decomposition.

**Why**: Google's official ADK NL2SQL sample defines two methods — BASELINE (single-stage prompt with full schema context) and CHASE (multi-agent decomposition). BASELINE is recommended when "queries are straightforward SELECT statements with simpler schemas requiring faster response times." Our trading domain has a fixed, well-documented schema with ~13 tables. The routing complexity is moderate (2 datasets, 5 trade types). A single comprehensive prompt with tool-augmented context retrieval (vector search + YAML metadata + few-shot examples) is simpler and faster than multi-agent orchestration.

**Reference**: [ADK NL2SQL Pipeline Architecture](https://deepwiki.com/google/adk-samples/6.2-nl2sql-pipeline-and-database-integration)

### 2. ADK Callbacks for Guardrails

**Decision**: Use `before_tool_callback` and `after_tool_callback` for structured logging and guardrails, not embedded logic in tool functions.

**Why**: ADK provides four context types (InvocationContext, ReadonlyContext, CallbackContext, ToolContext) following the principle of least privilege. Callbacks provide aspect-oriented observability without polluting tool logic. The `after_tool_callback` is the official ADK pattern for storing query results in session state for downstream use.

**Reference**: [ADK Callbacks Documentation](https://google.github.io/adk-docs/callbacks/types-of-callbacks/)

### 3. Instruction as a Dynamic Function

**Decision**: Use a callable `instruction` function (not a static string) that injects the current date and routing context dynamically.

**Why**: ADK supports callable instructions that receive `ReadonlyContext`. This lets us inject `CURRENT_DATE()` equivalent, project ID, and dataset names at each invocation rather than baking them in at module load time. This is critical for a trading desk agent where "today" changes daily and the instruction must always reflect the current trading date.

**Reference**: [ADK Context Documentation](https://google.github.io/adk-docs/context/)

### 4. Prompt Injection Defense (Defense-in-Depth)

**Decision**: Layer three defenses: (1) read-only enforcement in `execute_sql`, (2) system prompt instructing the LLM to refuse non-data questions, (3) `before_tool_callback` that validates SQL tool inputs.

**Why**: NL2SQL systems are uniquely vulnerable to "database query-based prompt injection" where malicious user input causes the LLM to generate harmful SQL. Traditional WAFs don't catch this because the payload is generated after user input. Our defense layers: `execute_sql` already rejects non-SELECT (Track 03), the system prompt explicitly forbids DDL/DML generation, and a callback provides a final validation layer.

**Reference**: [Keysight: Database Query-Based Prompt Injection](https://www.keysight.com/blogs/en/tech/nwvs/2025/07/31/db-query-based-prompt-injection)

### 5. Asymmetric Embedding Strategy

**Decision**: Continue using `RETRIEVAL_QUERY` for search queries and `RETRIEVAL_DOCUMENT` for stored content. This is already correctly implemented in Track 03.

**Why**: Google's text-embedding-005 model supports asymmetric task types. Using the correct task type improves retrieval precision by 10-15% compared to using symmetric `SEMANTIC_SIMILARITY` for both sides. Our schema_embeddings and query_memory use `RETRIEVAL_DOCUMENT` for stored rows, and the search tools use `RETRIEVAL_QUERY` — this is correct and should not change.

### 6. Low Temperature for SQL Generation

**Decision**: Use `temperature=0.1` for SQL generation via `generate_content_config`.

**Why**: The official ADK NL2SQL BASELINE method uses temperature 0.1. SQL generation requires deterministic, precise output — creative variability introduces syntax errors and hallucinated column names. Low temperature with high top_k metadata context produces the most reliable SQL.

---

## CARRIED FORWARD: Tracks 01-03 Conventions

All conventions from previous tracks remain in force. See Track 03 plan.md for the full list. Key ones for this track:

1. **Protocol-based DI**: Tools access BQ through `get_bq_service()`, never import `bigquery.Client` directly.
2. **Configuration**: All config via `settings.*`. Never hardcode project/dataset/model.
3. **Tool returns**: All tools return `dict` with `status` key. Never raise exceptions to the LLM.
4. **ADK conventions**: Plain functions in `tools=[]`, no `@tool` decorator, no `FunctionTool()` wrapper.
5. **BigQuery ARRAY semantics**: `ARRAY_LENGTH(col) = 0` not `IS NULL`.
6. **Embedding task types**: `RETRIEVAL_QUERY` for search, `RETRIEVAL_DOCUMENT` for stored content.

---

## File-by-File Specification

### 1. NL2SQL Agent Instruction (UPDATE `agent.py`)

**Path**: `nl2sql_agent/agent.py`

The agent instruction is the most critical artifact in this track. It transforms the LLM from a generic assistant into a domain-expert SQL generator for Mako Group's trading desk.

**Key changes**:

1. Replace the static `instruction` string with a **callable function** that receives `ReadonlyContext` and returns the instruction dynamically.
2. Move the instruction text to a dedicated module (`nl2sql_agent/prompts.py`) for readability and testability.
3. Add `generate_content_config` with `temperature=0.1` for deterministic SQL generation.
4. Add `before_tool_callback` and `after_tool_callback` for guardrails and logging.

```python
# nl2sql_agent/agent.py (updated)

from nl2sql_agent.prompts import build_nl2sql_instruction
from nl2sql_agent.callbacks import before_tool_guard, after_tool_log

nl2sql_agent = LlmAgent(
    name="nl2sql_agent",
    model=default_model,
    description=(...),  # unchanged
    instruction=build_nl2sql_instruction,  # callable, receives ReadonlyContext
    generate_content_config=GenerateContentConfig(temperature=0.1),
    tools=[...],  # unchanged from Track 03
    before_tool_callback=before_tool_guard,
    after_tool_callback=after_tool_log,
)
```

**DO NOT** use a static string for the instruction. Use a callable that injects the current date.

**DO NOT** set temperature=0 (causes issues with some models). Use 0.1.

---

### 2. NL2SQL System Prompt (NEW `prompts.py`)

**Path**: `nl2sql_agent/prompts.py`

This module contains the system prompt for the NL2SQL sub-agent. It is a callable function that ADK invokes with `ReadonlyContext` at each turn.

The prompt is structured in sections, following the BASELINE method pattern from Google's ADK NL2SQL sample:

```python
"""NL2SQL agent system prompt.

This module builds the comprehensive instruction for the NL2SQL sub-agent.
The instruction is a callable that ADK invokes with ReadonlyContext,
allowing dynamic injection of the current date and project ID.
"""

from datetime import date

from google.adk.agents import ReadonlyContext

from nl2sql_agent.config import settings


def build_nl2sql_instruction(ctx: ReadonlyContext) -> str:
    """Build the NL2SQL agent instruction with dynamic context.

    ADK calls this at each turn. The ReadonlyContext provides access
    to session state but not modification — safe for instruction generation.
    """
    today = date.today().isoformat()
    project = settings.gcp_project
    kpi = settings.kpi_dataset
    data = settings.data_dataset

    return f"""You are a SQL expert for Mako Group, an options market-making firm.
Your job is to answer natural language questions about trading data by generating
and executing BigQuery Standard SQL queries.

Today's date is {today}. Use this as the default for trade_date filters when
the user says "today". For "yesterday", use DATE_SUB('{today}', INTERVAL 1 DAY).

## TOOL USAGE ORDER (follow this EVERY TIME)

1. **vector_search_tables** — Find which table(s) are relevant to the question
2. **load_yaml_metadata** — Load column descriptions, synonyms, and business rules for those tables
3. **fetch_few_shot_examples** — Find similar past validated queries for reference
4. **Write the SQL** using metadata + examples as context (see SQL Rules below)
5. **dry_run_sql** — Validate syntax and estimate cost
6. **execute_sql** — Run the validated query and return results

If dry_run_sql fails, read the error message carefully, fix the SQL, and retry.
You may retry up to 3 times. After 3 failures, explain the error to the user.

## DATASET AND TABLE REFERENCE

There are two datasets:

### `{project}.{kpi}` (Gold Layer — KPI Metrics)
Performance metrics with computed columns: edge (edge_bps), instant_pnl,
instant_pnl_w_fees, delta_slippage at multiple intervals, delta_bucket.
**5 tables, one per trade origin:**
- **markettrade** — Exchange trades. DEFAULT when trade type is unspecified.
- **quotertrade** — Auto-quoter fills. Use for "quoter edge", "quoter PnL".
- **brokertrade** — Broker trades. Has `account` field. Use for broker comparison (BGC, MGN).
- **clicktrade** — Manual click trades.
- **otoswing** — OTO swing trades.

### `{project}.{data}` (Silver Layer — Raw Data)
Raw execution details, timestamps, prices, sizes. No computed KPI metrics.
- **theodata** — UNIQUE to data layer. Theoretical pricing: tv (fair value), delta, vol (implied volatility), vega, gamma, theta. Use for ANY question about vol, IV, greeks, theo, fair value.
- **marketdata** — Market data feed snapshots, top-of-book prices.
- **marketdepth** — Order book depth, multiple price levels, bid/ask sizes.
- **swingdata** — Raw swing trade data.
- **markettrade** — Raw trade execution details (NOT KPI enriched).
- **quotertrade** — Raw quoter execution details.
- **clicktrade** — Raw click trade execution details.

## ROUTING RULES (Critical — follow these exactly)

1. **Default**: If trade type is unspecified, use `{kpi}.markettrade`.
2. **KPI vs Data**: If the question asks about edge, PnL, slippage, or performance → use `{kpi}`. If it asks about raw execution details, exact timestamps, prices, or market data → use `{data}`.
3. **Theo/Vol/Greeks**: ALWAYS route to `{data}.theodata`. This is the ONLY table with theoretical pricing data. It does NOT exist in the KPI dataset.
4. **Broker comparison**: When question mentions broker names (BGC, MGN) or "broker performance" → use `{kpi}.brokertrade`. NOTE: brokertrade may have no rows for some dates.
5. **All trades / Total PnL**: Use UNION ALL across all 5 KPI tables (markettrade, quotertrade, brokertrade, clicktrade, otoswing). Include a `trade_type` column to identify the source.
6. **Market data vs depth**: "market price" / "price feed" → `{data}.marketdata`. "order book" / "depth" / "bid-ask levels" → `{data}.marketdepth`.
7. **Ambiguous table names**: markettrade, quotertrade, and clicktrade exist in BOTH datasets. Always use the fully-qualified name with the correct dataset based on routing rules above.

## SQL GENERATION RULES

- **ALWAYS** use fully-qualified table names: `{project}.dataset.table`
- **ALWAYS** filter on `trade_date` partition column. If no date specified, use '{today}'.
- **ALWAYS** use `ROUND()` for decimal outputs (4 decimals for edge/vol/delta, 2 for PnL).
- **ALWAYS** add `LIMIT` unless the user explicitly asks for all rows.
- **ALWAYS** use BigQuery Standard SQL dialect.
- **NEVER** generate INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, or any DDL/DML.
- **NEVER** query tables not listed above. Only query `{kpi}.*` and `{data}.*` tables.
- **NEVER** use SELECT * in production queries — always specify columns explicitly.
- Use the EXACT column names from the YAML metadata. Do not guess column names.
- Use column synonyms from metadata to map user language to actual column names.
- For time-bucketed slippage columns, use the exact names: delta_slippage_1s, delta_slippage_1m, delta_slippage_5m, delta_slippage_30m, delta_slippage_1h, delta_slippage_eod.

## CLARIFICATION RULES

Ask a clarifying question if:
- The user's question could apply to multiple tables and routing rules don't resolve it
- The user asks about a specific symbol but doesn't provide one (e.g., "what's the vol?" → "For which symbol?")
- The user asks about a date range but doesn't specify one
- The question is genuinely ambiguous (don't over-clarify obvious questions)

Do NOT ask for clarification if:
- The routing rules clearly identify the table
- The user says "today" (use {today})
- The question is about aggregate data across all symbols

## RESPONSE FORMAT

After executing a query successfully:
1. Present the results in a clear, readable format
2. Briefly explain what the data shows
3. If the results seem unexpected (e.g., 0 rows), explain possible reasons
4. Ask: "Is this what you were looking for?" — if the user confirms, offer to save the validated query using save_validated_query

## SECURITY

- You are a READ-ONLY agent. Never generate or execute data modification queries.
- If a user asks you to modify, delete, or create data, politely refuse.
- Stick to the datasets and tables listed above. Do not query other datasets.
"""
```

**CRITICAL PROMPT ENGINEERING DECISIONS**:

1. **Dynamic date injection**: `{today}` is computed at each invocation via the callable. This ensures "today" always means the current trading date.
2. **Explicit table catalog in prompt**: Even though vector search exists, the prompt includes the full table listing. This gives the LLM a reliable fallback when vector search returns unexpected results. The LLM should VERIFY vector search results against this catalog.
3. **Routing rules as numbered list**: Explicit, ordered rules the LLM follows for disambiguation. This is more reliable than relying solely on vector search distances.
4. **SQL rules as strict constraints**: "ALWAYS" and "NEVER" rules are proven to be more effective than soft suggestions in system prompts.
5. **UNION ALL pattern**: Explicitly described so the LLM knows how to handle "all trades" questions.
6. **No `SELECT *`**: Enforced in the prompt to prevent excessive data transfer and to force the LLM to use metadata column names.

---

### 3. Root Agent Instruction Update (UPDATE `agent.py`)

The root agent (`mako_assistant`) needs a refined instruction to:
- Delegate data questions to `nl2sql_agent` reliably
- Handle greetings and general questions directly
- Know the boundaries of what the NL2SQL agent can do

```python
root_agent_instruction = (
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
)
```

---

### 4. Callbacks Module (NEW `callbacks.py`)

**Path**: `nl2sql_agent/callbacks.py`

ADK callbacks provide non-invasive guardrails and structured logging.

```python
"""ADK callbacks for the NL2SQL agent.

Provides before_tool_callback and after_tool_callback for guardrails,
structured logging, and state management.
"""

from google.adk.agents import CallbackContext
from google.genai.types import FunctionCall, FunctionResponse

from nl2sql_agent.logging_config import get_logger

logger = get_logger(__name__)


def before_tool_guard(
    callback_context: CallbackContext, tool_name: str, function_call: FunctionCall
) -> FunctionResponse | None:
    """Validate tool inputs before execution.

    Returns None to allow the tool to proceed normally.
    Returns a FunctionResponse to short-circuit with a custom response.
    """
    args = function_call.args or {}

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
            logger.warning("callback_blocked_dml", tool=tool_name, first_word=first_word)
            return FunctionResponse(
                name=tool_name,
                response={
                    "status": "error",
                    "error_message": (
                        f"Blocked: {first_word} queries are not allowed. "
                        "Only SELECT/WITH queries are permitted."
                    ),
                },
            )

    return None  # Allow tool to proceed


def after_tool_log(
    callback_context: CallbackContext, tool_name: str, function_response: FunctionResponse
) -> FunctionResponse | None:
    """Log tool results and optionally store in session state."""
    response = function_response.response or {}
    status = response.get("status", "unknown")

    logger.info(
        "tool_call_complete",
        tool=tool_name,
        status=status,
        row_count=response.get("row_count"),
        result_count=response.get("result_count", len(response.get("results", []))),
    )

    return None  # Don't modify the response
```

**ADK CALLBACK PATTERNS**:

1. `before_tool_callback` receives `(CallbackContext, tool_name: str, FunctionCall)`. Return `None` to proceed, or `FunctionResponse` to short-circuit.
2. `after_tool_callback` receives `(CallbackContext, tool_name: str, FunctionResponse)`. Return `None` to pass through, or a modified `FunctionResponse`.
3. **Do NOT** do heavy processing in callbacks. They run on every tool call and should be fast.
4. **Do NOT** import tool modules in callbacks. Callbacks should be generic.

---

### 5. Generate Content Config (UPDATE `agent.py`)

Add `generate_content_config` for temperature control:

```python
from google.genai.types import GenerateContentConfig

nl2sql_agent = LlmAgent(
    ...
    generate_content_config=GenerateContentConfig(temperature=0.1),
    ...
)
```

**NOTE**: `GenerateContentConfig` is from `google.genai.types`, not from ADK directly. Check the import path matches the installed version.

---

## Updated Directory Tree

After Track 04 is complete, new files (marked with ★):

```
nl2sql_agent/
├── __init__.py
├── agent.py                    ← UPDATED (callbacks, generate_content_config, callable instruction)
├── config.py
├── logging_config.py
├── protocols.py
├── clients.py
├── catalog_loader.py
├── prompts.py                  ★ NEW (build_nl2sql_instruction)
├── callbacks.py                ★ NEW (before_tool_guard, after_tool_log)
├── .env
├── .env.example
└── tools/                      (from Track 03, unchanged)
    ├── __init__.py
    ├── _deps.py
    ├── vector_search.py
    ├── metadata_loader.py
    ├── sql_validator.py
    ├── sql_executor.py
    └── learning_loop.py

tests/
├── conftest.py                 ← UPDATED (add mock for ReadonlyContext)
├── test_prompts.py             ★ NEW
├── test_callbacks.py           ★ NEW
├── test_agent_init.py          ← UPDATED (new assertions for callbacks, config)
├── test_routing_logic.py       ★ NEW (prompt-based routing tests)
├── ... (existing Track 01-03 tests unchanged)
```

---

## Test Specifications

### Test: System Prompt (`tests/test_prompts.py`)

```python
"""Tests for the NL2SQL system prompt."""

from unittest.mock import MagicMock
from datetime import date

from nl2sql_agent.prompts import build_nl2sql_instruction


class TestBuildInstruction:
    def _make_ctx(self):
        """Create a mock ReadonlyContext."""
        return MagicMock()

    def test_returns_string(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert isinstance(result, str)
        assert len(result) > 500  # Should be a comprehensive prompt

    def test_contains_today_date(self):
        result = build_nl2sql_instruction(self._make_ctx())
        today = date.today().isoformat()
        assert today in result

    def test_contains_project_id_from_settings(self):
        result = build_nl2sql_instruction(self._make_ctx())
        from nl2sql_agent.config import settings
        assert settings.gcp_project in result

    def test_contains_both_dataset_names(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "nl2sql_omx_kpi" in result
        assert "nl2sql_omx_data" in result

    def test_contains_all_kpi_table_names(self):
        result = build_nl2sql_instruction(self._make_ctx())
        for table in ["markettrade", "quotertrade", "brokertrade", "clicktrade", "otoswing"]:
            assert table in result

    def test_contains_theodata_routing_rule(self):
        """theodata must be explicitly called out as data-layer only."""
        result = build_nl2sql_instruction(self._make_ctx())
        assert "theodata" in result
        # Must mention it's unique to data layer
        lower = result.lower()
        assert "unique" in lower or "only" in lower

    def test_contains_tool_usage_order(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "vector_search_tables" in result
        assert "load_yaml_metadata" in result
        assert "fetch_few_shot_examples" in result
        assert "dry_run_sql" in result
        assert "execute_sql" in result

    def test_contains_sql_rules(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "ROUND()" in result or "ROUND" in result
        assert "trade_date" in result
        assert "LIMIT" in result

    def test_contains_union_all_pattern(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "UNION ALL" in result

    def test_contains_security_constraints(self):
        result = build_nl2sql_instruction(self._make_ctx())
        upper = result.upper()
        assert "NEVER" in upper
        assert "INSERT" in upper or "DELETE" in upper or "DROP" in upper

    def test_no_hardcoded_project_ids(self):
        """The prompt must not contain hardcoded project IDs."""
        result = build_nl2sql_instruction(self._make_ctx())
        # These are the actual project IDs that must NOT be hardcoded
        assert "melodic-stone-437916-t3" not in result

    def test_contains_clarification_rules(self):
        result = build_nl2sql_instruction(self._make_ctx())
        lower = result.lower()
        assert "clarif" in lower

    def test_contains_retry_instruction(self):
        result = build_nl2sql_instruction(self._make_ctx())
        assert "retry" in result.lower() or "3 times" in result or "3 attempts" in result
```

---

### Test: Callbacks (`tests/test_callbacks.py`)

```python
"""Tests for ADK callbacks."""

from unittest.mock import MagicMock

from nl2sql_agent.callbacks import before_tool_guard, after_tool_log


class TestBeforeToolGuard:
    def _make_ctx(self):
        return MagicMock()

    def _make_function_call(self, args=None):
        fc = MagicMock()
        fc.args = args or {}
        return fc

    def test_allows_select_query(self):
        ctx = self._make_ctx()
        fc = self._make_function_call({"sql_query": "SELECT * FROM table"})

        result = before_tool_guard(ctx, "execute_sql", fc)

        assert result is None  # None means "proceed"

    def test_allows_with_cte_query(self):
        ctx = self._make_ctx()
        fc = self._make_function_call({"sql_query": "WITH cte AS (SELECT 1) SELECT * FROM cte"})

        result = before_tool_guard(ctx, "execute_sql", fc)

        assert result is None

    def test_blocks_insert_query(self):
        ctx = self._make_ctx()
        fc = self._make_function_call({"sql_query": "INSERT INTO table VALUES (1)"})

        result = before_tool_guard(ctx, "execute_sql", fc)

        assert result is not None
        assert "error" in result.response["status"]

    def test_blocks_drop_query(self):
        ctx = self._make_ctx()
        fc = self._make_function_call({"sql_query": "DROP TABLE important_table"})

        result = before_tool_guard(ctx, "dry_run_sql", fc)

        assert result is not None
        assert "Blocked" in result.response["error_message"]

    def test_allows_non_sql_tools(self):
        """Non-SQL tools should always be allowed."""
        ctx = self._make_ctx()
        fc = self._make_function_call({"question": "what was the edge?"})

        result = before_tool_guard(ctx, "vector_search_tables", fc)

        assert result is None

    def test_blocks_delete_query(self):
        ctx = self._make_ctx()
        fc = self._make_function_call({"sql_query": "DELETE FROM table WHERE 1=1"})

        result = before_tool_guard(ctx, "execute_sql", fc)

        assert result is not None

    def test_blocks_update_query(self):
        ctx = self._make_ctx()
        fc = self._make_function_call({"sql_query": "UPDATE table SET x = 1"})

        result = before_tool_guard(ctx, "execute_sql", fc)

        assert result is not None


class TestAfterToolLog:
    def _make_ctx(self):
        return MagicMock()

    def _make_response(self, response_dict):
        fr = MagicMock()
        fr.response = response_dict
        return fr

    def test_returns_none_for_passthrough(self):
        ctx = self._make_ctx()
        fr = self._make_response({"status": "success", "row_count": 5})

        result = after_tool_log(ctx, "execute_sql", fr)

        assert result is None  # None means "don't modify response"

    def test_handles_error_response(self):
        ctx = self._make_ctx()
        fr = self._make_response({"status": "error", "error_message": "timeout"})

        result = after_tool_log(ctx, "execute_sql", fr)

        assert result is None  # Still passes through, just logs
```

---

### Test: Routing Logic (`tests/test_routing_logic.py`)

These tests verify that the prompt content covers all critical routing scenarios. They test the prompt TEXT, not LLM behavior (that's Track 05).

```python
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
        # markettrade should be called out as default
        assert "markettrade" in prompt

    def test_brokertrade_mentions_account_field(self):
        prompt = _get_prompt()
        lower = prompt.lower()
        # Must mention that brokertrade has account field
        assert "account" in lower
        # And that it's for broker comparison
        assert "bgc" in lower or "broker" in lower

    def test_quotertrade_kpi_vs_data_distinguished(self):
        """The prompt must distinguish KPI quotertrade from data quotertrade."""
        prompt = _get_prompt()
        # Should mention both KPI and data versions
        assert "quotertrade" in prompt
        # KPI version for performance metrics
        assert "quoter" in prompt.lower()

    def test_union_all_for_total_pnl(self):
        prompt = _get_prompt()
        assert "UNION ALL" in prompt
        assert "5" in prompt or "five" in prompt.lower()  # across all 5 tables


class TestDataRouting:
    def test_theodata_explicit_routing(self):
        """theodata must have explicit routing rules."""
        prompt = _get_prompt()
        lower = prompt.lower()
        assert "theodata" in lower
        # Must mention vol/IV/greeks routing
        assert "vol" in lower
        assert "delta" in lower

    def test_marketdata_vs_marketdepth(self):
        prompt = _get_prompt()
        lower = prompt.lower()
        assert "marketdata" in lower
        assert "marketdepth" in lower
        # Should distinguish between them
        assert "order book" in lower or "depth" in lower

    def test_kpi_vs_data_disambiguation(self):
        """Prompt must explain when to use KPI vs data for same-name tables."""
        prompt = _get_prompt()
        lower = prompt.lower()
        # Must explain the difference
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
        # Must explicitly prohibit DML/DDL
        assert "NEVER" in upper
        blocked_keywords = ["INSERT", "UPDATE", "DELETE", "DROP"]
        assert any(kw in upper for kw in blocked_keywords)

    def test_round_function_required(self):
        prompt = _get_prompt()
        assert "ROUND" in prompt

    def test_limit_required(self):
        prompt = _get_prompt()
        assert "LIMIT" in prompt
```

---

### Test: Agent Wiring Update (`tests/test_agent_init.py`)

Add assertions for the new Track 04 features:

```python
# Add to existing TestAgentStructure class:

def test_nl2sql_agent_has_generate_content_config(self):
    from nl2sql_agent.agent import nl2sql_agent
    assert nl2sql_agent.generate_content_config is not None
    assert nl2sql_agent.generate_content_config.temperature == 0.1

def test_nl2sql_agent_has_callbacks(self):
    from nl2sql_agent.agent import nl2sql_agent
    assert nl2sql_agent.before_tool_callback is not None
    assert nl2sql_agent.after_tool_callback is not None

def test_nl2sql_agent_instruction_is_callable(self):
    from nl2sql_agent.agent import nl2sql_agent
    # ADK supports callable instructions
    assert callable(nl2sql_agent.instruction) or isinstance(nl2sql_agent.instruction, str)
```

---

## Implementation Order

Execute these steps in EXACTLY this order:

### Step 1: Create `nl2sql_agent/prompts.py`
Write the `build_nl2sql_instruction` callable function as specified in File Spec #2.

### Step 2: Create `nl2sql_agent/callbacks.py`
Write the `before_tool_guard` and `after_tool_log` callbacks as specified in File Spec #4.

### Step 3: Update `nl2sql_agent/agent.py`
- Import `build_nl2sql_instruction` from `prompts.py`
- Import `before_tool_guard`, `after_tool_log` from `callbacks.py`
- Import `GenerateContentConfig` from `google.genai.types`
- Replace static instruction string with callable
- Add `generate_content_config=GenerateContentConfig(temperature=0.1)`
- Add `before_tool_callback=before_tool_guard`
- Add `after_tool_callback=after_tool_log`
- Update root agent instruction as specified in File Spec #3

### Step 4: Create `tests/test_prompts.py`
Create all prompt tests as specified in the test section.

### Step 5: Create `tests/test_callbacks.py`
Create all callback tests as specified in the test section.

### Step 6: Create `tests/test_routing_logic.py`
Create all routing logic tests as specified in the test section.

### Step 7: Update `tests/test_agent_init.py`
Add the 3 new assertions for generate_content_config, callbacks, and callable instruction.

### Step 8: Run tests
```bash
pytest tests/ -v
```
ALL tests must pass (Tracks 01 + 02 + 03 + 04).

### Step 9: Manual integration test
```bash
docker compose run --rm -p 8000:8000 agent adk web --host 0.0.0.0 --port 8000 nl2sql_agent
```

Test these 5 core scenarios in the ADK web UI:

1. **Theo query**: "What is the implied vol for EMBRACB?" → should route to `data.theodata`, use correct columns (vol, delta, tv)
2. **KPI performance**: "What was the edge on our trades today?" → should route to `kpi.markettrade`, filter on trade_date
3. **Broker comparison**: "How did BGC compare to MGN?" → should route to `kpi.brokertrade`, group by account
4. **Quoter performance**: "How did the quoter perform?" → should route to `kpi.quotertrade` (not `data.quotertrade`)
5. **Total PnL**: "What was total PnL across all trade types?" → should generate UNION ALL across all 5 KPI tables
6. **Greeting**: "Hello" → should stay at root agent, no tool calls
7. **Ambiguous**: "Show me quoter data" → should ask clarification (KPI metrics or raw execution data?)

Verify in the ADK trace panel that:
- Tools are called in the correct order
- The system prompt includes today's date
- dry_run_sql is called before execute_sql
- Temperature is 0.1 in model config

---

## Acceptance Criteria

Track 04 is DONE when ALL of the following are true:

- [ ] `nl2sql_agent/prompts.py` exists with `build_nl2sql_instruction` callable
- [ ] `nl2sql_agent/callbacks.py` exists with `before_tool_guard` and `after_tool_log`
- [ ] `nl2sql_agent/agent.py` uses callable instruction (not static string)
- [ ] `nl2sql_agent/agent.py` has `generate_content_config` with temperature=0.1
- [ ] `nl2sql_agent/agent.py` has `before_tool_callback` and `after_tool_callback`
- [ ] Root agent instruction refined for reliable delegation
- [ ] System prompt contains dynamic date injection (today's date)
- [ ] System prompt contains all 13 table names with routing rules
- [ ] System prompt contains KPI vs Data disambiguation rules
- [ ] System prompt contains theodata-specific routing
- [ ] System prompt contains UNION ALL pattern for "all trades"
- [ ] System prompt contains clarification rules
- [ ] System prompt contains SQL generation rules (ROUND, LIMIT, trade_date, FQN)
- [ ] System prompt contains security constraints (read-only, no DDL/DML)
- [ ] `before_tool_guard` blocks DML/DDL in SQL tools
- [ ] All project/dataset references use `settings.*` (no hardcoded values)
- [ ] `pytest tests/ -v` passes all tests (Tracks 01 + 02 + 03 + 04)
- [ ] Manual integration test passes 5 core scenarios in ADK web UI

---

## Anti-Patterns (DO NOT DO THESE)

| Anti-Pattern | Why It's Wrong | Correct Approach |
|---|---|---|
| Static instruction string with hardcoded date | "today" becomes stale | Use callable instruction with `date.today()` |
| Hardcoded project IDs in prompt | Breaks in dev vs prod | Use `settings.gcp_project` in f-string |
| Temperature=0.0 | Causes issues with some models | Use 0.1 for near-deterministic SQL |
| Heavy logic in callbacks | Slows every tool call | Keep callbacks lightweight (logging + simple guards) |
| Modifying tool responses in `after_tool_callback` | Breaks tool contract with LLM | Return None to pass through |
| Prompt without explicit table catalog | LLM hallucinates table names | Include full table listing in prompt |
| Relying solely on vector search for routing | Vector search can return wrong tables | Combine with explicit routing rules in prompt |
| Using `InvocationContext` in instruction function | Wrong context type, breaks encapsulation | Use `ReadonlyContext` (ADK provides this) |
| Putting prompt in agent.py | Makes it untestable, clutters agent definition | Separate module (prompts.py) |
| No retry instruction for dry_run failures | Agent gives up after first SQL error | Explicit "retry up to 3 times" instruction |
| `SELECT *` allowed in prompt | Excessive data transfer, unclear columns | Instruct to always specify column names |
| Missing UNION ALL pattern | Agent can't handle "all trades" questions | Explicit pattern in prompt |
| No clarification rules | Agent guesses when it should ask | Explicit rules for when to clarify |
