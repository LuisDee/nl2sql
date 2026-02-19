# Track 03: Agent Tools — Implementation Plan

## Objective

Build the 6 Python tool functions that give the NL2SQL agent its capabilities: semantic table routing (`vector_search_tables`), few-shot example retrieval (`fetch_few_shot_examples`), YAML metadata loading (`load_yaml_metadata`), SQL validation (`dry_run_sql`), SQL execution (`execute_sql`), and learning loop (`save_validated_query`). Wire all 6 into `nl2sql_agent` so the LLM can call them. At the end of this track, `adk web` shows the agent calling tools in sequence when asked a data question — vector search → metadata → examples → dry run → execute — and the trace shows each tool invocation with inputs and outputs.

**Dependency**: Track 01 complete (agent skeleton, config, logging, Docker). Track 02 complete (YAML catalog, embedding tables populated, vector search validated).

---

## CARRIED FORWARD: Track 01 + 02 Conventions

These conventions from previous tracks remain in force. Do NOT violate them.

1. **ADK conventions**: `root_agent` variable, `__init__.py` with `from . import agent`, `.env` inside agent package.
2. **LiteLLM conventions**: `LiteLlm` (camelCase), model string via `settings.litellm_model`, env vars for API key/base.
3. **Protocol-based DI**: All BigQuery interactions go through `BigQueryProtocol` (defined in `protocols.py`, implemented by `LiveBigQueryClient` in `clients.py`). Never import `bigquery.Client` directly in tool logic.
4. **Configuration**: All config via `from nl2sql_agent.config import settings`. Never use `os.getenv()`.
5. **Logging**: `from nl2sql_agent.logging_config import get_logger`. Structured JSON via structlog. Every tool logs inputs and outputs.
6. **YAML**: Always `yaml.safe_load()`, never `yaml.load()`.
7. **Fully-qualified table names**: All SQL uses `` `{settings.gcp_project}.dataset.table` `` format. **Never hardcode project IDs.**
8. **Embedding model**: Referenced via `settings.embedding_model_ref` (cross-project in prod). Never create a new one.
9. **Metadata dataset**: Referenced via `settings.metadata_dataset` (default: `nl2sql_metadata`).
10. **Data datasets**: `settings.kpi_dataset` (gold, 5 tables) and `settings.data_dataset` (silver, 7 tables in dev).
11. **BigQuery ARRAY semantics**: Arrays are never NULL, they're empty `[]`. Use `ARRAY_LENGTH(col) = 0` not `col IS NULL`.

---

## Real Infrastructure Values (for reference only — code MUST use settings.*)

All values below come from `nl2sql_agent/.env`. Code must reference `settings.*` fields, never these literal strings.

```
settings.gcp_project           → dev: melodic-stone-437916-t3 | prod: cloud-data-n-base-d4b3
settings.bq_location           → europe-west2
settings.kpi_dataset           → nl2sql_omx_kpi       (5 tables, gold layer)
settings.data_dataset          → nl2sql_omx_data      (7 tables in dev, 8 in prod)
settings.metadata_dataset      → nl2sql_metadata      (embedding tables)
settings.embedding_model_ref   → dev: melodic-stone-437916-t3.nl2sql.text_embedding_model
                                 prod: cloud-ai-d-base-a2df.nl2sql.text_embedding_model
settings.vertex_ai_connection  → (cross-project in prod)
Embedding Dimension:           768 (ARRAY<FLOAT64>)
Distance Type:                 COSINE
```

**DO NOT** hardcode any of these values in code, SQL templates, agent instructions, or tests. Always use `settings.*`.

---

## CRITICAL TRACK 03 CONVENTIONS

### ADK Tool Patterns

ADK auto-discovers tools. When you add a plain Python function to an agent's `tools=[]` list, ADK wraps it as a `FunctionTool` automatically. The LLM reads the function's **name**, **docstring**, **parameter names**, and **type hints** to decide when and how to call it.

**Rules for tool functions:**

1. **Docstring is everything.** The LLM uses the docstring to decide when to call the tool. Make it specific and include trigger phrases the LLM should associate with this tool.
2. **Type hints are mandatory** on all parameters. ADK uses them to build the tool schema. Missing type hints = broken tool.
3. **Return type must be `dict`** (preferred) or `str`. If you return a non-dict, ADK wraps it as `{"result": value}`. We return `dict` explicitly for clarity.
4. **Parameter names matter.** The LLM sees them. Use descriptive names: `question` not `q`, `sql_query` not `sql`.
5. **Use basic types only.** `str`, `int`, `float`, `bool`, `list[str]`. No custom objects, no Pydantic models as params.
6. **`tool_context: ToolContext` is optional.** If present in the signature, ADK auto-injects it. We use it in the learning loop to store state. Do NOT add it to tools that don't need it — it's invisible to the LLM but clutters the function signature.
7. **No `FunctionTool()` wrapper needed.** Just pass the function directly to `tools=[]`. ADK wraps it automatically.

```python
# CORRECT — just a plain function in the tools list
def my_tool(question: str) -> dict:
    """Tool docstring the LLM reads."""
    return {"result": "answer"}

agent = LlmAgent(
    name="my_agent",
    model=...,
    tools=[my_tool],  # ADK wraps this automatically
)
```

```python
# WRONG — unnecessary FunctionTool wrapper
from google.adk.tools import FunctionTool
my_tool_wrapped = FunctionTool(my_tool)
agent = LlmAgent(tools=[my_tool_wrapped])  # Works but verbose
```

**DO NOT** use `@tool` decorator (doesn't exist in ADK).

**DO NOT** make tools async unless they need to be. Sync functions work fine and are simpler.

**DO NOT** import `FunctionTool` — just pass plain functions to `tools=[]`.

### Protocol-Based Tool Dependencies

Tools need BigQuery access. But we do NOT import `bigquery.Client` inside tool modules. Instead:

1. A `BigQueryProtocol` defines the interface.
2. The existing `LiveBigQueryClient` in `clients.py` implements it.
3. Tools receive the service via **module-level dependency injection** — set once at agent init time.
4. Tests swap in a mock that implements the same protocol.

```python
# nl2sql_agent/protocols.py
from typing import Protocol

class BigQueryProtocol(Protocol):
    def query(self, sql: str, params: list | None = None) -> list[dict]:
        """Execute a query and return rows as list of dicts."""
        ...

    def dry_run(self, sql: str) -> dict:
        """Dry-run a query and return metadata."""
        ...
```

This is the ONLY way tools talk to BigQuery. Never `from google.cloud import bigquery` inside a tool file.

### Tool Error Handling

All tools must return a dict, even on error. The LLM needs structured feedback to retry or explain the failure.

```python
# CORRECT — structured error in dict
return {"status": "error", "error_message": "Table not found: theodata2"}

# WRONG — raising an exception (LLM can't handle this)
raise ValueError("Table not found")

# WRONG — returning a bare string error
return "ERROR: Table not found"
```

### Timeout Convention

All BigQuery queries executed by tools MUST set a timeout. Use `settings.bq_query_timeout_seconds` (default: 30). This prevents runaway queries from blocking the agent.

---

## File-by-File Specification

### 1. Protocol Definitions (EXISTING — extend, do not replace)

**Path**: `nl2sql_agent/protocols.py`

This file ALREADY EXISTS from Track 01. It defines `BigQueryProtocol` with `execute_query()`, `dry_run_query()`, and `get_table_schema()`, plus `EmbeddingProtocol`. The existing `LiveBigQueryClient` in `clients.py` implements `BigQueryProtocol`.

**What to add**: A `query_with_params()` method for parameterised queries (needed by vector search tools to prevent SQL injection on user input):

```python
    def query_with_params(self, sql: str, params: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        """Execute a parameterised SQL query and return results as list of dicts.

        Args:
            sql: BigQuery SQL query with @param placeholders.
            params: List of query parameter dicts, each with keys:
                    name (str), type (str), value (Any).

        Returns:
            List of dicts, one per row. Column names are keys.
        """
        ...
```

**DO NOT** replace or rename the existing methods (`execute_query`, `dry_run_query`, `get_table_schema`). Only add `query_with_params`.

**DO NOT** create a separate `BigQueryToolProtocol`. Extend the existing one.

The tools will use:
- `query_with_params()` for parameterised queries (vector search, learning loop)
- `execute_query()` for non-parameterised queries (execute_sql tool returns DataFrame → converted to list[dict])
- `dry_run_query()` for SQL validation

---

### 2. BigQuery Client Update (EXISTING — extend `clients.py`, do NOT create `services.py`)

**Path**: `nl2sql_agent/clients.py`

This file ALREADY EXISTS from Track 01 with `LiveBigQueryClient` implementing `BigQueryProtocol`. It already has `execute_query()` (returns DataFrame) and `dry_run_query()`.

**What to add**: The `query_with_params()` method for parameterised queries:

```python
    def query_with_params(self, sql: str, params: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        """Execute a parameterised SQL query and return results as list of dicts."""
        job_config = bigquery.QueryJobConfig()

        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(p["name"], p["type"], p["value"])
                for p in params
            ]

        logger.info("bq_query_with_params", sql_preview=sql[:200], has_params=bool(params))

        try:
            query_job = self._client.query(
                sql,
                job_config=job_config,
                timeout=settings.bq_query_timeout_seconds,
            )
            rows = [dict(row) for row in query_job.result(timeout=settings.bq_query_timeout_seconds)]
            logger.info("bq_query_with_params_complete", row_count=len(rows))
            return rows
        except Exception as e:
            logger.error("bq_query_with_params_error", error=str(e), sql_preview=sql[:200])
            raise
```

Also add `from typing import Any` to imports.

**DO NOT** create `nl2sql_agent/services.py`. The existing `clients.py` is the only file that imports `from google.cloud import bigquery`.

**IMPORTANT**: The `timeout` parameter uses `settings.bq_query_timeout_seconds`. We need to add this to the Settings class.

---

### 3. Config Update

**Path**: `nl2sql_agent/config.py` (UPDATE — add new fields only)

The following fields ALREADY EXIST from Tracks 01/02 and must NOT be duplicated:
- `metadata_dataset` (default: `nl2sql_metadata`)
- `embedding_model_ref` (default: `cloud-ai-d-base-a2df.nl2sql.text_embedding_model`)

Add ONLY these NEW fields to the existing `Settings` class:

```python
    # --- Query Limits (NEW for Track 03) ---
    bq_query_timeout_seconds: float = Field(
        default=30.0,
        description="Timeout in seconds for BigQuery query execution",
    )
    bq_max_result_rows: int = Field(
        default=1000,
        description="Maximum rows returned by execute_sql tool",
    )
    vector_search_top_k: int = Field(
        default=5,
        description="Number of results for vector search queries",
    )
```

**DO NOT** add `embedding_model_fqn` — use existing `settings.embedding_model_ref` everywhere.

**DO NOT** add `metadata_dataset` — it already exists.

**DO NOT** remove or rename any existing fields.

---

### 4. Tool Dependencies Module

**Path**: `nl2sql_agent/tools/_deps.py`

```python
"""Shared dependency injection for all tool modules.

This module holds the BigQuery service instance that all tools share.
It is initialised once at agent startup via init_bq_service().

Usage (inside tool modules):
    from nl2sql_agent.tools._deps import get_bq_service
"""

from typing import Any

from nl2sql_agent.logging_config import get_logger

logger = get_logger(__name__)

_bq_service: Any = None


def init_bq_service(service) -> None:
    """Set the shared BigQuery service for all tools.

    Args:
        service: An object implementing BigQueryProtocol.
    """
    global _bq_service
    _bq_service = service
    logger.info("tools_bq_service_initialised", service_type=type(service).__name__)


def get_bq_service() -> Any:
    """Get the shared BigQuery service.

    Raises:
        RuntimeError: If init_bq_service() has not been called yet.
    """
    if _bq_service is None:
        raise RuntimeError(
            "BigQuery service not initialised. Call init_bq_service() in agent.py before using tools."
        )
    return _bq_service
```

---

### 5. Tool: Vector Search Tables

**Path**: `nl2sql_agent/tools/vector_search.py`

```python
"""Vector search tools for semantic table and example routing.

These tools use BigQuery VECTOR_SEARCH to find the most relevant tables
and past validated queries for a given natural language question.

The embedding model and metadata dataset are configured via settings:
    settings.embedding_model_ref  (e.g. project.dataset.model)
    settings.metadata_dataset     (e.g. nl2sql_metadata)
"""

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.tools._deps import get_bq_service

logger = get_logger(__name__)


# --- VECTOR_SEARCH SQL Templates ---
# These are parameterised SQL strings. The @question parameter is injected
# via BigQuery query parameters (safe from SQL injection).
#
# IMPORTANT: We use RETRIEVAL_QUERY task type for the search query
# (not RETRIEVAL_DOCUMENT — that's for stored content).
#
# IMPORTANT: The embedding model is cross-project. We reference it by its
# fully-qualified name.

_SCHEMA_SEARCH_SQL = """
SELECT
    base.source_type,
    base.layer,
    base.dataset_name,
    base.table_name,
    base.description,
    ROUND(distance, 4) AS distance
FROM VECTOR_SEARCH(
    (SELECT * FROM `{metadata_dataset}.schema_embeddings`),
    'embedding',
    (
        SELECT ml_generate_embedding_result AS embedding
        FROM ML.GENERATE_EMBEDDING(
            MODEL `{embedding_model}`,
            (SELECT @question AS content),
            STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output)
        )
    ),
    top_k => {top_k},
    distance_type => 'COSINE'
)
ORDER BY distance ASC
"""

_QUERY_MEMORY_SEARCH_SQL = """
SELECT
    base.question AS past_question,
    base.sql_query,
    base.tables_used,
    base.dataset AS past_dataset,
    base.complexity,
    base.routing_signal,
    ROUND(distance, 4) AS distance
FROM VECTOR_SEARCH(
    (SELECT * FROM `{metadata_dataset}.query_memory`),
    'embedding',
    (
        SELECT ml_generate_embedding_result AS embedding
        FROM ML.GENERATE_EMBEDDING(
            MODEL `{embedding_model}`,
            (SELECT @question AS content),
            STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output)
        )
    ),
    top_k => {top_k},
    distance_type => 'COSINE'
)
ORDER BY distance ASC
"""


def vector_search_tables(question: str) -> dict:
    """Find the most relevant BigQuery tables for a natural language question.

    Use this tool FIRST for every data question. It searches table and dataset
    descriptions using semantic similarity to determine which tables contain
    the data needed to answer the question. Results include table names,
    dataset names, descriptions, and relevance scores.

    Examples of when to use this tool:
    - "what was the edge on our trade?" → finds KPI tables
    - "how did implied vol change?" → finds theodata
    - "broker BGC vs MGN performance" → finds kpi brokertrade
    - "what levels were we quoting at 11:15?" → finds data quotertrade

    Args:
        question: The trader's natural language question about trading data.

    Returns:
        Dict with 'status' and 'results' (list of matching tables with
        source_type, layer, dataset_name, table_name, description, distance).
    """
    bq = get_bq_service()

    fq_metadata = f"{settings.gcp_project}.{settings.metadata_dataset}"
    sql = _SCHEMA_SEARCH_SQL.format(
        metadata_dataset=fq_metadata,
        embedding_model=settings.embedding_model_ref,
        top_k=settings.vector_search_top_k,
    )

    logger.info("vector_search_tables_start", question=question[:100])

    try:
        rows = bq.query_with_params(
            sql,
            params=[{"name": "question", "type": "STRING", "value": question}],
        )
        logger.info("vector_search_tables_complete", result_count=len(rows))
        return {"status": "success", "results": rows}
    except Exception as e:
        logger.error("vector_search_tables_error", error=str(e))
        return {"status": "error", "error_message": str(e), "results": []}


def fetch_few_shot_examples(question: str) -> dict:
    """Find similar past validated SQL queries to use as few-shot examples.

    Use this tool AFTER vector_search_tables to find proven question→SQL
    patterns that are similar to the current question. These examples help
    generate accurate SQL by showing correct table names, column names,
    WHERE clauses, and aggregation patterns.

    The results include the original question, the validated SQL query,
    which tables were used, complexity level, and a routing signal explaining
    why that table was chosen.

    Args:
        question: The trader's natural language question about trading data.

    Returns:
        Dict with 'status' and 'examples' (list of past validated queries
        with past_question, sql_query, tables_used, complexity, distance).
    """
    bq = get_bq_service()

    fq_metadata = f"{settings.gcp_project}.{settings.metadata_dataset}"
    sql = _QUERY_MEMORY_SEARCH_SQL.format(
        metadata_dataset=fq_metadata,
        embedding_model=settings.embedding_model_ref,
        top_k=settings.vector_search_top_k,
    )

    logger.info("fetch_few_shot_start", question=question[:100])

    try:
        rows = bq.query_with_params(
            sql,
            params=[{"name": "question", "type": "STRING", "value": question}],
        )
        logger.info("fetch_few_shot_complete", example_count=len(rows))
        return {"status": "success", "examples": rows}
    except Exception as e:
        logger.error("fetch_few_shot_error", error=str(e))
        return {"status": "error", "error_message": str(e), "examples": []}
```

**CRITICAL DETAILS**:

1. SQL templates use `.format()` for structural parts (dataset names, model references, top_k) and `@question` BigQuery query parameters for user input. This prevents SQL injection on user input while allowing config-driven dataset/model names.
2. All tools import `get_bq_service` from `_deps.py` — centralised dependency.
3. All `VECTOR_SEARCH` calls use `distance_type => 'COSINE'` and `RETRIEVAL_QUERY` task type.
4. Return type is always `dict` with `status` key.

**DO NOT** use f-strings for the `@question` parameter. That would be SQL injection. Use BigQuery query parameters.

**DO NOT** hardcode dataset or model names. Use `settings.*` for everything.

**DO NOT** use `ML.GENERATE_EMBEDDING` without `flatten_json_output = TRUE`. Without it, the output structure is nested and VECTOR_SEARCH won't match.

---

### 6. Tool: Metadata Loader

**Path**: `nl2sql_agent/tools/metadata_loader.py`

```python
"""YAML metadata loading tool for the NL2SQL agent.

Loads rich table descriptions, column metadata, synonyms, and business rules
from the YAML catalog files created in Track 02. This gives the LLM detailed
context about what each column means and how to write correct SQL.

Depends on: catalog_loader module from Track 02.
"""

import yaml

from nl2sql_agent.catalog_loader import load_yaml, CATALOG_DIR
from nl2sql_agent.logging_config import get_logger

logger = get_logger(__name__)

# Map of unique table names to their YAML file paths (relative to CATALOG_DIR).
# This mapping must match the actual catalog files created in Track 02.
#
# For tables that exist in BOTH datasets (markettrade, quotertrade, clicktrade),
# they are NOT in this map — use _DATASET_TABLE_MAP with (dataset, table) instead.
# This map is for tables with unique names only (no ambiguity).
_TABLE_YAML_MAP: dict[str, str] = {
    # KPI-only tables
    "brokertrade": "kpi/brokertrade.yaml",  # Defaults to KPI (most common ask)
    "otoswing": "kpi/otoswing.yaml",
    # Data-only tables
    "theodata": "data/theodata.yaml",
    "swingdata": "data/swingdata.yaml",
    "marketdata": "data/marketdata.yaml",
    "marketdepth": "data/marketdepth.yaml",
}

# Aliases — the vector search may return just "markettrade" without
# specifying kpi vs data. These aliases resolve ambiguity using the dataset.
_DATASET_TABLE_MAP: dict[tuple[str, str], str] = {
    ("nl2sql_omx_kpi", "markettrade"): "kpi/markettrade.yaml",
    ("nl2sql_omx_kpi", "quotertrade"): "kpi/quotertrade.yaml",
    ("nl2sql_omx_kpi", "brokertrade"): "kpi/brokertrade.yaml",
    ("nl2sql_omx_kpi", "clicktrade"): "kpi/clicktrade.yaml",
    ("nl2sql_omx_kpi", "otoswing"): "kpi/otoswing.yaml",
    ("nl2sql_omx_data", "theodata"): "data/theodata.yaml",
    ("nl2sql_omx_data", "quotertrade"): "data/quotertrade.yaml",
    ("nl2sql_omx_data", "markettrade"): "data/markettrade.yaml",
    # NOTE: data/brokertrade.yaml does not exist in dev (table not in sample data)
    # ("nl2sql_omx_data", "brokertrade"): "data/brokertrade.yaml",
    ("nl2sql_omx_data", "clicktrade"): "data/clicktrade.yaml",
    ("nl2sql_omx_data", "swingdata"): "data/swingdata.yaml",
    ("nl2sql_omx_data", "marketdata"): "data/marketdata.yaml",
    ("nl2sql_omx_data", "marketdepth"): "data/marketdepth.yaml",
}


def _resolve_yaml_path(table_name: str, dataset_name: str = "") -> str | None:
    """Resolve a table name + optional dataset to a YAML file path.

    Tries dataset+table first (most specific), then table alone.
    Returns None if no mapping found.
    """
    # Try dataset+table combo first
    if dataset_name:
        key = (dataset_name, table_name)
        if key in _DATASET_TABLE_MAP:
            return _DATASET_TABLE_MAP[key]

    # Try direct table name
    if table_name in _TABLE_YAML_MAP:
        return _TABLE_YAML_MAP[table_name]

    # Try case-insensitive match
    for known_name, path in _TABLE_YAML_MAP.items():
        if known_name.lower() == table_name.lower():
            return path

    return None


def load_yaml_metadata(table_name: str, dataset_name: str = "") -> dict:
    """Load the YAML metadata catalog for a specific BigQuery table.

    Use this tool AFTER vector_search_tables to get detailed column
    descriptions, synonyms, business rules, and data types for the
    tables identified as relevant. This metadata is essential for
    generating correct SQL — it tells you exact column names, what
    they mean, what values they contain, and how calculations work.

    For KPI tables (nl2sql_omx_kpi), the response also includes the
    KPI dataset context with shared column definitions and routing rules.

    Args:
        table_name: The table to load metadata for. Examples:
            'markettrade', 'theodata', 'quotertrade', 'brokertrade'.
        dataset_name: Optional dataset name to disambiguate tables that
            exist in both KPI and data datasets. Examples:
            'nl2sql_omx_kpi', 'nl2sql_omx_data'.

    Returns:
        Dict with 'status' and either 'metadata' (the full YAML content
        as a string) or 'error_message' if the table was not found.
    """
    logger.info(
        "load_yaml_metadata_start",
        table_name=table_name,
        dataset_name=dataset_name,
    )

    yaml_path = _resolve_yaml_path(table_name, dataset_name)
    if yaml_path is None:
        logger.warning("load_yaml_metadata_not_found", table_name=table_name)
        return {
            "status": "error",
            "error_message": (
                f"No metadata found for table '{table_name}'"
                + (f" in dataset '{dataset_name}'" if dataset_name else "")
                + f". Known tables: {sorted(_TABLE_YAML_MAP.keys())}"
            ),
        }

    full_path = CATALOG_DIR / yaml_path
    if not full_path.exists():
        logger.error("load_yaml_metadata_file_missing", path=str(full_path))
        return {
            "status": "error",
            "error_message": f"YAML file not found at {full_path}. Was Track 02 completed?",
        }

    try:
        content = load_yaml(full_path)
    except Exception as e:
        logger.error("load_yaml_metadata_parse_error", path=str(full_path), error=str(e))
        return {"status": "error", "error_message": f"Failed to parse YAML: {e}"}

    # If this is a KPI table, also load the KPI dataset context
    if "kpi/" in yaml_path:
        dataset_yaml_path = CATALOG_DIR / "kpi" / "_dataset.yaml"
        if dataset_yaml_path.exists():
            try:
                dataset_context = load_yaml(dataset_yaml_path)
                content["_kpi_dataset_context"] = dataset_context
            except Exception:
                pass  # Non-fatal — table metadata is still useful without dataset context

    # If this is a data table, also load the data dataset context
    if "data/" in yaml_path:
        dataset_yaml_path = CATALOG_DIR / "data" / "_dataset.yaml"
        if dataset_yaml_path.exists():
            try:
                dataset_context = load_yaml(dataset_yaml_path)
                content["_data_dataset_context"] = dataset_context
            except Exception:
                pass

    # Convert to YAML string for the LLM (more readable than nested dict repr)
    metadata_str = yaml.dump(content, default_flow_style=False, sort_keys=False)

    logger.info(
        "load_yaml_metadata_complete",
        table_name=table_name,
        yaml_path=yaml_path,
        content_length=len(metadata_str),
    )

    return {
        "status": "success",
        "table_name": table_name,
        "dataset_name": dataset_name or content.get("table", {}).get("dataset", "unknown"),
        "metadata": metadata_str,
    }
```

**CRITICAL DETAILS**:

1. The `_DATASET_TABLE_MAP` uses `(dataset, table)` tuples to resolve the `kpi.quotertrade` vs `data.quotertrade` ambiguity. Vector search results include both `dataset_name` and `table_name`, so the LLM should pass both.
2. The function returns `metadata` as a YAML-formatted string, not a raw dict. YAML is more readable for the LLM than Python dict repr.
3. Dataset context (`_dataset.yaml`) is automatically appended for KPI and data tables. This gives the LLM routing rules and shared column definitions without a separate tool call.

**DO NOT** return the raw dict from `yaml.safe_load()`. Convert it to a YAML string — the LLM reads text, not Python objects.

**DO NOT** raise exceptions. Always return a dict with `status` and `error_message`.

---

### 7. Tool: SQL Dry Run (Validator)

**Path**: `nl2sql_agent/tools/sql_validator.py`

```python
"""SQL validation tool using BigQuery dry run.

Validates SQL syntax, column references, table permissions, and estimates
query cost — all without executing the query.
"""

from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.tools._deps import get_bq_service

logger = get_logger(__name__)


def dry_run_sql(sql_query: str) -> dict:
    """Validate a BigQuery SQL query without executing it.

    Use this tool AFTER generating SQL to check for syntax errors,
    invalid column names, missing table permissions, and to estimate
    the query cost in bytes processed. If the dry run fails, examine
    the error message and fix the SQL before trying execute_sql.

    Common errors and fixes:
    - "Unrecognized name: column_x" → check YAML metadata for correct column name
    - "Not found: Table" → check fully-qualified table name format
    - "Access Denied" → table may not exist or permissions missing

    Args:
        sql_query: The BigQuery SQL query to validate.

    Returns:
        Dict with 'status' ('valid' or 'invalid'), and either
        'estimated_bytes' and 'estimated_mb' if valid, or
        'error_message' if invalid.
    """
    bq = get_bq_service()

    logger.info("dry_run_start", sql_preview=sql_query[:200])

    result = bq.dry_run_query(sql_query)

    if result["valid"]:
        mb = result["total_bytes_processed"] / (1024 * 1024)
        logger.info("dry_run_valid", estimated_mb=round(mb, 2))
        return {
            "status": "valid",
            "estimated_bytes": result["total_bytes_processed"],
            "estimated_mb": round(mb, 2),
        }
    else:
        logger.warning("dry_run_invalid", error=result["error"])
        return {
            "status": "invalid",
            "error_message": result["error"],
        }
```

---

### 8. Tool: SQL Executor

**Path**: `nl2sql_agent/tools/sql_executor.py`

```python
"""SQL execution tool for BigQuery read-only queries.

Executes validated SQL and returns results. Enforces read-only (SELECT only)
and row limits to prevent runaway queries.
"""

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.tools._deps import get_bq_service

logger = get_logger(__name__)


def execute_sql(sql_query: str) -> dict:
    """Execute a validated BigQuery SQL query and return the results.

    Use this tool ONLY after dry_run_sql confirms the query is valid.
    Only SELECT queries are allowed — any attempt to INSERT, UPDATE,
    DELETE, DROP, or otherwise modify data will be rejected.

    Results are limited to 1000 rows maximum. If the query returns more,
    the results are truncated and a warning is included.

    Args:
        sql_query: The BigQuery SQL query to execute (must be SELECT).

    Returns:
        Dict with 'status', 'row_count', and 'rows' (list of row dicts)
        if successful, or 'error_message' if execution failed.
    """
    # --- Read-only enforcement ---
    stripped = sql_query.strip()
    first_keyword = stripped.split()[0].upper() if stripped else ""
    if first_keyword not in ("SELECT", "WITH"):
        logger.warning("execute_sql_rejected", first_keyword=first_keyword)
        return {
            "status": "error",
            "error_message": (
                f"Only SELECT queries are allowed. Got: {first_keyword}. "
                "This tool is read-only and cannot modify data."
            ),
        }

    # --- Add LIMIT if not present ---
    max_rows = settings.bq_max_result_rows
    upper = stripped.upper()
    if "LIMIT" not in upper:
        sql_query = f"{stripped}\nLIMIT {max_rows}"
        logger.info("execute_sql_limit_added", limit=max_rows)

    bq = get_bq_service()

    logger.info("execute_sql_start", sql_preview=sql_query[:200])

    try:
        df = bq.execute_query(sql_query)
        rows = df.to_dict(orient="records")
        truncated = len(rows) >= max_rows

        logger.info(
            "execute_sql_complete",
            row_count=len(rows),
            truncated=truncated,
        )

        result = {
            "status": "success",
            "row_count": len(rows),
            "rows": rows,
        }
        if truncated:
            result["warning"] = f"Results truncated to {max_rows} rows. Add more specific filters to see all data."

        return result

    except Exception as e:
        logger.error("execute_sql_error", error=str(e))
        return {
            "status": "error",
            "error_message": str(e),
        }
```

**CRITICAL DETAILS**:

1. **Read-only enforcement**: Checks the first keyword is `SELECT` or `WITH` (for CTEs). Rejects everything else.
2. **Auto-LIMIT**: If the SQL doesn't contain `LIMIT`, one is appended. This prevents accidentally scanning entire tables.
3. **WITH support**: CTEs start with `WITH`, not `SELECT`. Both are valid read-only queries.

**DO NOT** check for `SELECT` only. CTEs use `WITH ... AS (SELECT ...) SELECT ...`.

**DO NOT** use regex to detect LIMIT. Simple `"LIMIT" not in upper` is sufficient — false positives (e.g., column named `rate_limit`) are harmless since we're just adding an extra LIMIT.

---

### 9. Tool: Learning Loop

**Path**: `nl2sql_agent/tools/learning_loop.py`

```python
"""Learning loop tool — saves validated question→SQL pairs for future retrieval.

When a trader confirms a query result is correct, this tool saves the
question and SQL to query_memory. The embedding is generated immediately
so the pair is available for future few-shot retrieval.
"""

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.tools._deps import get_bq_service

logger = get_logger(__name__)

_INSERT_SQL = """
INSERT INTO `{metadata_dataset}.query_memory`
    (question, sql_query, tables_used, dataset, complexity, routing_signal, validated_by)
VALUES
    (@question, @sql_query, SPLIT(@tables_used, ','), @dataset, @complexity, @routing_signal, 'trader')
"""

_EMBED_NEW_ROWS_SQL = """
UPDATE `{metadata_dataset}.query_memory` t
SET embedding = (
    SELECT ml_generate_embedding_result
    FROM ML.GENERATE_EMBEDDING(
        MODEL `{embedding_model}`,
        (SELECT t.question AS content),
        STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_DOCUMENT' AS task_type)
    )
)
WHERE ARRAY_LENGTH(t.embedding) = 0
"""


def save_validated_query(
    question: str,
    sql_query: str,
    tables_used: str,
    dataset: str,
    complexity: str,
    routing_signal: str,
) -> dict:
    """Save a validated question→SQL pair to query memory for future retrieval.

    Call this tool when the trader confirms the query result was correct.
    The pair will be embedded and available for future few-shot retrieval,
    improving accuracy for similar questions.

    Args:
        question: The original natural language question.
        sql_query: The SQL query that produced correct results.
        tables_used: Comma-separated list of table names used.
            Example: "markettrade" or "markettrade,brokertrade"
        dataset: The dataset used. Example: "nl2sql_omx_kpi" or "nl2sql_omx_data".
        complexity: Query complexity level: "simple", "medium", or "complex".
        routing_signal: Brief note on why this table was chosen.

    Returns:
        Dict with 'status' and confirmation message.
    """
    bq = get_bq_service()

    fq_metadata = f"{settings.gcp_project}.{settings.metadata_dataset}"

    logger.info(
        "save_validated_query_start",
        question=question[:100],
        tables_used=tables_used,
    )

    # Step 1: Insert the row
    insert_sql = _INSERT_SQL.format(metadata_dataset=fq_metadata)

    try:
        bq.query_with_params(
            insert_sql,
            params=[
                {"name": "question", "type": "STRING", "value": question},
                {"name": "sql_query", "type": "STRING", "value": sql_query},
                {"name": "tables_used", "type": "STRING", "value": tables_used},
                {"name": "dataset", "type": "STRING", "value": dataset},
                {"name": "complexity", "type": "STRING", "value": complexity},
                {"name": "routing_signal", "type": "STRING", "value": routing_signal},
            ],
        )
    except Exception as e:
        logger.error("save_validated_query_insert_error", error=str(e))
        return {"status": "error", "error_message": f"Failed to insert: {e}"}

    # Step 2: Generate embedding for the new row
    embed_sql = _EMBED_NEW_ROWS_SQL.format(
        metadata_dataset=fq_metadata,
        embedding_model=settings.embedding_model_ref,
    )

    try:
        bq.execute_query(embed_sql)
        tables_array = [t.strip() for t in tables_used.split(",")]
        logger.info(
            "save_validated_query_complete",
            tables_used=tables_array,
        )
        return {
            "status": "success",
            "message": (
                f"Saved validated query. Tables: {tables_array}. "
                "This will improve future answers to similar questions."
            ),
        }
    except Exception as e:
        # Insert succeeded but embedding failed — still a partial success
        logger.warning("save_validated_query_embed_error", error=str(e))
        return {
            "status": "partial_success",
            "message": (
                f"Query saved but embedding generation failed: {e}. "
                "The query is stored and will be embedded on next batch run."
            ),
        }
```

**CRITICAL DETAILS**:

1. Two-step process: INSERT first, then UPDATE with embedding. BigQuery scripting (multi-statement) has quirks with parameterised queries, so we split them.
2. `tables_used` is a comma-separated string (not a list) because ADK tool params should be basic types. The INSERT SQL uses `SPLIT(@tables_used, ',')` to convert to ARRAY for the `tables_used ARRAY<STRING>` column.
3. Embedding uses `RETRIEVAL_DOCUMENT` task type (this is stored content to be retrieved, not a search query).
4. The UPDATE targets only rows with `ARRAY_LENGTH(embedding) = 0` (BQ arrays are never NULL, they're empty `[]`).
5. All tool parameters are required (no defaults) per ADK best practice — default values can break tool schema discovery.

**DO NOT** use `ARRAY` type for `tables_used` parameter. ADK can't auto-generate the schema for complex types. Use a comma-separated string.

**DO NOT** use `WHERE embedding IS NULL`. BQ arrays are never NULL — use `ARRAY_LENGTH(embedding) = 0`.

---

### 10. Tools Package Init

**Path**: `nl2sql_agent/tools/__init__.py`

```python
"""NL2SQL agent tools package.

All tools are plain functions that ADK wraps as FunctionTool automatically.
Import and use init_bq_service() in agent.py to set up dependencies.
"""

from nl2sql_agent.tools._deps import init_bq_service
from nl2sql_agent.tools.vector_search import vector_search_tables, fetch_few_shot_examples
from nl2sql_agent.tools.metadata_loader import load_yaml_metadata
from nl2sql_agent.tools.sql_validator import dry_run_sql
from nl2sql_agent.tools.sql_executor import execute_sql
from nl2sql_agent.tools.learning_loop import save_validated_query

__all__ = [
    "init_bq_service",
    "vector_search_tables",
    "fetch_few_shot_examples",
    "load_yaml_metadata",
    "dry_run_sql",
    "execute_sql",
    "save_validated_query",
]
```

---

### 11. Agent Wiring (UPDATE agent.py)

**Path**: `nl2sql_agent/agent.py` (UPDATE — add tools)

Replace the existing `nl2sql_agent` definition with tools wired in:

```python
"""ADK agent definitions: root agent and NL2SQL sub-agent.

The root_agent variable is REQUIRED by ADK convention.
ADK discovers it automatically when running `adk run nl2sql_agent`.
"""

import os

from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import setup_logging, get_logger
from nl2sql_agent.clients import LiveBigQueryClient
from nl2sql_agent.tools import (
    init_bq_service,
    vector_search_tables,
    fetch_few_shot_examples,
    load_yaml_metadata,
    dry_run_sql,
    execute_sql,
    save_validated_query,
)

# --- Initialise logging ---
setup_logging()
logger = get_logger(__name__)

# --- Configure LiteLLM environment ---
os.environ["LITELLM_API_KEY"] = settings.litellm_api_key
os.environ["LITELLM_API_BASE"] = settings.litellm_api_base

# --- Initialise tool dependencies ---
bq_client = LiveBigQueryClient(project=settings.gcp_project, location=settings.bq_location)
init_bq_service(bq_client)

# --- Model instances ---
default_model = LiteLlm(model=settings.litellm_model)

# --- NL2SQL Sub-Agent (now with tools!) ---
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
        "Your job is to answer natural language questions about trading data "
        "by generating and executing BigQuery SQL queries.\n\n"
        "## Tool Usage Order (follow this EVERY TIME)\n"
        "1. vector_search_tables — find which table(s) are relevant\n"
        "2. load_yaml_metadata — load column descriptions for those tables\n"
        "3. fetch_few_shot_examples — find similar past validated queries\n"
        "4. Write the SQL using metadata and examples as context\n"
        "5. dry_run_sql — validate the SQL syntax and cost\n"
        "6. execute_sql — run the validated query\n\n"
        "## Critical Rules\n"
        f"- ALWAYS use fully-qualified table names: `{settings.gcp_project}.dataset.table`\n"
        "- ALWAYS filter on trade_date partition column\n"
        "- ALWAYS add LIMIT unless the user explicitly asks for all rows\n"
        "- Use ROUND() for decimal outputs\n"
        "- If dry_run fails, fix the SQL and retry (up to 3 attempts)\n"
        "- If you're unsure which table, ask for clarification\n"
        "- After returning results, ask if the answer was correct\n"
    ),
    tools=[
        vector_search_tables,
        fetch_few_shot_examples,
        load_yaml_metadata,
        dry_run_sql,
        execute_sql,
        save_validated_query,
    ],
)

# --- Root Agent ---
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
    tool_count=len(nl2sql_agent.tools),
)
```

**CRITICAL CHANGES from Track 01**:

1. `LiveBigQueryClient()` is created and passed to `init_bq_service()` before agents are defined. This initialises the shared dependency for all tools.
2. Six tool functions are imported and passed to `tools=[...]`.
3. The `nl2sql_agent` instruction now includes explicit tool usage order and critical SQL rules. The project ID is injected from `settings.gcp_project` (not hardcoded).
4. Tools are plain functions — no `FunctionTool()` wrapping needed.

**DO NOT** import `LiveBigQueryClient` inside tool modules. Only import it in `agent.py`.

**DO NOT** create a separate `services.py`. Use the existing `clients.py` with `LiveBigQueryClient`.

---

## Updated Directory Tree

After Track 03 is complete, the new files (marked with ★) are:

```
nl2sql-agent/
├── .gitignore
├── pyproject.toml
├── Dockerfile
├── docker-compose.yml
├── README.md
│
├── nl2sql_agent/
│   ├── __init__.py
│   ├── agent.py                    ← UPDATED (tools wired in)
│   ├── config.py                   ← UPDATED (new fields)
│   ├── logging_config.py
│   ├── protocols.py                ← UPDATED (add query_with_params)
│   ├── clients.py                  ← UPDATED (add query_with_params impl)
│   ├── catalog_loader.py           (from Track 02)
│   ├── .env
│   ├── .env.example
│   │
│   └── tools/                      ★ NEW directory
│       ├── __init__.py             ★ NEW (exports all tools + init)
│       ├── _deps.py                ★ NEW (shared BQ dependency)
│       ├── vector_search.py        ★ NEW (vector_search_tables, fetch_few_shot_examples)
│       ├── metadata_loader.py      ★ NEW (load_yaml_metadata)
│       ├── sql_validator.py        ★ NEW (dry_run_sql)
│       ├── sql_executor.py         ★ NEW (execute_sql)
│       └── learning_loop.py        ★ NEW (save_validated_query)
│
├── catalog/                        (from Track 02)
│   ├── _routing.yaml
│   ├── kpi/
│   │   ├── _dataset.yaml
│   │   ├── markettrade.yaml
│   │   ├── quotertrade.yaml
│   │   ├── brokertrade.yaml
│   │   ├── clicktrade.yaml
│   │   └── otoswing.yaml
│   └── data/
│       ├── _dataset.yaml
│       ├── theodata.yaml
│       ├── quotertrade.yaml
│       └── ... (other data tables)
│
├── examples/                       (from Track 02)
│   ├── kpi_examples.yaml
│   ├── data_examples.yaml
│   └── routing_examples.yaml
│
├── embeddings/                     (from Track 02)
│
├── setup/                          (from Track 01)
│
├── schemas/                        (from Track 01)
│
├── eval/
│   └── .gitkeep
│
├── scripts/                        (from Track 02)
│
└── tests/
    ├── __init__.py
    ├── conftest.py                 ← UPDATED (mock BQ fixtures)
    ├── test_config.py              (from Track 01)
    ├── test_agent_init.py          ← UPDATED (new tool assertions)
    ├── test_yaml_catalog.py        (from Track 02)
    ├── test_catalog_loader.py      (from Track 02)
    ├── test_vector_search.py       ★ NEW
    ├── test_metadata_loader.py     ★ NEW
    ├── test_sql_validator.py       ★ NEW
    ├── test_sql_executor.py        ★ NEW
    ├── test_learning_loop.py       ★ NEW
    └── test_tool_wiring.py         ★ NEW
```

**DO NOT** create additional directories not listed here.

**DO NOT** put tool tests in a `tests/tools/` subdirectory. Keep all tests flat in `tests/`.

---

## Test Specifications

### Test Configuration Update

**Path**: `tests/conftest.py` (UPDATE — add mock BQ fixtures, PRESERVE existing patterns)

**CRITICAL**: The existing conftest.py sets env vars at MODULE LEVEL (not just in fixtures) because `Settings()` singleton runs at import/collection time before fixtures execute. **DO NOT** remove this pattern. Only ADD the mock BQ fixtures.

The existing module-level env vars and `set_test_env` fixture MUST be preserved exactly as-is. Add the `MockBigQueryService` class and `mock_bq` fixture below the existing code.

```python
# --- EXISTING CODE (DO NOT MODIFY) ---
# Module-level _TEST_ENV dict, os.environ.setdefault loop, and
# set_test_env autouse fixture stay exactly as-is from Track 02.
# ---

# --- NEW imports for Track 03 ---
from typing import Any
import pandas as pd


class MockBigQueryService:
    """Mock BigQuery service implementing BigQueryProtocol for tests.

    Implements all protocol methods: execute_query, dry_run_query,
    get_table_schema, query_with_params.

    Configure expected responses by setting attributes before test assertions.
    """

    def __init__(self):
        self.query_responses: dict[str, list[dict[str, Any]]] = {}
        self.dry_run_responses: dict[str, dict[str, Any]] = {}
        self.last_query: str | None = None
        self.last_params: list | None = None
        self.query_call_count: int = 0
        self._default_query_response: list[dict[str, Any]] = []
        self._default_dry_run_response: dict[str, Any] = {
            "valid": True,
            "total_bytes_processed": 1024 * 1024,
            "error": None,
        }

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Mock execute_query — returns DataFrame (matching existing protocol)."""
        self.last_query = sql
        self.query_call_count += 1

        for keyword, response in self.query_responses.items():
            if keyword.lower() in sql.lower():
                return pd.DataFrame(response)

        return pd.DataFrame(self._default_query_response)

    def query_with_params(self, sql: str, params: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        """Mock query_with_params — returns list[dict] (new method for Track 03)."""
        self.last_query = sql
        self.last_params = params
        self.query_call_count += 1

        for keyword, response in self.query_responses.items():
            if keyword.lower() in sql.lower():
                return response

        return self._default_query_response

    def dry_run_query(self, sql: str) -> dict[str, Any]:
        """Mock dry_run_query — returns validation dict (matching existing protocol)."""
        self.last_query = sql

        for keyword, response in self.dry_run_responses.items():
            if keyword.lower() in sql.lower():
                return response

        return self._default_dry_run_response

    def get_table_schema(self, dataset: str, table: str) -> list[dict]:
        """Mock get_table_schema (matching existing protocol)."""
        return []

    def set_query_response(self, keyword: str, rows: list[dict[str, Any]]) -> None:
        """Set a response for queries containing the given keyword."""
        self.query_responses[keyword] = rows

    def set_dry_run_response(self, keyword: str, response: dict[str, Any]) -> None:
        """Set a dry-run response for queries containing the given keyword."""
        self.dry_run_responses[keyword] = response


@pytest.fixture
def mock_bq():
    """Provide a MockBigQueryService and inject it into tools._deps."""
    mock = MockBigQueryService()

    from nl2sql_agent.tools._deps import init_bq_service
    init_bq_service(mock)

    yield mock

    # Reset to None after test
    import nl2sql_agent.tools._deps as deps
    deps._bq_service = None
```

---

### Test: Vector Search Tools

**Path**: `tests/test_vector_search.py`

```python
"""Tests for vector search tools."""

from nl2sql_agent.tools.vector_search import vector_search_tables, fetch_few_shot_examples


class TestVectorSearchTables:
    def test_returns_results_on_success(self, mock_bq):
        mock_bq.set_query_response("schema_embeddings", [
            {
                "source_type": "table",
                "layer": "kpi",
                "dataset_name": "nl2sql_omx_kpi",
                "table_name": "markettrade",
                "description": "KPI metrics for market trades",
                "distance": 0.1234,
            }
        ])

        result = vector_search_tables("what was the edge on our trade?")

        assert result["status"] == "success"
        assert len(result["results"]) == 1
        assert result["results"][0]["table_name"] == "markettrade"

    def test_passes_question_as_parameter(self, mock_bq):
        vector_search_tables("test question")

        assert mock_bq.last_params is not None
        assert mock_bq.last_params[0]["name"] == "question"
        assert mock_bq.last_params[0]["value"] == "test question"

    def test_uses_retrieval_query_task_type(self, mock_bq):
        vector_search_tables("any question")

        assert "RETRIEVAL_QUERY" in mock_bq.last_query
        assert "RETRIEVAL_DOCUMENT" not in mock_bq.last_query

    def test_uses_cosine_distance(self, mock_bq):
        vector_search_tables("any question")

        assert "COSINE" in mock_bq.last_query

    def test_returns_error_dict_on_exception(self, mock_bq):
        original_query = mock_bq.query
        def exploding_query(*args, **kwargs):
            raise RuntimeError("BQ connection failed")
        mock_bq.query = exploding_query

        result = vector_search_tables("test")

        assert result["status"] == "error"
        assert "BQ connection failed" in result["error_message"]

    def test_sql_contains_embedding_model_reference(self, mock_bq):
        vector_search_tables("any question")

        # Should reference the model from settings (test-project.test_dataset.test_model)
        assert "test_model" in mock_bq.last_query or "embedding_model" in mock_bq.last_query.lower()


class TestFetchFewShotExamples:
    def test_returns_examples_on_success(self, mock_bq):
        mock_bq.set_query_response("query_memory", [
            {
                "past_question": "what was edge yesterday?",
                "sql_query": "SELECT edge_bps FROM ...",
                "tables_used": "markettrade",
                "past_dataset": "nl2sql_omx_kpi",
                "complexity": "simple",
                "routing_signal": "mentions edge",
                "distance": 0.0567,
            }
        ])

        result = fetch_few_shot_examples("what was the edge?")

        assert result["status"] == "success"
        assert len(result["examples"]) == 1
        assert "edge" in result["examples"][0]["past_question"]

    def test_returns_error_dict_on_exception(self, mock_bq):
        def exploding_query(*args, **kwargs):
            raise RuntimeError("timeout")
        mock_bq.query = exploding_query

        result = fetch_few_shot_examples("test")

        assert result["status"] == "error"
```

---

### Test: Metadata Loader

**Path**: `tests/test_metadata_loader.py`

```python
"""Tests for the YAML metadata loader tool."""

from pathlib import Path
from unittest.mock import patch

from nl2sql_agent.tools.metadata_loader import load_yaml_metadata, _resolve_yaml_path


class TestResolveYamlPath:
    def test_resolves_kpi_table_with_dataset(self):
        path = _resolve_yaml_path("markettrade", "nl2sql_omx_kpi")
        assert path == "kpi/markettrade.yaml"

    def test_resolves_data_table_with_dataset(self):
        path = _resolve_yaml_path("theodata", "nl2sql_omx_data")
        assert path == "data/theodata.yaml"

    def test_disambiguates_quotertrade(self):
        kpi_path = _resolve_yaml_path("quotertrade", "nl2sql_omx_kpi")
        data_path = _resolve_yaml_path("quotertrade", "nl2sql_omx_data")
        assert kpi_path == "kpi/quotertrade.yaml"
        assert data_path == "data/quotertrade.yaml"

    def test_returns_none_for_unknown_table(self):
        path = _resolve_yaml_path("nonexistent_table")
        assert path is None


class TestLoadYamlMetadata:
    def test_returns_error_for_unknown_table(self):
        result = load_yaml_metadata("fake_table_xyz")
        assert result["status"] == "error"
        assert "No metadata found" in result["error_message"]

    def test_returns_error_when_file_missing(self):
        """Even if mapping exists, the file might not."""
        with patch(
            "nl2sql_agent.tools.metadata_loader.CATALOG_DIR",
            Path("/tmp/nonexistent_catalog"),
        ):
            result = load_yaml_metadata("markettrade", "nl2sql_omx_kpi")
            assert result["status"] == "error"
            assert "not found" in result["error_message"].lower()

    def test_returns_metadata_string_for_valid_table(self, tmp_path):
        """Test with a real YAML file in a temp directory."""
        # Create a minimal YAML catalog
        kpi_dir = tmp_path / "kpi"
        kpi_dir.mkdir()
        yaml_content = (
            "table:\n"
            "  name: markettrade\n"
            "  dataset: nl2sql_omx_kpi\n"
            "  fqn: '{project}.nl2sql_omx_kpi.markettrade'\n"
            "  layer: kpi\n"
            "  description: KPI metrics for market trades\n"
            "  partition_field: trade_date\n"
            "  columns:\n"
            "    - name: edge_bps\n"
            "      type: FLOAT64\n"
            "      description: Edge in basis points\n"
        )
        (kpi_dir / "markettrade.yaml").write_text(yaml_content)

        with patch("nl2sql_agent.tools.metadata_loader.CATALOG_DIR", tmp_path):
            result = load_yaml_metadata("markettrade", "nl2sql_omx_kpi")

        assert result["status"] == "success"
        assert "edge_bps" in result["metadata"]
        assert isinstance(result["metadata"], str)  # Must be string, not dict

    def test_includes_dataset_context_for_kpi(self, tmp_path):
        """KPI tables should include _dataset.yaml context."""
        kpi_dir = tmp_path / "kpi"
        kpi_dir.mkdir()
        (kpi_dir / "markettrade.yaml").write_text(
            "table:\n  name: markettrade\n  dataset: nl2sql_omx_kpi\n"
            "  fqn: x\n  layer: kpi\n  description: test\n"
            "  partition_field: trade_date\n  columns: []\n"
        )
        (kpi_dir / "_dataset.yaml").write_text(
            "dataset:\n  name: nl2sql_omx_kpi\n  routing:\n"
            "    - patterns: [edge]\n      table: markettrade\n"
        )

        with patch("nl2sql_agent.tools.metadata_loader.CATALOG_DIR", tmp_path):
            result = load_yaml_metadata("markettrade", "nl2sql_omx_kpi")

        assert result["status"] == "success"
        assert "_kpi_dataset_context" in result["metadata"]
```

---

### Test: SQL Validator

**Path**: `tests/test_sql_validator.py`

```python
"""Tests for the SQL dry run validator tool."""

from nl2sql_agent.tools.sql_validator import dry_run_sql


class TestDryRunSql:
    def test_valid_query_returns_valid_status(self, mock_bq):
        result = dry_run_sql("SELECT * FROM my_table")

        assert result["status"] == "valid"
        assert "estimated_mb" in result
        assert result["estimated_mb"] > 0

    def test_invalid_query_returns_error(self, mock_bq):
        mock_bq._default_dry_run_response = {
            "valid": False,
            "total_bytes_processed": 0,
            "error": "Unrecognized name: fake_column",
        }

        result = dry_run_sql("SELECT fake_column FROM my_table")

        assert result["status"] == "invalid"
        assert "fake_column" in result["error_message"]

    def test_passes_sql_to_service(self, mock_bq):
        sql = "SELECT edge_bps FROM `test-project.nl2sql_omx_kpi.markettrade`"
        dry_run_sql(sql)

        assert mock_bq.last_query == sql
```

---

### Test: SQL Executor

**Path**: `tests/test_sql_executor.py`

```python
"""Tests for the SQL executor tool."""

from nl2sql_agent.tools.sql_executor import execute_sql


class TestExecuteSql:
    def test_select_query_succeeds(self, mock_bq):
        mock_bq._default_query_response = [
            {"edge_bps": 5.2, "symbol": "TEST"},
        ]

        result = execute_sql("SELECT edge_bps, symbol FROM my_table")

        assert result["status"] == "success"
        assert result["row_count"] == 1
        assert result["rows"][0]["edge_bps"] == 5.2

    def test_with_cte_query_succeeds(self, mock_bq):
        mock_bq._default_query_response = [{"total": 42}]

        result = execute_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")

        assert result["status"] == "success"

    def test_rejects_insert_query(self, mock_bq):
        result = execute_sql("INSERT INTO my_table VALUES (1, 2)")

        assert result["status"] == "error"
        assert "Only SELECT" in result["error_message"]

    def test_rejects_delete_query(self, mock_bq):
        result = execute_sql("DELETE FROM my_table WHERE id = 1")

        assert result["status"] == "error"

    def test_rejects_drop_query(self, mock_bq):
        result = execute_sql("DROP TABLE my_table")

        assert result["status"] == "error"

    def test_rejects_update_query(self, mock_bq):
        result = execute_sql("UPDATE my_table SET x = 1")

        assert result["status"] == "error"

    def test_adds_limit_when_missing(self, mock_bq):
        execute_sql("SELECT * FROM my_table")

        assert "LIMIT" in mock_bq.last_query

    def test_does_not_add_limit_when_present(self, mock_bq):
        execute_sql("SELECT * FROM my_table LIMIT 10")

        # Should NOT add a second LIMIT
        assert mock_bq.last_query.count("LIMIT") == 1

    def test_returns_truncation_warning(self, mock_bq):
        # Return exactly max_rows to trigger truncation warning
        mock_bq._default_query_response = [{"x": i} for i in range(1000)]

        result = execute_sql("SELECT x FROM my_table")

        assert "warning" in result
        assert "truncated" in result["warning"].lower()

    def test_returns_error_on_exception(self, mock_bq):
        def exploding_query(*args, **kwargs):
            raise RuntimeError("timeout exceeded")
        mock_bq.query = exploding_query

        result = execute_sql("SELECT 1")

        assert result["status"] == "error"
        assert "timeout" in result["error_message"]
```

---

### Test: Learning Loop

**Path**: `tests/test_learning_loop.py`

```python
"""Tests for the learning loop (save validated query) tool."""

from nl2sql_agent.tools.learning_loop import save_validated_query


class TestSaveValidatedQuery:
    def test_successful_save(self, mock_bq):
        result = save_validated_query(
            question="what was edge yesterday?",
            sql_query="SELECT edge_bps FROM ...",
            tables_used="markettrade",
            dataset="nl2sql_omx_kpi",
        )

        assert result["status"] == "success"
        assert "markettrade" in result["message"]

    def test_passes_question_as_parameter(self, mock_bq):
        save_validated_query(
            question="test question",
            sql_query="SELECT 1",
            tables_used="markettrade",
        )

        # Should have been called at least twice (insert + embed)
        assert mock_bq.query_call_count >= 1
        assert mock_bq.last_params is not None or mock_bq.query_call_count >= 2

    def test_insert_failure_returns_error(self, mock_bq):
        call_count = [0]
        def failing_on_first(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("insert failed")
            return []
        mock_bq.query = failing_on_first

        result = save_validated_query(
            question="test",
            sql_query="SELECT 1",
            tables_used="markettrade",
        )

        assert result["status"] == "error"
        assert "insert" in result["error_message"].lower()

    def test_embed_failure_returns_partial_success(self, mock_bq):
        call_count = [0]
        def failing_on_second(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 2:
                raise RuntimeError("embed failed")
            return []
        mock_bq.query = failing_on_second

        result = save_validated_query(
            question="test",
            sql_query="SELECT 1",
            tables_used="markettrade",
        )

        assert result["status"] == "partial_success"

    def test_uses_retrieval_document_for_embedding(self, mock_bq):
        save_validated_query(
            question="test",
            sql_query="SELECT 1",
            tables_used="markettrade",
        )

        # The embed query (second call) should use RETRIEVAL_DOCUMENT
        # We check the last query since embed runs second
        assert mock_bq.query_call_count == 2
```

---

### Test: Tool Wiring

**Path**: `tests/test_tool_wiring.py`

```python
"""Tests that tools are correctly wired into the agent."""


class TestToolWiring:
    def test_nl2sql_agent_has_six_tools(self):
        from nl2sql_agent.agent import nl2sql_agent

        assert nl2sql_agent.tools is not None
        assert len(nl2sql_agent.tools) == 6

    def test_root_agent_still_has_sub_agents(self):
        from nl2sql_agent.agent import root_agent

        assert len(root_agent.sub_agents) == 1
        assert root_agent.sub_agents[0].name == "nl2sql_agent"

    def test_root_agent_has_no_tools(self):
        from nl2sql_agent.agent import root_agent

        assert root_agent.tools is None or len(root_agent.tools) == 0

    def test_all_tools_are_callable(self):
        from nl2sql_agent.agent import nl2sql_agent

        for tool in nl2sql_agent.tools:
            # ADK wraps functions as FunctionTool. The underlying func should be callable.
            assert callable(tool) or hasattr(tool, "func")
```

---

### Running Tests

```bash
# Inside Docker container:
pytest tests/ -v

# Expected NEW test results (in addition to Track 01 + 02 tests):
# tests/test_vector_search.py       — 8 tests PASSED
# tests/test_metadata_loader.py     — 8 tests PASSED
# tests/test_sql_validator.py       — 3 tests PASSED
# tests/test_sql_executor.py        — 10 tests PASSED
# tests/test_learning_loop.py       — 5 tests PASSED
# tests/test_tool_wiring.py         — 4 tests PASSED
#
# 38 new tests passed (Track 03)
```

ALL 38 new tests must pass. Combined with Track 01 and Track 02 tests, the total suite should be green.

---

## Implementation Order

Execute these steps in EXACTLY this order. Do not skip steps. Do not reorder.

### Step 1: Update `nl2sql_agent/protocols.py` (add `query_with_params` method)
Add the `query_with_params()` method to the existing `BigQueryProtocol` as specified in File Spec #1. Do NOT remove or rename existing methods.

### Step 2: Update `nl2sql_agent/config.py`
Add only the NEW fields from File Spec #3 (`bq_query_timeout_seconds`, `bq_max_result_rows`, `vector_search_top_k`). Do NOT add `embedding_model_fqn` or `metadata_dataset` — they already exist as `embedding_model_ref` and `metadata_dataset`.

### Step 3: Update `nl2sql_agent/clients.py` (add `query_with_params` method)
Add the `query_with_params()` method to the existing `LiveBigQueryClient` as specified in File Spec #2. Do NOT create a new `services.py`.

### Step 4: Create `nl2sql_agent/tools/` directory and `_deps.py`
Create `nl2sql_agent/tools/__init__.py` (empty initially) and `_deps.py` from File Spec #4.

### Step 5: Create `nl2sql_agent/tools/vector_search.py`
Copy exactly as specified in File Spec #5.

### Step 6: Create `nl2sql_agent/tools/metadata_loader.py`
Copy exactly as specified in File Spec #6. Verify that the `_TABLE_YAML_MAP` and `_DATASET_TABLE_MAP` entries match the actual YAML files created in Track 02.

### Step 7: Create `nl2sql_agent/tools/sql_validator.py`
Copy exactly as specified in File Spec #7.

### Step 8: Create `nl2sql_agent/tools/sql_executor.py`
Copy exactly as specified in File Spec #8.

### Step 9: Create `nl2sql_agent/tools/learning_loop.py`
Copy exactly as specified in File Spec #9.

### Step 10: Update `nl2sql_agent/tools/__init__.py`
Add the full content from File Spec #10 — imports all tools and `init_bq_service`.

### Step 11: Update `nl2sql_agent/agent.py`
Wire tools in as specified in File Spec #11. Import `LiveBigQueryClient`, call `init_bq_service()`, add 6 tools to `nl2sql_agent`. Update instruction with tool usage order (inject `settings.gcp_project` dynamically, not hardcoded).

### Step 12: Update `tests/conftest.py`
Add `MockBigQueryService` class and `mock_bq` fixture.

### Step 13: Create all test files
Create `test_vector_search.py`, `test_metadata_loader.py`, `test_sql_validator.py`, `test_sql_executor.py`, `test_learning_loop.py`, `test_tool_wiring.py`.

### Step 14: Run tests
```bash
pytest tests/ -v
```
ALL tests must pass (Track 01 + 02 + 03).

### Step 15: Manual integration test
```bash
docker compose run --rm -p 8000:8000 agent adk web --host 0.0.0.0 --port 8000 nl2sql_agent
```

Test in ADK web UI:
1. Ask "what was the edge yesterday?" — should see vector_search_tables called in trace
2. Ask "show me PnL by delta bucket" — should see full tool chain in trace
3. Ask "Hello" — should stay at root agent, no tool calls

---

## Acceptance Criteria

Track 03 is DONE when ALL of the following are true:

- [ ] `nl2sql_agent/protocols.py` updated with `query_with_params` method on `BigQueryProtocol`
- [ ] `nl2sql_agent/clients.py` updated with `query_with_params` implementation on `LiveBigQueryClient`
- [ ] `nl2sql_agent/tools/` directory exists with 7 files (init, deps, 5 tools)
- [ ] `nl2sql_agent/config.py` has `bq_query_timeout_seconds`, `bq_max_result_rows`, `vector_search_top_k` (uses existing `embedding_model_ref`)
- [ ] `nl2sql_agent/agent.py` creates `LiveBigQueryClient` and calls `init_bq_service()`
- [ ] `nl2sql_agent/agent.py` wires 6 tools into `nl2sql_agent.tools=[]`
- [ ] All 6 tools return `dict` with `status` key
- [ ] All 6 tools log inputs and outputs via structlog
- [ ] `execute_sql` rejects non-SELECT queries
- [ ] `execute_sql` auto-adds LIMIT when missing
- [ ] `vector_search_tables` uses `RETRIEVAL_QUERY` task type
- [ ] `save_validated_query` uses `RETRIEVAL_DOCUMENT` task type
- [ ] All SQL in tools uses `settings.*` for dataset/model names (no hardcoded values)
- [ ] No tool module imports `from google.cloud import bigquery`
- [ ] `pytest tests/ -v` passes all tests (Track 01 + 02 + 03)
- [ ] ADK web UI shows tool invocations in trace when asking data questions

---

## Anti-Patterns (DO NOT DO THESE)

| Anti-Pattern | Why It's Wrong | Correct Approach |
|---|---|---|
| Import `bigquery.Client` in tool modules | Violates protocol-based DI, breaks testing | Use `get_bq_service()` from `_deps.py` |
| Use `FunctionTool()` wrapper | Unnecessary — ADK auto-wraps plain functions | Pass functions directly to `tools=[]` |
| Use `@tool` decorator | Doesn't exist in ADK | Just define plain functions |
| Return bare strings from tools | LLM gets less structured feedback | Always return `dict` with `status` key |
| Raise exceptions in tools | LLM can't handle Python exceptions | Return `{"status": "error", "error_message": ...}` |
| Use f-strings for user input in SQL | SQL injection vulnerability | Use BigQuery `@param` query parameters |
| Hardcode dataset names in SQL templates | Breaks when environment changes | Use `settings.metadata_dataset`, `settings.gcp_project` |
| Hardcode project IDs in agent instructions | Breaks in dev vs prod | Use f-string with `settings.gcp_project` |
| Use `RETRIEVAL_DOCUMENT` for search queries | Wrong task type, poor results | Use `RETRIEVAL_QUERY` for search, `RETRIEVAL_DOCUMENT` for stored content |
| Use `WHERE embedding IS NULL` | BQ arrays are never NULL | Use `WHERE ARRAY_LENGTH(embedding) = 0` |
| Use default values on tool parameters | Can break ADK tool schema discovery | Make all tool params required |
| Skip LIMIT in execute_sql | Accidental full table scans | Auto-add LIMIT when not present |
| Use complex types in tool parameters | ADK can't generate schema for Pydantic models | Use `str`, `int`, `float`, `bool`, `list[str]` only |
| Make tools async unnecessarily | Adds complexity, ADK handles sync fine | Use sync functions unless truly needed |
| Create `LiveBigQueryClient()` at module level in `clients.py` | Connects at import time, breaks testing | Create instance only in `agent.py` |
| Create a separate `services.py` | Duplicates existing `clients.py` | Extend `LiveBigQueryClient` in `clients.py` |
| Put tool tests in `tests/tools/` subdirectory | Breaks flat test convention | Keep all tests flat in `tests/` |
| Replace conftest.py module-level env vars | Breaks Settings singleton during test collection | Only ADD to existing conftest, never replace |
| Use `yaml.load()` without SafeLoader | Security vulnerability | Always use `yaml.safe_load()` |
| Use `os.getenv()` for config values | Bypasses settings validation | Use `from nl2sql_agent.config import settings` |
