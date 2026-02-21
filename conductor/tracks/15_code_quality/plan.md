# Track 15: Code Quality & Reliability — Implementation Plan

## Phase 1: Exchange-Aware Semantic Cache

### [x] Task 1.1: Write exchange-aware cache tests (TDD red) `b408d72`

The semantic cache currently returns cached SQL regardless of exchange context. After Track 14, a cached OMX query could be returned for a Brazil question.

**Create:** `tests/test_cache_exchange.py`

Tests:
- Cache hit where `cached_dataset` matches resolved exchange datasets → still a hit
- Cache hit where `cached_dataset` is an OMX dataset but resolved exchange is Brazil → treated as miss
- Cache hit with no exchange context (no `resolve_exchange` called) → still a hit (backward compatible)
- Cache miss → unchanged behavior

**Run:** `pytest tests/test_cache_exchange.py -v` — expect failures.

### [x] Task 1.2: Add exchange-aware validation to semantic cache (TDD green) `711b3b8`

**File:** `nl2sql_agent/tools/semantic_cache.py`

**Change:** After finding a cache hit (distance <= threshold), check if the `cached_dataset` is compatible with the current exchange context. The exchange context is available in `ToolContext.state` (set by `resolve_exchange` in the same session).

Since `check_semantic_cache` is a plain function (not receiving ToolContext), the exchange validation needs to happen either:
- (a) In the `after_tool_callback` in `callbacks.py` — inspect the cache result and override if exchange mismatch
- (b) By passing exchange context as an optional parameter to `check_semantic_cache`

**Chosen approach (b):** Add an optional `exchange_datasets` parameter. The agent prompt already tells the LLM to call `resolve_exchange` first when exchange context is detected. For questions without exchange context, the param is omitted and cache behaves as before.

```python
def check_semantic_cache(question: str, exchange_datasets: str = "") -> dict:
    """...
    Args:
        question: The trader's natural language question.
        exchange_datasets: Optional comma-separated dataset names from resolve_exchange.
            If provided, cache hits with a different dataset are treated as misses.
    """
```

Post-hit validation:
```python
if exchange_datasets and best.get("cached_dataset"):
    allowed = {d.strip() for d in exchange_datasets.split(",")}
    if best["cached_dataset"] not in allowed:
        return {"cache_hit": False, "reason": "cached result is for a different exchange"}
```

**Run:** `pytest tests/test_cache_exchange.py tests/test_semantic_cache.py -v` — expect all green.

### [x] Task 1.3: Update prompt to pass exchange datasets to cache `fd59608`

**File:** `nl2sql_agent/prompts.py`

**Change:** In the cache check instructions, note that if the user's question mentions a specific exchange or symbol, the LLM should call `resolve_exchange` FIRST, then pass the returned dataset names to `check_semantic_cache` via the `exchange_datasets` parameter.

Update tool usage order:
```
Step 0: resolve_exchange (if exchange/symbol detected)
Step 0.5: check_semantic_cache(question, exchange_datasets=<from step 0>)
```

For questions without exchange context, the existing flow is unchanged:
```
Step 0: check_semantic_cache(question)
```

**Test:** Update `tests/test_prompts.py` to verify exchange_datasets parameter is mentioned.

---

## Phase 2: TypedDict Return Contracts [checkpoint: aa3e632]

### [x] Task 2.1: Define TypedDict types for all tool returns `4d1f106`

**Create:** `nl2sql_agent/types.py`

Define TypedDicts for each tool's return type:

```python
from typing import TypedDict

class CacheHitResult(TypedDict):
    cache_hit: bool  # True
    cached_sql: str
    cached_question: str
    cached_dataset: str
    tables_used: list[str]
    distance: float

class CacheMissResult(TypedDict):
    cache_hit: bool  # False
    reason: str

class ExchangeResult(TypedDict):
    status: str
    exchange: str
    kpi_dataset: str
    data_dataset: str

class ColumnSearchResult(TypedDict):
    status: str
    tables: list[dict]
    examples: list[dict]

class FewShotResult(TypedDict):
    status: str
    examples: list[dict]

class MetadataResult(TypedDict):
    status: str
    metadata: str  # or dict

class DryRunResult(TypedDict):
    status: str
    valid: bool
    bytes_processed: int
    error: str | None

class ExecuteResult(TypedDict):
    status: str
    row_count: int
    columns: list[str]
    data: list[dict]

class SaveQueryResult(TypedDict):
    status: str
    message: str
```

**Note:** Some tools have multiple return shapes (success vs error). Use `Union` types or a base `ErrorResult` TypedDict.

### [x] Task 2.2: Write type contract tests (TDD red) `25d5360`

**Create:** `tests/test_types.py`

Tests:
- Each tool's success return matches its TypedDict (required keys present, correct types)
- Error returns have `status: "error"` and `error_message` key
- Import all TypedDict types successfully

**Run:** `pytest tests/test_types.py -v` — expect failures.

### [x] Task 2.3: Annotate all tools with TypedDict returns (TDD green) `25d5360`

**Files:** All 8 tool files in `nl2sql_agent/tools/`

**Change:** Update return type annotations from `-> dict` to the specific TypedDict union. Ensure all return paths match the declared type.

Example for `semantic_cache.py`:
```python
from nl2sql_agent.types import CacheHitResult, CacheMissResult

def check_semantic_cache(question: str) -> CacheHitResult | CacheMissResult:
```

**Run:** `pytest tests/ -v` — expect all green.

---

## Phase 3: Prompt Optimization & Developer Tooling

### [x] Task 3.1: Split prompt into static and dynamic sections `ed6baec`

**File:** `nl2sql_agent/prompts.py`

**Change:** Extract the static parts of `build_nl2sql_instruction()` into a cached function:

```python
@functools.lru_cache(maxsize=1)
def _static_instruction() -> str:
    """Tool descriptions, routing rules, SQL guidelines — never change per-turn."""
    ...

def build_nl2sql_instruction(context: ReadonlyContext) -> str:
    """Combine static instruction with dynamic session state."""
    static = _static_instruction()
    dynamic = _build_dynamic_section(context)
    return f"{static}\n\n{dynamic}"
```

The exchange list from the registry IS static (loaded once from YAML) and can be cached. The dynamic section includes: session history summary, resolved exchange for current question, any context from prior tool calls.

**Test:** Verify `build_nl2sql_instruction` returns consistent static sections across calls; verify dynamic section changes with context.

### [ ] Task 3.2: Review and document column_search_top_k

**File:** `nl2sql_agent/config.py`

**Change:** The current `column_search_top_k=30` retrieves 30 column embeddings per search. With 4,631 columns across ~12 tables, this means ~0.6% of columns are returned — reasonable for precision but may over-retrieve for simple questions.

Add a comment documenting the tuning rationale:
```python
column_search_top_k: int = 30  # ~0.6% of 4,631 columns; reduces to ~3-5 tables after aggregation
```

No default change needed — the value is defensible. The main improvement is documentation.

### [ ] Task 3.3: Create .env.example template

**Create:** `.env.example`

```bash
# NL2SQL Agent Configuration
# Copy to .env and fill in values

# --- Required ---
GCP_PROJECT=your-gcp-project-id
LITELLM_API_KEY=your-litellm-api-key
LITELLM_API_BASE=http://localhost:4000

# --- Optional (with defaults) ---
# BQ_LOCATION=US
# METADATA_DATASET=nl2sql_metadata
# KPI_DATASET=nl2sql_omx_kpi
# DATA_DATASET=nl2sql_omx_data
# EMBEDDING_MODEL_REF=your-project.nl2sql_metadata.text_embedding_004
# LITELLM_MODEL=openai/gemini-3-flash-preview
# VECTOR_SEARCH_TOP_K=5
# COLUMN_SEARCH_TOP_K=30
# SEMANTIC_CACHE_THRESHOLD=0.10
# BQ_MAX_RESULT_ROWS=1000
# BQ_QUERY_TIMEOUT_SECONDS=30
```

**Test:** Verify file exists and contains all Settings fields from `config.py`.

---

## Phase 4: Code Hygiene

### [ ] Task 4.1: Remove dead get_table_schema method

`get_table_schema()` is defined in `BigQueryProtocol`, `LiveBigQueryClient`, `FakeBigQueryClient`, and `MockBigQueryService` but is never called by any tool.

**Files:**
- `nl2sql_agent/protocols.py` — remove from `BigQueryProtocol`
- `nl2sql_agent/clients.py:66-78` — remove from `LiveBigQueryClient`
- `tests/fakes.py` — remove from `FakeBigQueryClient`
- `tests/conftest.py` — remove from `MockBigQueryService`
- `tests/test_protocols.py` — remove any `get_table_schema` tests

**Test:** `pytest tests/ -v` — all existing tests still pass (no test should be calling the removed method from production code paths).

### [ ] Task 4.2: Document thread safety constraints in _deps.py

**File:** `nl2sql_agent/tools/_deps.py`

**Change:** Add module docstring update:

```python
"""Shared dependency injection for all tool modules.

THREAD SAFETY: This module uses mutable module-level globals
(_bq_service, _vector_cache_*). It is NOT thread-safe.
Current usage (single ADK session per process) is safe.
If concurrent request handling is needed (e.g., async MCP server),
these globals must be replaced with thread-local storage or
a per-request context object.
"""
```

No code change — documentation only. This prevents future contributors from accidentally introducing concurrency without addressing the globals.

### [ ] Task 4.3: Consolidate error handling patterns

**Files:** All tool files in `nl2sql_agent/tools/`

**Observation:** Most tools follow the same error handling pattern:
```python
try:
    ...
except Exception as e:
    logger.error("tool_name_error", error=str(e))
    return {"status": "error", "error_message": str(e)}
```

This is duplicated 8+ times. Extract a helper:

```python
# In nl2sql_agent/tools/_deps.py or a new _utils.py
def tool_error_result(tool_name: str, error: Exception) -> dict:
    logger.error(f"{tool_name}_error", error=str(error))
    return {"status": "error", "error_message": str(error)}
```

**Note:** This is a LOW priority cleanup. Only apply if it meaningfully reduces duplication without obscuring the error context. Skip if it feels like over-abstraction.

---

## Summary

| Phase | Tasks | Key Outcome |
|-------|-------|-------------|
| 1 | 1.1–1.3 | Semantic cache respects exchange context |
| 2 | 2.1–2.3 | All tools have TypedDict return contracts |
| 3 | 3.1–3.3 | Prompt caching, documented config, .env.example |
| 4 | 4.1–4.3 | Dead code removed, thread safety documented, error consolidation |

## Verification

```bash
# All tests pass
pytest tests/ -v

# Type checking (if mypy configured)
mypy nl2sql_agent/types.py

# .env.example exists
test -f .env.example && echo "OK"

# Prompt caching works (static section same across calls)
python -c "
from nl2sql_agent.prompts import _static_instruction
a = _static_instruction()
b = _static_instruction()
assert a is b, 'Static section should be cached (same object)'
print('Prompt caching OK')
"
```
