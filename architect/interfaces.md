# Interface Contracts

> Track-to-track API and tool contracts. Each interface has exactly one owner.
> Consumers implement against this specification. Mismatches are INTERFACE_MISMATCH discoveries.
>
> Updated by: `/architect-decompose` (initial), `/architect-sync` (discovery-driven changes)
> Last synced: 2026-02-19

---

## Architecture Note

The implementation uses a **tools-first architecture** — standalone Python functions with
shared dependency injection via `tools/_deps.py` — rather than the service-class layer
originally envisioned. Each tool is a plain function that returns a `dict`. The BigQuery
client is injected at startup via `init_bq_service()` and accessed via `get_bq_service()`.

SQL generation is handled by the LLM itself (via the agent's system prompt), not by a
dedicated `generate_sql` tool. The LLM uses metadata and few-shot examples to write SQL
directly.

---

## Agent Tools (Python Functions)

### 03_agent_tools: check_semantic_cache

**File:** `nl2sql_agent/tools/semantic_cache.py`

| Function | Signature | Description | Returns |
|----------|-----------|-------------|---------|
| `check_semantic_cache` | `(question: str) -> dict` | Check query_memory for near-exact match via VECTOR_SEARCH | `{cache_hit: bool, cached_sql?, cached_question?, distance?}` |

**Owned by:** Track 05_eval_hardening
**Consumed by:** Agent pipeline (called first, before vector search)

---

### 03_agent_tools: vector_search_tables

**File:** `nl2sql_agent/tools/vector_search.py`

| Function | Signature | Description | Returns |
|----------|-----------|-------------|---------|
| `vector_search_tables` | `(question: str) -> dict` | Semantic search over schema_embeddings to find relevant tables. Also pre-fetches few-shot examples in single BQ round-trip. | `{status, results: [{source_type, layer, dataset_name, table_name, description, distance}]}` |

**Owned by:** Track 03_agent_tools
**Consumed by:** Agent pipeline (called after semantic cache miss)

---

### 03_agent_tools: fetch_few_shot_examples

**File:** `nl2sql_agent/tools/vector_search.py`

| Function | Signature | Description | Returns |
|----------|-----------|-------------|---------|
| `fetch_few_shot_examples` | `(question: str) -> dict` | Retrieve validated Q->SQL pairs from query_memory. Returns from cache if vector_search_tables was already called for this question. | `{status, examples: [{past_question, sql_query, tables_used, complexity, distance}]}` |

**Owned by:** Track 03_agent_tools
**Consumed by:** Agent pipeline (called after vector_search_tables)

---

### 03_agent_tools: load_yaml_metadata

**File:** `nl2sql_agent/tools/metadata_loader.py`

| Function | Signature | Description | Returns |
|----------|-----------|-------------|---------|
| `load_yaml_metadata` | `(table_name: str, dataset_name: str) -> dict` | Load YAML catalog metadata for a table. Includes dataset context for KPI/data tables. | `{status, table_name, dataset_name, metadata: str(yaml)}` |

**Owned by:** Track 03_agent_tools
**Consumed by:** Agent pipeline (called after vector search identifies relevant tables)

---

### 03_agent_tools: dry_run_sql

**File:** `nl2sql_agent/tools/sql_validator.py`

| Function | Signature | Description | Returns |
|----------|-----------|-------------|---------|
| `dry_run_sql` | `(sql_query: str) -> dict` | BigQuery dry-run validation. Checks syntax, column references, permissions, estimates cost. | `{status: "valid"|"invalid", estimated_bytes?, estimated_mb?, error_message?}` |

**Owned by:** Track 03_agent_tools
**Consumed by:** Agent pipeline (called after LLM generates SQL)

---

### 03_agent_tools: execute_sql

**File:** `nl2sql_agent/tools/sql_executor.py`

| Function | Signature | Description | Returns |
|----------|-----------|-------------|---------|
| `execute_sql` | `(sql_query: str) -> dict` | Execute validated BigQuery SQL. Read-only (SELECT/WITH only). Auto-adds LIMIT if missing. | `{status, row_count, rows: [dict], warning?}` |

**Owned by:** Track 03_agent_tools
**Consumed by:** Agent pipeline (called after successful dry run)

---

### 03_agent_tools: save_validated_query

**File:** `nl2sql_agent/tools/learning_loop.py`

| Function | Signature | Description | Returns |
|----------|-----------|-------------|---------|
| `save_validated_query` | `(question: str, sql_query: str, tables_used: str, dataset: str, complexity: str, routing_signal: str) -> dict` | Save validated Q->SQL pair to query_memory with auto-embedding. | `{status, message}` |

**Owned by:** Track 03_agent_tools
**Consumed by:** Agent pipeline (called when trader confirms result)

---

## Dependency Injection

### tools/_deps.py: Shared BigQuery Service

| Function | Signature | Description |
|----------|-----------|-------------|
| `init_bq_service` | `(service: BigQueryProtocol) -> None` | Set shared BQ service at agent startup |
| `get_bq_service` | `() -> BigQueryProtocol` | Get shared BQ service (raises RuntimeError if not initialised) |
| `cache_vector_result` | `(question: str, result: dict) -> None` | Cache combined vector search result |
| `get_cached_vector_result` | `(question: str) -> dict | None` | Return cached result if question matches |

**Owned by:** Track 03_agent_tools
**Consumed by:** All tool modules

---

## Protocols (Abstract Interfaces)

### protocols.py: BigQueryProtocol

**File:** `nl2sql_agent/protocols.py`

| Method | Signature | Description |
|--------|-----------|-------------|
| `execute_query` | `(sql: str) -> pd.DataFrame` | Execute SQL, return DataFrame |
| `dry_run_query` | `(sql: str) -> dict` | Validate SQL without executing |
| `get_table_schema` | `(dataset: str, table: str) -> list[dict]` | Get table schema |
| `query_with_params` | `(sql: str, params: list[dict]) -> list[dict]` | Parameterised query |

**Implemented by:** `LiveBigQueryClient` (production), `FakeBigQueryClient` (tests)
**Consumed by:** All tools via `_deps.get_bq_service()`

### protocols.py: EmbeddingProtocol

| Method | Signature | Description |
|--------|-----------|-------------|
| `embed_text` | `(text: str) -> list[float]` | Single text embedding |
| `embed_batch` | `(texts: list[str]) -> list[list[float]]` | Batch embedding |

**Note:** Not currently used directly by tools — embeddings are generated via BigQuery ML.GENERATE_EMBEDDING SQL.

---

## Shared Data Schemas

All tools return plain `dict` objects. There are no Pydantic response models — tools
return status dicts with consistent patterns:

```python
# Success pattern
{"status": "success", "results": [...], ...}

# Error pattern
{"status": "error", "error_message": "..."}

# Validation pattern
{"status": "valid"|"invalid", ...}

# Cache pattern
{"cache_hit": True|False, ...}
```

---

## Contract Change Protocol

When a tool interface needs to change:

1. Owner proposes change in interfaces.md
2. All consumers listed under the interface are checked:
   - new: auto-inherit via header regeneration
   - in_progress: flag for developer review
   - completed: INTERFACE_MISMATCH discovery -> patch phase if needed
3. Breaking changes require developer approval before applying
