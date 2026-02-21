# Track 13: Autopsy Fixes — Implementation Plan

## Phase 1: Critical Production Bugs

### [x] Task 1.1: Fix ARRAY_LENGTH(NULL) bug in 4 locations `739ee26`

The `WHERE ARRAY_LENGTH(t.embedding) = 0` predicate never matches newly inserted rows because `embedding` defaults to `NULL`, and `ARRAY_LENGTH(NULL)` returns `NULL` in BigQuery. Fix all 4 locations:

**Files:**
- `nl2sql_agent/tools/learning_loop.py:31` — `_EMBED_NEW_ROWS_SQL`
- `scripts/run_embeddings.py:244` — schema_embeddings UPDATE
- `scripts/run_embeddings.py:257` — column_embeddings UPDATE
- `scripts/run_embeddings.py:270` — query_memory UPDATE

**Change:** Replace `WHERE ARRAY_LENGTH(t.embedding) = 0` with `WHERE t.embedding IS NULL OR ARRAY_LENGTH(t.embedding) = 0` in all 4 locations.

**Test:** New `tests/test_embedding_pipeline.py` — assert the SQL templates contain the `IS NULL` predicate. Integration: `run_embeddings.py --step generate-embeddings` actually processes NULL rows.

### [x] Task 1.2: Reset circuit breaker state on new question `766002a`

When `check_semantic_cache` is called (marking a new question), only `tool_call_count` and `tool_call_history` are reset. The `dry_run_attempts` and `max_retries_reached` keys persist, permanently blocking the agent after 3 dry-run failures.

**File:** `nl2sql_agent/callbacks.py:82-84`

**Change:** Add to the `check_semantic_cache` reset block:
```python
tool_context.state["dry_run_attempts"] = 0
tool_context.state["max_retries_reached"] = False
```

**Test:** Extend `tests/test_callbacks.py` — verify that after triggering max retries, a new `check_semantic_cache` call resets the state and allows `dry_run_sql` again.

### [x] Task 1.3: Fix incorrect column names in example YAML files `0358a72`

Multiple wrong column names in few-shot examples poison the LLM's SQL generation:

| Wrong | Correct | Files |
|-------|---------|-------|
| `edge` (on non-markettrade tables) | `instant_edge` | `kpi_examples.yaml` |
| `vol_slippage_per_unit_10m` | `vol_slippage_10m_per_unit` | `kpi_examples.yaml` |
| `delta_slippage_fired_at_1h` | `delta_slippage_1h_fired_at` | `kpi_examples.yaml` |
| `bid_size_0` / `ask_size_0` | `bid_volume_0` / `ask_volume_0` | `data_examples.yaml` |
| `putcall` | `option_type_name` | `data_examples.yaml` |

**Files:** `examples/kpi_examples.yaml`, `examples/data_examples.yaml`

**Approach:** Audit each example SQL against the YAML catalog column definitions. Fix all wrong column names systematically.

**Test:** New `tests/test_example_column_validation.py` — parse all example YAML files, extract column names from SQL, validate each against the corresponding catalog YAML.

### [x] Task 1.4: Fix PnL double-counting routing rule `fd985f5`

The "total PnL across all trade types" example UNION ALLs markettrade with quotertrade/clicktrade/otoswing. But markettrade already includes all Mako trades (it's the superset). This double-counts PnL.

**Files:**
- `examples/kpi_examples.yaml:237-264` — fix or remove the UNION ALL example
- `catalog/kpi/_dataset.yaml:185-187` — fix routing rule that directs "total PnL" to all tables
- `nl2sql_agent/prompts.py` — update any routing guidance that references this pattern

**Change:** The "total PnL" example should query markettrade only (which already contains all trade types). The routing rule should direct aggregate PnL questions to markettrade, not a UNION of all tables.

### [x] Task 1.5: Add timeout to execute_query() `08b804d`

`clients.py:42-47` has no timeout on `self._client.query(sql).to_dataframe()`. The agent can hang indefinitely on expensive queries.

**File:** `nl2sql_agent/clients.py:42-47`

**Change:** Use BigQuery's `QueryJobConfig(job_timeout_ms=...)` or `job.result(timeout=...)`:
```python
def execute_query(self, sql: str) -> pd.DataFrame:
    job = self._client.query(sql)
    results = job.result(timeout=settings.bq_query_timeout_seconds)
    return results.to_dataframe()
```

**Test:** Add test in `tests/test_clients.py` or `tests/test_sql_executor.py` verifying timeout parameter is passed.

### [x] Task 1.6: Fix LIMIT detection regex `9ad69aa`

`sql_executor.py:48` uses `if "LIMIT" not in upper` which is a naive string check. It matches `LIMIT` inside column names, string literals, or comments, and misses `LIMIT` in subqueries vs. the outer query.

**File:** `nl2sql_agent/tools/sql_executor.py:46-50`

**Change:** Use a regex that matches `LIMIT` as a standalone keyword at the end of the query (after the final closing paren or the last clause):
```python
import re
has_outer_limit = bool(re.search(r'\bLIMIT\s+\d+\s*$', upper))
```

This won't catch every edge case but is significantly better than substring matching, and handles the common cases (LIMIT in subqueries, LIMIT-like column names).

**Test:** Add tests for: LIMIT in subquery only (should add outer LIMIT), LIMIT at end (should not add), column named `LIMIT_VALUE` (should not trigger false match).

### [x] Task 1.7: Fix vector search CTE missing columns `7cf6c70`

The combined VECTOR_SEARCH in `vector_search.py:61-77` maps example_results columns to match schema_results using aliases (`base.dataset AS dataset_name`, `base.question AS table_name`, `base.sql_query AS description`). This loses the actual example metadata (complexity, routing_signal, tables_used).

**File:** `nl2sql_agent/tools/vector_search.py:61-77`

**Change:** Add the missing columns to `example_results` CTE so they're available when the results are parsed. Options:
1. Add `base.complexity`, `base.routing_signal`, `base.tables_used` to the CTE and use separate column names in the UNION ALL (requires matching column count)
2. Or: remove the UNION ALL pattern and return two separate result sets

The current code already separates results by `search_type` when parsing, so adding the missing columns to the shared UNION ALL schema (with NULLs for schema_results) is the cleanest approach.

### [x] Task 1.8: Fix embedding pipeline destructive CREATE OR REPLACE `2348aee`

`run_embeddings.py:58-106` uses `CREATE OR REPLACE TABLE` for all 3 embedding tables. Running `--step create-tables` in an existing environment destroys all data.

**File:** `scripts/run_embeddings.py:58-106`

**Change:** Replace `CREATE OR REPLACE TABLE` with `CREATE TABLE IF NOT EXISTS`. Add a `--force` flag that uses `CREATE OR REPLACE` when explicitly requested. Update the docstring/comments.

```python
def create_embedding_tables(bq: BigQueryProtocol, s: Settings, force: bool = False) -> None:
    create_stmt = "CREATE OR REPLACE TABLE" if force else "CREATE TABLE IF NOT EXISTS"
    ...
```

---

## Phase 2: Structural Improvements

### [x] Task 2.1: Extract shared SQL guard module `3fbfe33`

The DML/DDL check is duplicated in `callbacks.py:52-65` and `sql_executor.py:33-35`, both using a first-keyword heuristic that misses `WITH ... INSERT INTO ...`.

**Create:** `nl2sql_agent/sql_guard.py`

```python
def contains_dml(sql: str) -> tuple[bool, str]:
    """Check if SQL contains DML/DDL keywords anywhere in the body.

    Returns (is_blocked, reason).
    Scans full body for INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/MERGE/CREATE.
    Also rejects multiple statements (semicolons).
    """
```

**Modify:**
- `nl2sql_agent/callbacks.py:52-65` — replace first-keyword check with `contains_dml()`
- `nl2sql_agent/tools/sql_executor.py:33-35` — replace first-keyword check with `contains_dml()`

**Create:** `tests/test_sql_guard.py` with cases:
- `SELECT * FROM t` → allowed
- `WITH cte AS (...) SELECT ...` → allowed
- `INSERT INTO t SELECT ...` → blocked
- `WITH cte AS (...) INSERT INTO t ...` → blocked
- `SELECT 1; DROP TABLE t` → blocked (semicolon)
- `DELETE FROM t WHERE ...` → blocked

### [x] Task 2.2: Auto-discover YAML catalog mappings `310a481`

`metadata_loader.py:21-30` has a hardcoded `_TABLE_YAML_MAP` that must be updated manually when new tables are added. The `_dataset_to_layer()` function handles some cases dynamically, but unique tables require the map.

**File:** `nl2sql_agent/tools/metadata_loader.py:18-30`

**Change:** Replace the hardcoded map with auto-discovery:
```python
def _discover_table_yaml_map() -> dict[str, str]:
    """Scan catalog/{kpi,data}/*.yaml, read table.name, build map."""
    table_map = {}
    for layer in ("kpi", "data"):
        layer_dir = CATALOG_DIR / layer
        if not layer_dir.exists():
            continue
        for yaml_file in sorted(layer_dir.glob("*.yaml")):
            if yaml_file.name.startswith("_"):
                continue  # Skip _dataset.yaml descriptors
            content = load_yaml(yaml_file)
            table_name = content.get("table", {}).get("name", yaml_file.stem)
            table_map[table_name] = f"{layer}/{yaml_file.name}"
    return table_map

_TABLE_YAML_MAP = _discover_table_yaml_map()
```

**Test:** Extend `tests/test_metadata_loader.py` — mock the catalog directory structure and verify auto-discovery finds all tables.

### [x] Task 2.3: Fix Dockerfile and container security `156e6a6`

Multiple Docker issues from the autopsy:
- Dockerfile runs `pip install -e .` before source is copied (editable install fails)
- Container runs as root
- No `.dockerignore` (copies `.git`, `__pycache__`, `.env` into image)

**Files:**
- `Dockerfile` — fix build order, add non-root user
- `docker-compose.yml` — mount ADC credentials properly
- New `.dockerignore`

**Changes:**
1. Reorder Dockerfile: COPY requirements first → install deps → COPY source → install package
2. Add `RUN useradd -m agent` + `USER agent`
3. Create `.dockerignore` with: `.git`, `__pycache__`, `*.pyc`, `.env*`, `conductor/`, `architect/`, `.autopsy/`, `tests/`, `*.egg-info`
4. Fix `docker-compose.yml` ADC volume mount path

**Test:** `docker build .` succeeds. Container runs as non-root (`docker run --rm <image> id` shows non-root UID).

### [x] Task 2.4: Defer module-level BQ client initialization `9a3f23c`

`agent.py:35-38` creates `LiveBigQueryClient` at import time, crashing any import without GCP credentials.

**File:** `nl2sql_agent/agent.py:34-38`

**Change:** Use a lazy initialization pattern:
```python
_bq_client = None

def _get_bq_client():
    global _bq_client
    if _bq_client is None:
        _bq_client = LiveBigQueryClient(
            project=settings.gcp_project, location=settings.bq_location
        )
        init_bq_service(_bq_client)
    return _bq_client
```

Wire the lazy init into the agent setup so it runs on first tool call, not import. This may require moving `init_bq_service` into a callback or using ADK's lifecycle hooks.

**Test:** Verify `from nl2sql_agent.agent import root_agent` works without GCP credentials (mock-only environment).

---

## Phase 3: Strategic Migrations

### [x] Task 3.1: Fix online eval with InMemoryRunner `f07818c`

`eval/run_eval.py` calls `nl2sql_agent.run()` which doesn't exist on ADK `LlmAgent`. The online eval mode is completely broken.

**File:** `eval/run_eval.py` (online eval section, ~lines 307-424)

**Change:** Replace with ADK's `InMemoryRunner`:
```python
from google.adk.runners import InMemoryRunner
from google.adk.sessions import InMemorySessionService

runner = InMemoryRunner(agent=root_agent, app_name="nl2sql_eval")
```

Use `runner.run_async()` with proper session management. Extract generated SQL from tool call events using the after_tool_callback pattern or by inspecting session history.

**Test:** Unit test that online eval can initialize and run a single question (with mocked BQ).

### [x] Task 3.2: Expand ADK built-in evaluation `46b2d48`

Create ADK evaluation test files for hallucination detection, safety, and multi-turn conversations.

**Create:**
- `eval/adk/hallucination_eval.test.json` — questions about nonexistent tables/columns
- `eval/adk/safety_eval.test.json` — DML injection attempts, prompt injection
- `eval/adk/multi_turn_eval.test.json` — follow-up questions, refinements

**Format:** ADK eval JSON format with `input`, `expected_tool_calls`, and `expected_output` fields.

### [x] Task 3.3: Migrate to autonomous embedding generation `864de8a`

Replace the manual INSERT→UPDATE embedding pattern with BigQuery's `GENERATED ALWAYS AS ... STORED` columns.

**Create:** `scripts/migrate_autonomous_embeddings.sql`

```sql
ALTER TABLE `{project}.{dataset}.query_memory`
ADD COLUMN IF NOT EXISTS embedding_auto ARRAY<FLOAT64>
GENERATED ALWAYS AS (
  AI.EMBED(
    MODEL `{embedding_model}`,
    question,
    STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output)
  ).ml_generate_embedding_result
) STORED OPTIONS(asynchronous = TRUE);
```

**Modify:**
- `nl2sql_agent/tools/learning_loop.py` — remove the `_EMBED_NEW_ROWS_SQL` UPDATE step (embedding now auto-generated)
- `scripts/run_embeddings.py` — update `generate_embeddings()` to skip tables with autonomous embedding columns

**Note:** This is a BQ schema migration. Test in dev first. The `asynchronous = TRUE` option means embeddings are generated in the background, not blocking the INSERT.

---

## Summary

| Phase | Tasks | Key Outcome |
|-------|-------|-------------|
| 1 | 1.1–1.8 | Critical bugs fixed, agent functional |
| 2 | 2.1–2.4 | Hardened guard, auto-discovery, clean Docker, safe imports |
| 3 | 3.1–3.3 | Online eval works, expanded eval coverage, autonomous embeddings |

## Verification

```bash
# Phase 1: All tests pass + new tests
pytest tests/ -v

# Phase 1: Embedding pipeline processes NULL rows (integration)
python scripts/run_embeddings.py --step generate-embeddings

# Phase 2: Docker builds
docker build .

# Phase 2: Agent importable without credentials
python -c "from nl2sql_agent.agent import root_agent; print('OK')"

# Phase 2: SQL guard blocks WITH...INSERT
python -c "from nl2sql_agent.sql_guard import contains_dml; print(contains_dml('WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x'))"

# Phase 3: Online eval runs
python eval/run_eval.py --mode online --limit 1

# Phase 3: ADK eval
adk eval eval/adk/
```
