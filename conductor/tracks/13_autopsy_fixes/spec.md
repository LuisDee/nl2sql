# Track 13: Autopsy Fixes

## Problem

The autopsy multi-agent deep review (2026-02-20) found 68 unique issues across 89 files — 10 critical and 20 high severity. Several of these directly break core agent functionality:

1. **Embedding pipeline is a no-op**: `ARRAY_LENGTH(NULL)` returns NULL in BigQuery, so the `WHERE ARRAY_LENGTH(t.embedding) = 0` predicate never matches newly inserted rows. This affects 4 locations across `learning_loop.py` and `run_embeddings.py`. The learning loop and all three embedding tables (schema, column, query_memory) silently fail to generate embeddings.

2. **Agent permanently breaks after 3 dry-run failures**: The circuit breaker state (`dry_run_attempts`, `max_retries_reached`) is not reset when a new question begins. Once triggered, all subsequent questions in the session are blocked.

3. **Few-shot examples have wrong column names**: `edge` instead of `instant_edge`, `bid_size_0` instead of `bid_volume_0`, `putcall` instead of `option_type_name`, and several slippage column name inversions. The LLM copies these into generated SQL, producing queries that fail at execution.

4. **PnL routing double-counts**: The "total PnL" example UNION ALLs markettrade with Mako-specific tables (quotertrade, clicktrade, otoswing), but markettrade already includes all Mako trades. The routing rule in `_dataset.yaml` reinforces this error.

5. **No query timeout**: `execute_query()` in `clients.py` has no timeout — the agent can hang indefinitely on expensive queries.

6. **SQL guard bypassable**: The read-only check uses a first-keyword heuristic. `WITH cte AS (...) INSERT INTO ...` passes the guard because the first keyword is `WITH`.

7. **Online eval broken**: `eval/run_eval.py` calls `nl2sql_agent.run()` which does not exist on ADK `LlmAgent`.

8. **Module-level BQ init**: `agent.py` creates a `LiveBigQueryClient` at import time, crashing any import without GCP credentials (breaks testing, Docker builds, IDE indexing).

Additionally, the architecture report identified strategic improvements: auto-discovery of YAML catalog mappings (eliminating hardcoded table maps), Docker container fixes, and migration to autonomous BQ embeddings.

## Solution

A 3-phase track that fixes critical production bugs first, then addresses structural issues, and finally tackles strategic migrations:

- **Phase 1 (Critical Bugs)**: Fix the 8 most impactful bugs that directly break agent functionality
- **Phase 2 (Structural)**: Harden the SQL guard, auto-discover YAML mappings, fix Docker, defer BQ init
- **Phase 3 (Strategic)**: Fix online eval, expand ADK eval, migrate to autonomous embeddings

## Scope

### In Scope
- All 10 critical findings from REVIEW_REPORT.md (except #1 API key rotation — handled separately)
- High-severity findings: SQL guard bypass, LIMIT detection, vector search CTE, embedding pipeline destructive DDL, Docker fixes
- Architecture report recommendations: SQL guard hardening, YAML auto-discovery, lazy BQ init, eval fixes, autonomous embeddings
- New tests for each fix
- New `sql_guard.py` module for shared DML detection

### Out of Scope
- API key rotation and git history rewrite (Critical #1 — separate operational task)
- KPI YAML deduplication (medium priority, large scope — separate track)
- Pre-commit secret scanning setup (operational)
- Dependency lock file / requirements pinning (separate track)
- Misplaced integration test reorganization (low priority)

## Key Design Decisions

### 1. Shared SQL guard module
Extract DML detection into `nl2sql_agent/sql_guard.py` used by both `callbacks.py` and `sql_executor.py`. Full-body keyword scan (not just first keyword) + semicolon rejection + BQ dry-run `statement_type` check.

### 2. YAML auto-discovery
Scan `catalog/{kpi,data}/*.yaml` (skip `_*.yaml` dataset descriptors), read `table.name`/`table.dataset` from each file, build maps dynamically. Replaces hardcoded `_TABLE_YAML_MAP` and `_DATASET_TABLE_MAP`.

### 3. Online eval via InMemoryRunner
Use ADK's `InMemoryRunner.run_async()` with proper session management. Extract SQL from events via `after_tool_callback` pattern instead of calling nonexistent `.run()` method.

### 4. Autonomous embeddings
Two-step: first fix the NULL predicate bug (Phase 1), then migrate to `AI.EMBED(...) GENERATED ALWAYS AS ... STORED OPTIONS(asynchronous = TRUE)` (Phase 3) to eliminate the manual INSERT+UPDATE pattern.

### 5. Lazy agent init
Use a factory function or `__getattr__` module hook to defer `LiveBigQueryClient` creation until first access, allowing imports without BQ credentials.

## Files Modified

| File | Phase | Change |
|------|-------|--------|
| `nl2sql_agent/tools/learning_loop.py` | 1, 3 | ARRAY_LENGTH fix; Phase 3 embedding removal |
| `scripts/run_embeddings.py` | 1 | ARRAY_LENGTH fix + CREATE IF NOT EXISTS + --force flag |
| `nl2sql_agent/callbacks.py` | 1, 2 | Circuit breaker reset; SQL guard extraction |
| `examples/kpi_examples.yaml` | 1 | Column name corrections |
| `examples/data_examples.yaml` | 1 | Column name corrections |
| `catalog/kpi/_dataset.yaml` | 1 | PnL routing rule fix |
| `nl2sql_agent/prompts.py` | 1 | Routing rule update |
| `nl2sql_agent/clients.py` | 1 | execute_query timeout |
| `nl2sql_agent/tools/sql_executor.py` | 1, 2 | LIMIT regex fix; SQL guard |
| `nl2sql_agent/tools/vector_search.py` | 1 | CTE missing columns fix |
| `nl2sql_agent/tools/metadata_loader.py` | 2 | Auto-discovery |
| `nl2sql_agent/agent.py` | 2 | Lazy init |
| `Dockerfile` | 2 | Build fix + non-root user |
| `docker-compose.yml` | 2 | ADC mount fix |
| `eval/run_eval.py` | 3 | InMemoryRunner rewrite |

## Files Created

| File | Phase | Purpose |
|------|-------|---------|
| `nl2sql_agent/sql_guard.py` | 2 | Shared DML detection |
| `.dockerignore` | 2 | Docker build exclusions |
| `tests/test_sql_guard.py` | 2 | SQL guard tests |
| `tests/test_example_column_validation.py` | 1 | Validate example columns against catalog |
| `tests/test_embedding_pipeline.py` | 1 | Embedding NULL predicate tests |
| `eval/adk/hallucination_eval.test.json` | 3 | ADK built-in eval |
| `eval/adk/safety_eval.test.json` | 3 | ADK built-in eval |
| `eval/adk/multi_turn_eval.test.json` | 3 | ADK built-in eval |
| `scripts/migrate_autonomous_embeddings.sql` | 3 | BQ migration DDL |

## Acceptance Criteria

1. All existing tests pass (currently 324+)
2. `run_embeddings.py --step generate-embeddings` actually generates embeddings for newly inserted rows (NULL embedding)
3. Circuit breaker resets between questions — 3 failures on Q1 do not block Q2
4. All column names in `kpi_examples.yaml` and `data_examples.yaml` match actual BQ schema
5. `WITH ... INSERT INTO` is blocked by the SQL guard
6. `execute_query()` times out after configurable duration (default 120s)
7. Agent module importable without GCP credentials
8. `docker build .` succeeds and container runs as non-root
9. `eval/run_eval.py --mode online` executes without `AttributeError`
10. ~20+ new tests covering all fixes

## Dependencies

- Track 12 (column semantic search) — complete
- Track 09 (production hardening) — complete

## Risks

| Risk | Mitigation |
|------|------------|
| Example column fixes may not cover all wrong names | Task 1.3 includes systematic audit against YAML catalog |
| SQL guard changes may block legitimate CTEs | Only block statements containing DML keywords after CTE; BQ dry-run provides second check |
| Lazy init may break existing test fixtures | Tests already mock BQ client; factory pattern compatible with existing mocks |
| Autonomous embeddings require BQ schema migration | Phase 3 runs last; migration SQL tested in dev before prod |

## Sources

- `.autopsy/REVIEW_REPORT.md` — 68 deduplicated code review findings
- `.autopsy/ARCHITECTURE_REPORT.md` — 13 architecture recommendations
