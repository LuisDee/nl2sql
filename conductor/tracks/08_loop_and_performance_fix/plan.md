# Track 08: Implementation Plan

## Status: COMPLETE

## Phase 1: Combined Vector Search
- [x] Add vector cache to `_deps.py` (cache_vector_result, get_cached_vector_result, clear_vector_cache)
- [x] Combined CTE SQL in `vector_search.py` (single ML.GENERATE_EMBEDDING)
- [x] `vector_search_tables()` runs combined query, caches examples
- [x] `fetch_few_shot_examples()` checks cache first, falls back to independent query
- [x] Fallback to schema-only search if combined query fails
- [x] Tests: `tests/test_combined_vector_search.py`
- [x] Updated: `tests/test_vector_search.py`

## Phase 2: Hard Circuit Breaker
- [x] `before_tool_guard` blocks dry_run_sql/execute_sql when `max_retries_reached=True`
- [x] Global tool call counter with `max_tool_calls_per_turn` config
- [x] Tests: `tests/test_circuit_breaker.py`
- [x] Updated: `tests/test_callbacks.py` (proper state dict in TestBeforeToolGuard)

## Phase 3: YAML Caching
- [x] `@lru_cache(maxsize=50)` on `load_yaml()`
- [x] `clear_yaml_cache()` helper
- [x] Tests: `tests/test_yaml_cache.py`

## Phase 4: Prompt & Threshold Tuning
- [x] Trim follow-up SQL to 500 chars in `prompts.py`
- [x] Reduce preview from 5 to 3 rows in `callbacks.py`
- [x] `semantic_cache_threshold` default: 0.05 → 0.10
- [x] `max_tool_calls_per_turn` setting added (default 15)
- [x] Updated: `tests/test_semantic_cache.py` (threshold values)
- [x] Updated: `tests/test_callbacks.py` (3-row preview)
- [x] Added: `tests/test_prompts.py` (SQL trimming test)

## Files Modified
- `nl2sql_agent/tools/vector_search.py`
- `nl2sql_agent/tools/_deps.py`
- `nl2sql_agent/callbacks.py`
- `nl2sql_agent/catalog_loader.py`
- `nl2sql_agent/config.py`
- `nl2sql_agent/prompts.py`
- `tests/conftest.py`
- `tests/test_vector_search.py`
- `tests/test_callbacks.py`
- `tests/test_semantic_cache.py`
- `tests/test_prompts.py`

## Files Created
- `tests/test_combined_vector_search.py`
- `tests/test_circuit_breaker.py`
- `tests/test_yaml_cache.py`
- `conductor/tracks/08_loop_and_performance_fix/spec.md`
- `conductor/tracks/08_loop_and_performance_fix/plan.md`

## Verification
- 229 tests pass, 0 failures
