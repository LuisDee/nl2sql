# Track 15: Code Quality & Reliability

## Problem

A thorough codebase review (2026-02-20) identified several issues NOT covered by Track 13 (Autopsy Fixes). These fall into four categories:

1. **Exchange-aware cache pollution**: After Track 14 (Multi-Exchange Support), the semantic cache key is only the question text. A cached OMX query can be returned for a Brazil question if the text is similar enough. The cache needs exchange context.

2. **Return type inconsistency**: All 8 agent tools return `dict` but with different ad-hoc key structures. No shared contracts, no IDE autocompletion, no compile-time validation. This makes it fragile to refactor and hard for the LLM to reason about tool outputs consistently.

3. **Prompt regeneration overhead**: The `build_nl2sql_instruction()` function regenerates ~1800 tokens of static content (exchange list, tool descriptions, routing rules) on every LLM turn. Static sections should be cached.

4. **Module-level global state**: `_deps.py` uses mutable module-level globals (`_bq_service`, `_vector_cache_*`) with no thread safety. Single-session use is safe today, but any future concurrency (e.g., MCP server handling multiple requests) would cause race conditions.

5. **Dead code and missing developer tooling**: `get_table_schema()` exists in the protocol and client but is never called by any tool. No `.env.example` template exists for onboarding.

### Relationship to Track 13

Track 13 (Autopsy Fixes) covers critical production bugs: ARRAY_LENGTH(NULL), circuit breaker reset, example column names, PnL routing, query timeout, SQL guard bypass, LIMIT detection, vector search CTE, destructive DDL, Docker fixes, lazy BQ init, online eval, auto-discovery, autonomous embeddings.

This track covers issues that Track 13 does NOT address: cache exchange-awareness, type safety, prompt optimization, global state hygiene, and developer experience.

### False Positives from Review

Two findings from the review were verified as NOT bugs:

1. **`learning_loop.py:28` uses `RETRIEVAL_DOCUMENT`** — This is CORRECT. Stored content (documents to be searched against) should use `RETRIEVAL_DOCUMENT`. The search query side correctly uses `RETRIEVAL_QUERY` in `semantic_cache.py` and `vector_search.py`.

2. **`vector_search_tables()` is dead code** — FALSE. It serves as the fallback path in `vector_search_columns()` (line 443) when column-level search fails.

## Solution

A 4-phase track prioritized by impact:

- **Phase 1 (HIGH)**: Exchange-aware semantic cache — prevents cross-exchange cache pollution
- **Phase 2 (MEDIUM)**: TypedDict return contracts for all tools — type safety
- **Phase 3 (MEDIUM)**: Prompt caching + retrieval tuning + developer tooling
- **Phase 4 (LOW)**: Dead code cleanup, thread safety documentation, error handling

## Scope

### In Scope
- Exchange-aware cache key in `semantic_cache.py`
- TypedDict definitions for all 8 tool return types
- Prompt instruction caching (static vs. dynamic sections)
- `.env.example` template file
- Dead code removal (`get_table_schema` from protocol, client, fakes)
- Thread safety documentation for `_deps.py` globals
- Column search `top_k` configurability review

### Out of Scope
- All items covered by Track 13 (see list above)
- KPI YAML deduplication (separate large-scope track)
- Dependency lock file / requirements pinning
- Pre-commit hooks setup
- Integration test reorganization

## Key Design Decisions

### 1. Exchange-aware cache key
Prepend resolved exchange to the cache key: `f"{exchange}:{question}"`. When no exchange is resolved, use `"default:{question}"`. This requires the cache check to happen AFTER `resolve_exchange` in the tool chain, or accept that cache-first means default-exchange-only cache hits.

**Chosen approach**: Keep cache check FIRST (latency optimization). Add exchange to the cache result's `cached_dataset` field. If the cache hit's dataset doesn't match the resolved exchange's dataset, treat it as a miss. This is a post-hoc validation rather than key modification — simpler, no tool reordering needed.

### 2. TypedDict granularity
One TypedDict per tool return type (e.g., `CacheHitResult`, `CacheMissResult`, `VectorSearchResult`). Union types for tools with multiple return shapes. Defined in a new `nl2sql_agent/types.py` module.

### 3. Prompt caching strategy
Split `build_nl2sql_instruction()` into static (cached at module level) and dynamic (session state dependent) sections. Use `@functools.lru_cache` for the static part. The dynamic part (session history, resolved exchange) is appended per-call.

### 4. Dead code policy
Remove `get_table_schema()` from `BigQueryProtocol`, `LiveBigQueryClient`, `FakeBigQueryClient`, and `MockBigQueryService`. If it was never called, it shouldn't be in the protocol.

## Files Modified

| File | Phase | Change |
|------|-------|--------|
| `nl2sql_agent/tools/semantic_cache.py` | 1 | Exchange-aware cache validation |
| `nl2sql_agent/callbacks.py` | 1 | Pass exchange context to cache |
| `nl2sql_agent/types.py` | 2 | NEW: TypedDict definitions |
| `nl2sql_agent/tools/semantic_cache.py` | 2 | Typed returns |
| `nl2sql_agent/tools/vector_search.py` | 2 | Typed returns |
| `nl2sql_agent/tools/metadata_loader.py` | 2 | Typed returns |
| `nl2sql_agent/tools/sql_validator.py` | 2 | Typed returns |
| `nl2sql_agent/tools/sql_executor.py` | 2 | Typed returns |
| `nl2sql_agent/tools/learning_loop.py` | 2 | Typed returns |
| `nl2sql_agent/tools/exchange_resolver.py` | 2 | Typed returns |
| `nl2sql_agent/prompts.py` | 3 | Split static/dynamic, cache static |
| `nl2sql_agent/config.py` | 3 | Review column_search_top_k default |
| `nl2sql_agent/protocols.py` | 4 | Remove get_table_schema |
| `nl2sql_agent/clients.py` | 4 | Remove get_table_schema |
| `nl2sql_agent/tools/_deps.py` | 4 | Thread safety docs |
| `tests/fakes.py` | 4 | Remove get_table_schema |
| `tests/conftest.py` | 4 | Remove get_table_schema from mock |
| `tests/test_protocols.py` | 4 | Remove get_table_schema tests |

## Files Created

| File | Phase | Purpose |
|------|-------|---------|
| `nl2sql_agent/types.py` | 2 | Shared TypedDict definitions |
| `.env.example` | 3 | Onboarding template |
| `tests/test_cache_exchange.py` | 1 | Exchange-aware cache tests |
| `tests/test_types.py` | 2 | TypedDict contract tests |

## Acceptance Criteria

1. All existing tests pass (357+)
2. Semantic cache miss when cached dataset doesn't match resolved exchange
3. All 8 tools annotated with TypedDict return types
4. `build_nl2sql_instruction()` only regenerates dynamic sections per-call
5. `.env.example` exists with all required env vars documented
6. `get_table_schema()` removed from protocol and all implementations
7. `_deps.py` has documented thread safety constraints
8. ~15+ new tests covering cache exchange awareness and type contracts

## Dependencies

- Track 14 (Multi-Exchange Support) — complete
- Track 13 (Autopsy Fixes) — independent, can run in parallel

## Risks

| Risk | Mitigation |
|------|------------|
| Exchange-aware cache changes may reduce cache hit rate | Post-hoc validation only rejects cross-exchange hits; same-exchange hits preserved |
| TypedDict adoption may break existing test mocks | TypedDict is structural typing — existing dicts that match the shape still pass |
| Prompt caching with lru_cache may serve stale exchange lists | Cache static section only (tool descriptions, routing rules); exchange list is dynamic |
| Removing get_table_schema may break future features | It's unused today and easy to re-add if needed |
