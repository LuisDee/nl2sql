# Track 08: Loop Fix & Performance Optimization

## Problem Statement

1. **Redundant embedding generation**: Every question triggers `ML.GENERATE_EMBEDDING` 3 separate times (semantic cache, vector_search_tables, fetch_few_shot_examples) for the same string. Each call costs ~1-2s of Vertex AI latency.

2. **No hard circuit breaker**: `after_tool_log` sets `max_retries_reached=True` but nothing prevents the LLM from calling `dry_run_sql` again.

3. **No YAML caching**: disk I/O on every `load_yaml()` call.

4. **Tight semantic cache threshold**: 0.05 misses reasonable paraphrases.

## Solution

- Combined CTE query: single embedding → two VECTOR_SEARCH in one round-trip
- Per-question cache in `_deps.py`: `fetch_few_shot_examples()` is a Python-level cache hit
- Hard circuit breaker in `before_tool_guard`: returns error dict when `max_retries_reached=True`
- Global tool call counter with configurable max (default 15)
- `@lru_cache` on `load_yaml()`
- Prompt trimming (500 char SQL, 3 row preview)
- Threshold tuned from 0.05 → 0.10

## Expected Impact

| Metric | Before | After |
|--------|--------|-------|
| Embedding calls/question | 3 | 1-2 |
| Retry loop risk | Soft hint only | Hard block |
| YAML disk reads/question | 2-3 | 0 (cached) |
| Max runaway tool calls | Unlimited | 15 |
