# Implementation Plan: Eval & Hardening

> Based on `spec.md` and `brief.md`.

## Phase 1: Online Evaluation Runner
Implement an execution mode that runs queries end-to-end against the agent, validating not just SQL but also result accuracy.

- [x] Task 1.1: Create `OnlineEvalRunner` class in `eval/run_eval.py` to instantiate and execute `nl2sql_agent`.
- [x] Task 1.2: Add result comparison logic (compare row counts, schema, and sample values) for "Result Match" metric.
- [x] Task 1.3: Run `run_eval.py --online` against `dev_agent_test` and verify basic functionality.

## Phase 2: LoopAgent & Retry Logic
Formalize and test the self-correcting retry loop for SQL generation.

- [x] Task 2.1: Create a dedicated test case `tests/integration/test_retry_loop.py` that mocks a `dry_run_sql` failure to force a retry.
- [x] Task 2.2: Refactor or verify `callbacks.py` to ensure robust error handling and loop termination (max 3 retries).
- [x] Task 2.3: Verify that the agent correctly receives the error message in the observation and generates a fixed query.

## Phase 3: Semantic Cache & Learning Loop
Verify and integrate the semantic caching mechanism.

- [x] Task 3.1: Add a test case `tests/integration/test_semantic_cache.py` to verify `check_semantic_cache` returns a hit for a known query.
- [x] Task 3.2: Verify `save_validated_query` correctly inserts into `query_memory` and updates embeddings.

## Phase 4: Full Evaluation & Hardening
Run the full suite and harden the system.

- [ ] Task 4.1: Populate `eval/gold_queries.yaml` with any missing expected SQL/results.
- [ ] Task 4.2: Execute `run_eval.py` in full online mode and achieve >90% routing accuracy.
- [ ] Task 4.3: Document performance metrics (latency, error rate).
