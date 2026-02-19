# Execution Sequence

> Wave-based ordering derived from the dependency graph.
> Tracks within the same wave are independent and can run in parallel.
> Last synced: 2026-02-19

---

## Wave 1 — Foundation (Phase A) [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 01 | 01_foundation | M | Repo scaffolding, Dev Data, Schema extraction, Agent Skeleton | completed |
| 07 | 07_dependency_fix | S | Add db-dtypes dependency for timestamp handling | completed |

### Wave 1 Completion Criteria
- [x] Repo initialized with `google-adk`, `google-cloud-bigquery`.
- [x] Dev dataset populated with sample data.
- [x] Schemas extracted.
- [x] Root Agent (`mako_assistant`) delegates to Sub-Agent (`nl2sql_agent`) successfully.
- [x] db-dtypes added to resolve execute_sql timestamp errors.

---

## Wave 2 — Context Layer (Phase B) [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 02 | 02_context_layer | L | YAML Catalog (Layer 1), Examples, Embeddings (Layer 2) | completed |

### Wave 2 Completion Criteria
- [x] YAML catalog files created matching actual schema.
- [x] Validated Q->SQL examples written and tested.
- [x] `metadata` dataset created in BigQuery.
- [x] `schema_embeddings` and `query_memory` tables populated and indexed.
- [x] Vector search routing tests passing.

---

## Wave 3 — Agent Tools + Metadata Enrichment (Phase C) [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 03 | 03_agent_tools | M | Implement tools: vector_search, metadata_loader, dry_run, execute_sql, learning_loop | completed |
| 06 | 06_metadata_enrichment | L | Enrich YAML catalog + BQ embeddings from kpi-findings and proto-findings | completed |

### Wave 3 Completion Criteria
- [x] `vector_search_tables` tool retrieves correct tables.
- [x] `fetch_few_shot_examples` shares embedding call with vector_search.
- [x] `load_yaml_metadata` returns full YAML content with dataset context.
- [x] `dry_run_sql` catches syntax errors.
- [x] `execute_sql` runs SELECT queries (read-only) with auto-LIMIT.
- [x] `save_validated_query` writes back to `query_memory` with auto-embedding.
- [x] All 7 tools wired into `nl2sql_agent`.
- [x] YAML catalog enriched from source analysis.

---

## Wave 4 — Agent Logic (Phase D) [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 04 | 04_agent_logic | M | System Prompts, Routing Logic, End-to-End Testing | completed |

### Wave 4 Completion Criteria
- [x] NL2SQL system prompt refined with routing rules.
- [x] Agent correctly routes KPI vs data table questions.
- [x] Agent correctly handles "all trades" unions.
- [x] End-to-end tests pass for core request types.

---

## Wave 5 — Eval & Hardening (Phase E/F) [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 05 | 05_eval_hardening | L | Eval Set, Semantic Cache, Learning Loop integration | completed |

### Wave 5 Completion Criteria
- [x] Evaluation set created.
- [x] Semantic caching implemented (`check_semantic_cache` tool).
- [x] Learning loop (`save_validated_query`) integrated.

---

## Wave 6 — Loop Fix & Performance (Phase G) [PENDING]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 08 | 08_loop_and_performance_fix | M | Fix infinite reasoning loops, reduce LLM round-trips, optimize context | new |

### Wave 6 Completion Criteria
- [ ] No infinite reasoning loops in agent conversation.
- [ ] LLM round-trips reduced (target: <5 per question).
- [ ] Context size optimized (no redundant metadata loading).
