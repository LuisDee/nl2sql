# Execution Sequence

> Wave-based ordering derived from the dependency graph.
> Tracks within the same wave are independent and can run in parallel.

---

## Wave 1 — Foundation (Phase A)

| # | Track ID | Complexity | Description |
|---|----------|------------|-------------|
| 01 | 01_foundation | M | Repo scaffolding, Dev Data, Schema extraction, Agent Skeleton |

### Wave 1 Completion Criteria
- [ ] Repo initialized with `google-adk`, `google-cloud-bigquery`.
- [ ] Dev dataset `dev_agent_test` populated with sample data.
- [ ] Schemas extracted to JSON.
- [ ] Root Agent (`mako_assistant`) delegates to Sub-Agent (`nl2sql_agent`) successfully.
- [ ] Agent responds "I can help with that" (even without tools).

---

## Wave 2 — Context Layer (Phase B)

| # | Track ID | Complexity | Description |
|---|----------|------------|-------------|
| 02 | 02_context_layer | L | YAML Catalog (Layer 1), Examples, Embeddings (Layer 2) |

### Wave 2 Completion Criteria
- [ ] 8 YAML catalog files created matching actual schema.
- [ ] 30+ validated Q->SQL examples written and tested.
- [ ] `metadata` dataset created in BigQuery.
- [ ] `schema_embeddings` and `query_memory` tables populated and indexed.
- [ ] 5/5 routing vector search tests passing.

---

## Wave 3 — Agent Tools (Phase C)

| # | Track ID | Complexity | Description |
|---|----------|------------|-------------|
| 03 | 03_agent_tools | M | Implement `vector_search`, `metadata_loader`, `dry_run`, `execute_sql` |

### Wave 3 Completion Criteria
- [ ] `vector_search_tables` tool retrieves correct tables.
- [ ] `load_yaml_metadata` returns full YAML content.
- [ ] `dry_run_sql` catches syntax errors.
- [ ] `execute_sql` runs SELECT queries (read-only).
- [ ] `save_validated_query` writes back to `query_memory`.
- [ ] All 6 tools wired into `nl2sql_agent`.

---

## Wave 4 — Agent Logic (Phase D)

| # | Track ID | Complexity | Description |
|---|----------|------------|-------------|
| 04 | 04_agent_logic | M | System Prompts, Routing Logic, End-to-End Testing |

### Wave 4 Completion Criteria
- [ ] `NL2SQL_SYSTEM_PROMPT` refined with routing rules.
- [ ] Agent correctly routes KPI vs Quoter vs Theodata questions.
- [ ] Agent correctly handles "all trades" unions.
- [ ] Agent asks clarifying questions for ambiguous inputs.
- [ ] End-to-end tests pass for all 5 core request types.

---

## Wave 5 — Eval & Hardening (Phase E/F)

| # | Track ID | Complexity | Description |
|---|----------|------------|-------------|
| 05 | 05_eval_hardening | L | Gold Eval Set, Automated Runner, Retry Loop, Deploy Prep |

### Wave 5 Completion Criteria
- [ ] 50-question gold standard eval set created.
- [ ] `run_eval.py` measures accuracy and latency.
- [ ] `LoopAgent` retry logic handles dry-run failures.
- [ ] Learning loop (save query) integrated into UX.
- [ ] Semantic caching implemented for repeated queries.
