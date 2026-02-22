# Execution Sequence

> Wave-based ordering derived from the dependency graph.
> Tracks within the same wave are independent and can run in parallel.
> Last synced: 2026-02-21

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
- [x] All 7 tools wired into `nl2sql_agent`.
- [x] YAML catalog enriched from source analysis.

---

## Wave 4 — Agent Logic (Phase D) [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 04 | 04_agent_logic | M | System Prompts, Routing Logic, End-to-End Testing | completed |

---

## Wave 5 — Eval & Hardening (Phase E/F) [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 05 | 05_eval_hardening | L | Eval Set, Semantic Cache, Learning Loop integration | completed |

---

## Wave 6 — Loop Fix & Performance [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 08 | 08_loop_and_performance_fix | M | Fix infinite reasoning loops, reduce LLM round-trips | completed |

---

## Wave 7 — Production Hardening [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 09 | 09_production_hardening | M | Serialization fix, hash-based repetition detection | completed |

---

## Wave 8 — Metadata Gaps + MCP [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 10 | 10_metadata_gaps | M | Trade taxonomy, preferred timestamps, ATM strike patterns | completed |
| 11 | 11_gemini_cli_mcp | M | MCP server with stdio transport for Gemini CLI | completed |

---

## Wave 10 — Column Semantic Search [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 12 | 12_column_semantic_search | L | Two-tier column-level vector search (4,631 embeddings) | completed |

---

## Wave 11 — Autopsy Fixes + Multi-Exchange [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 13 | 13_autopsy_fixes | L | Fix 10 critical + 20 high-severity findings | completed |
| 14 | 14_multi_exchange | M | Multi-exchange routing via alias/symbol lookup | completed |

---

## Wave 12 — Code Quality [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 15 | 15_code_quality | M | Exchange-aware cache, TypedDict contracts, prompt caching | completed |

---

## Wave 13 — Repo Scaffolding [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 16 | 16_repo_scaffolding | M | Ruff, mypy, pre-commit, GitHub Actions CI, Makefile | completed |

---

## Wave 14 — Routing Consolidation [COMPLETED]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 17 | 17_routing_and_pipeline | M | Single source of truth for routing rules, embedding pipeline tests | completed |

---

## Wave 15 — Schema Enrichment + Example Expansion [PENDING]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 18 | 18_yaml_schema_enrichment | L | YAML enrichment (category, formula, example_values, etc.) + BQ profiling pipeline | new |
| 20 | 20_few_shot_expansion | M | Expand Q→SQL examples from 54 to 150+ | new |

### Wave 15 Completion Criteria
- [ ] Pydantic schema models defined and CI-enforced (catalog/schema.py)
- [ ] All existing YAML files pass Pydantic validation
- [ ] Standalone validation script available (scripts/validate_catalog.py)
- [ ] 150+ validated Q→SQL examples passing dry-run and column checks
- [ ] Coverage matrix shows >80% table × pattern coverage

---

## Wave 16 — Embedding Strategy + Metrics/Filters [PENDING]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 19 | 19_embedding_enrichment | L | Separate retrieval/generation context in embeddings, glossary collection | new |
| 21 | 21_metrics_and_filters | M | Metric definitions, named filters, circuit breaker fix, alignment scorecard | new |

### Wave 16 Completion Criteria
- [ ] Embedding text rebuilt with category + example_values (not formula/description)
- [ ] Business glossary collection in BQ with 20-30 concept embeddings
- [ ] Vector search CTE searches both column and glossary collections
- [ ] 15-20 metric definitions in `catalog/metrics.yaml`
- [ ] 10-15 named filters in `catalog/named_filters.yaml`
- [ ] Circuit breaker resets between questions
- [ ] Alignment scorecard test suite reports coverage metrics

---

## Wave 17 — Metadata Population [PENDING]

| # | Track ID | Complexity | Description | Status |
|---|----------|------------|-------------|--------|
| 22 | 22_metadata_population | L | Populate enrichment fields, verify existing metadata, BQ profiling | new |

### Wave 17 Completion Criteria
- [ ] All 4,631 columns have `category` populated
- [ ] All `measure` columns have `typical_aggregation`
- [ ] Categorical columns (<250 cardinality) have `example_values` with `comprehensive` flag
- [ ] All existing descriptions, synonyms, formulas verified against source repos
- [ ] Heuristic enrichment script auto-assigns >80% of columns
- [ ] BQ profiling pipeline generates tiered example_values
