# Track 17: Routing Consolidation & Pipeline Testing

## Problem

### Routing Fragmentation

Table routing rules (which BigQuery table to query for a given question type) are duplicated across **5 locations** with no automated sync:

| Source | Lines | What It Contains | Who Reads It |
|--------|-------|-----------------|--------------|
| `catalog/_routing.yaml` | 88 | Descriptive routing docs, disambiguation table | Humans, embedding pipeline |
| `catalog/kpi/_dataset.yaml` | 166-188 | Structured `patterns → table` rules for KPI | Catalog loader |
| `catalog/data/_dataset.yaml` | 78-110 | Structured `patterns → table` rules for data | Catalog loader |
| `nl2sql_agent/prompts.py` | 131-139 | Hardcoded routing rules in system prompt | LLM (highest authority) |
| `scripts/run_embeddings.py` | 183-256 | Routing descriptions baked into BQ rows | Vector search |

The rules are currently consistent (no active conflicts), but there is **zero drift detection**. Any change to routing in one location must be manually replicated to the others. This will break as the codebase evolves — someone will update `_routing.yaml` and forget `prompts.py`, or vice versa.

### Embedding Pipeline Untested

`scripts/run_embeddings.py` (~270 LOC, 7 pipeline steps) and `scripts/populate_embeddings.py` have **zero unit tests**. This pipeline powers the entire vector search system — if it breaks, the agent loses its ability to route questions to the right tables.

## Solution

### Routing: YAML-Driven Prompt Generation

Make `_routing.yaml` + `_dataset.yaml` files the **single source of truth**. The system prompt reads routing rules from YAML at build time instead of hardcoding them in Python. This eliminates duplication while keeping each source authoritative for its domain:

- `_routing.yaml` — cross-cutting rules (KPI vs data, theodata-only, UNION warning)
- `kpi/_dataset.yaml` — KPI table pattern matching
- `data/_dataset.yaml` — data table pattern matching
- `prompts.py` — **reads from YAML** instead of hardcoding rules
- `run_embeddings.py` — **reads from YAML** for descriptions

### Pipeline: Unit Tests for All Steps

Add unit tests for `run_embeddings.py` covering:
- Table creation SQL generation
- Schema embedding population (row structure, SQL templates)
- Column embedding population
- Symbol map population
- Embedding generation SQL templates (the ARRAY_LENGTH fix from Track 13)

## Scope

### In Scope

- Refactor `prompts.py` routing section to read from YAML files
- Refactor `run_embeddings.py` descriptions to read from YAML files
- Add drift detection test: verify all routing sources are consistent
- Unit tests for `run_embeddings.py` pipeline steps
- Unit tests for `populate_embeddings.py`

### Out of Scope

- KPI YAML deduplication (intentional design choice — self-contained files)
- Changes to the actual routing rules themselves
- Vector search changes
- Exchange resolver changes (Track 14, complete)
- ARRAY_LENGTH fix and embedding generation fixes (Track 13 scope)

## Key Design Decisions

### 1. How prompts.py reads routing rules

**Option A**: `prompts.py` imports `catalog_loader.py` functions to read `_routing.yaml` and `_dataset.yaml` at prompt build time, then formats them into the prompt string.

**Option B**: `prompts.py` reads a single rendered routing section from `_routing.yaml` that's already in prompt-ready format.

**Chosen: Option A** — keeps YAML files as structured data (not prompt text), and `prompts.py` controls the rendering format. The YAML files stay clean for other consumers (embedding pipeline, tests).

### 2. Drift detection approach

A pytest test that:
1. Parses all routing YAML files for pattern→table mappings
2. Checks that `prompts.py`'s generated routing section mentions all tables from YAML
3. Checks that embedding descriptions in `run_embeddings.py` reference all tables

This runs in CI — if someone adds a table to YAML but forgets to update descriptions, the test fails.

### 3. Embedding pipeline test strategy

Test the SQL template generation and row structure, NOT actual BQ execution. Mock the BQ client. Verify:
- SQL templates are syntactically correct (contain expected clauses)
- Row structures match expected schema
- All pipeline steps are callable
- Edge cases: empty tables, NULL embeddings, duplicate rows

## Files Modified

| File | Change |
|------|--------|
| `nl2sql_agent/prompts.py` | Replace hardcoded routing rules with YAML-driven generation |
| `nl2sql_agent/catalog_loader.py` | Add `load_routing_rules()` function |
| `scripts/run_embeddings.py` | Replace hardcoded descriptions with YAML-driven generation |

## Files Created

| File | Purpose |
|------|---------|
| `tests/test_routing_consistency.py` | Drift detection: all routing sources in sync |
| `tests/test_run_embeddings.py` | Unit tests for embedding pipeline steps |
| `tests/test_populate_embeddings.py` | Unit tests for populate script |

## Acceptance Criteria

1. `prompts.py` routing section generated from YAML, not hardcoded
2. Adding a new table to `_dataset.yaml` automatically appears in the prompt
3. Drift detection test catches when YAML and prompt are out of sync
4. `run_embeddings.py` has >80% test coverage for non-BQ-execution code
5. All existing tests pass (370+)
6. No change to actual routing behavior (same rules, different source)

## Dependencies

- Track 13 (Autopsy Fixes) — ARRAY_LENGTH fix in embedding pipeline
- Track 15 (Code Quality) — TypedDict returns for catalog_loader
- Track 16 (Repo Scaffolding) — ruff/mypy must pass on new code

## Risks

| Risk | Mitigation |
|------|------------|
| YAML-driven prompt may produce different LLM behavior | Compare generated prompt before/after; diff should be whitespace/ordering only |
| Routing YAML structure may not capture all nuance in prompt text | Keep cross-cutting rules (UNION warning, theodata-only) as explicit sections in _routing.yaml |
| Embedding pipeline tests may be brittle (SQL string matching) | Test structure/clauses, not exact SQL strings; use regex patterns |
