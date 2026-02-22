# Plan: YAML Schema Enrichment & Data Profiling

## Phase 1: Pydantic Schema Models & Validation Infrastructure

### Task 1.1: Create Pydantic validation models (fd88c1b)
- [x] Create `catalog/schema.py` with `ColumnSchema`, `TableSchema`, `DatasetSchema` Pydantic models
- [x] `ColumnSchema` validates: name, type, description (required), category, typical_aggregation, filterable, example_values, comprehensive, formula, related_columns, synonyms, source, business_rules (optional)
- [x] Category constrained to `Literal["dimension", "measure", "time", "identifier"]`
- [x] Aggregation constrained to `Literal["SUM", "AVG", "WEIGHTED_AVG", "COUNT", "MIN", "MAX"]`
- [x] Cross-field validation: `typical_aggregation` only allowed when `category == "measure"`, `comprehensive` only allowed when `example_values` present

### Task 1.2: Write CI validation test (6e3e36e)
- [x] Create `tests/test_catalog_validation.py` — loads every YAML in `catalog/{kpi,data}/`, validates against Pydantic models
- [x] Test validates column-level fields when present (category enum, aggregation enum, etc.)
- [x] Test passes with current YAMLs (no enrichment yet — all new fields optional)

### Task 1.3: Create standalone validation script (826f13a)
- [x] Create `scripts/validate_catalog.py` — accepts file path(s) or `--all` flag
- [x] Outputs per-file validation results (pass/fail with error details)
- [x] Exit code 0 if all valid, 1 if any errors

### Phase 1 Completion — Verification and Checkpointing

---

## Phase 2: Track Completion & Deferred Population Track

### Task 2.1: Create deferred population track
- [~] Create `conductor/tracks/22_metadata_population/metadata.json`
- [ ] Create `conductor/tracks/22_metadata_population/brief.md` covering:
  - Heuristic enrichment script (scripts/enrich_columns.py)
  - BQ data profiling script (scripts/profile_columns.py)
  - Tiered population: Tier 1 human-curated, Tier 2 heuristic, Tier 3 LLM-assisted
  - Verification of ALL existing metadata (descriptions, synonyms, example_values, formulas)
  - Prerequisites: C++ repo access + KPI repo access for formula/business_rules verification
- [ ] Update `conductor/tracks.md` with new track entry
- [ ] Update `architect/dependency-graph.md` and `architect/execution-sequence.md`

### Task 2.2: Final validation pass
- [ ] Run full test suite `pytest tests/ -v` — all tests pass
- [ ] Run `scripts/validate_catalog.py --all` — zero errors
- [ ] Verify Pydantic models accept enrichment fields when populated

### Phase 2 Completion — Verification and Checkpointing
