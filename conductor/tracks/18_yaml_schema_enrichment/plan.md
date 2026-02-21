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

### Task 1.3: Create standalone validation script
- [ ] Create `scripts/validate_catalog.py` — accepts file path(s) or `--all` flag
- [ ] Outputs per-file validation results (pass/fail with error details)
- [ ] Exit code 0 if all valid, 1 if any errors

### Phase 1 Completion — Verification and Checkpointing

---

## Phase 2: Heuristic Enrichment Script & BQ Profiling Pipeline

### Task 2.1: Create heuristic enrichment script
- [ ] Create `scripts/enrich_columns.py` with pattern-matching rules for auto-assigning category
- [ ] Rules: type-based (TIMESTAMP→time, BOOLEAN→dimension), name-pattern (\_id$→identifier, \_pnl→measure), interval-expanded pattern
- [ ] Script reads YAML, applies rules, writes enriched YAML back
- [ ] Reports: assigned count, unassigned count, confidence breakdown

### Task 2.2: Create BQ data profiling script
- [ ] Create `scripts/profile_columns.py` — queries `APPROX_TOP_COUNT` + `APPROX_COUNT_DISTINCT` per STRING column
- [ ] Tiered output: <25 cardinality → comprehensive=true (all values), 25-250 → comprehensive=false (top 10), 250+ → skip or top 5
- [ ] Outputs JSON file with profiling results per table
- [ ] Uses `Settings()` for project/dataset config

### Task 2.3: Write tests for enrichment and profiling scripts
- [ ] Unit tests for heuristic rules (test each pattern matches expected columns)
- [ ] Unit tests for cardinality tier classification logic
- [ ] Integration test marker for `profile_columns.py` (requires live BQ)

### Phase 2 Completion — Verification and Checkpointing

---

## Phase 3: Enrich KPI Table YAMLs (5 tables, ~4,074 columns)

### Task 3.1: Run heuristic enrichment on KPI tables
- [ ] Run `scripts/enrich_columns.py` on all 5 KPI table YAMLs
- [ ] Verify >80% of columns get auto-assigned category
- [ ] Run `scripts/validate_catalog.py --all` to confirm validity

### Task 3.2: Run BQ profiling for KPI tables
- [ ] Run `scripts/profile_columns.py` for KPI dataset
- [ ] Merge profiling results (example_values + comprehensive flag) into KPI YAMLs
- [ ] Validate with Pydantic

### Task 3.3: Human-curate Tier 1 KPI columns
- [ ] Identify Tier 1 columns in KPI tables (from examples + shared_columns)
- [ ] Add formula, related_columns, review category and typical_aggregation
- [ ] Validate with Pydantic

### Phase 3 Completion — Verification and Checkpointing

---

## Phase 4: Enrich Data Table YAMLs (7 tables, ~557 columns)

### Task 4.1: Run heuristic enrichment on data tables
- [ ] Run `scripts/enrich_columns.py` on all 7 data table YAMLs
- [ ] Verify >80% of columns get auto-assigned category
- [ ] Run `scripts/validate_catalog.py --all` to confirm validity

### Task 4.2: Run BQ profiling for data tables
- [ ] Run `scripts/profile_columns.py` for data dataset
- [ ] Merge profiling results into data YAMLs
- [ ] Validate with Pydantic

### Task 4.3: Human-curate Tier 1 data columns
- [ ] Identify Tier 1 columns in data tables (from examples)
- [ ] Add formula, related_columns, review category and typical_aggregation
- [ ] Validate with Pydantic

### Phase 4 Completion — Verification and Checkpointing

---

## Phase 5: Final Validation & Coverage Report

### Task 5.1: Full validation pass
- [ ] Run `scripts/validate_catalog.py --all` — zero errors
- [ ] Run `pytest tests/test_catalog_validation.py -v` — all pass
- [ ] Run full test suite `pytest tests/ -v` — all 578+ pass

### Task 5.2: Coverage report
- [ ] Generate enrichment coverage report: % columns with category, % with example_values, % with typical_aggregation
- [ ] Verify: 100% category coverage, >90% measure columns have typical_aggregation, all cardinality <250 STRING columns have example_values

### Phase 5 Completion — Verification and Checkpointing
