# Plan: Metadata Population — Remaining Gaps

## Phase 1: BQ Data Profiling (FR-1)

### Task 1.1: Write profiling script + tests
- [x] Create `scripts/profile_columns.py` with BQ profiling logic (5bb7f42)
- [x] Write tests for cardinality tier classification and YAML update logic (23 tests)
- [x] Script queries APPROX_COUNT_DISTINCT + APPROX_TOP_COUNT per table
- [x] Partition filter: `WHERE trade_date = "2026-02-17"`
- [x] Tiered logic: <25 comprehensive, 25-250 top 10, 250+ skip

### Task 1.2: Run profiling and update YAMLs
- [x] Run script against all 12 tables — 458 assigned, 68 preserved (4c8f44e)
- [x] Validate with catalog/schema.py (93 tests pass)
- [x] Commit changes (4c8f44e)

### Task 1.3: Phase 1 checkpoint
- [x] example_values populated: 526/4,631 (11.4%)
- [x] comprehensive flag: 458/4,631 (9.9%)
- [ ] Human review

---

## Phase 2: Source Field Population (FR-2)

### Task 2.1: Write source enrichment script + tests
- [x] Create `scripts/enrich_source.py` reading proto_fields.yaml (d766e0e)
- [x] Map proto message.field → YAML source field
- [x] Write tests for mapping logic (12 tests)

### Task 2.2: Run source enrichment
- [x] Run against all 12 tables — 223 assigned (80f956d)
- [x] Validate with catalog/schema.py (70 tests pass)
- [x] Commit changes (80f956d)

### Task 2.3: Phase 2 checkpoint
- [x] source field populated: 1,267/4,631 (27.4%, up from 22.5%)
- [ ] Human review

---

## Phase 3: Business Rules & Synonyms Verification (FR-3, FR-4)

### Task 3.1: Verify business_rules via sub-agents
- [~] Launch sub-agents with KPI repo AGENTS.md context
- [ ] Verify 428 business_rules against source
- [ ] Fix hallucinated rules
- [ ] Commit fixes

### Task 3.2: Verify synonyms via sub-agents
- [ ] Launch sub-agents with all repo AGENTS.md cards
- [ ] Verify 2,262 synonyms against source context
- [ ] Fix hallucinated synonyms
- [ ] Commit fixes

### Task 3.3: Phase 3 checkpoint
- [ ] All business_rules verified
- [ ] All synonyms verified
- [ ] Full test suite passes
- [ ] Human review
