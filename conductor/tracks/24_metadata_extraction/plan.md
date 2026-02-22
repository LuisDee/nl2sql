# Plan: Metadata Extraction from Source Repos

## Phase 1: Formula Verification & Extraction (FR-1)

### Task 1.1: Write formula enrichment script
- [~] Create `scripts/enrich_formulas.py` that reads `metadata/kpi_computations.yaml` and updates KPI table YAMLs
- [ ] Script verifies existing 318 formulas against source, flags mismatches
- [ ] Script adds missing formulas for columns found in kpi_computations.yaml
- [ ] Script includes intermediate calculations (mid_base_val, delta, etc.)
- [ ] Write tests for the enrichment logic

### Task 1.2: Run formula enrichment and verify
- [ ] Run the script against all 5 KPI table YAMLs
- [ ] Validate output with `catalog/schema.py`
- [ ] Launch source repo agent to spot-check 10 formulas against KPI repo SQL
- [ ] Commit changes

### Task 1.3: Phase 1 checkpoint
- [ ] All KPI formulas verified and updated
- [ ] Human review: spot-check 5 formula changes

---

## Phase 2: Category Assignment (FR-2)

### Task 2.1: Write category assignment script
- [ ] Create `scripts/enrich_categories.py` with deterministic heuristic rules
- [ ] Rules use column name patterns, proto type info from `proto_fields.yaml`, and existing formula presence
- [ ] Script is idempotent — safe to re-run
- [ ] Write tests for the heuristic rules (unit test each rule with known columns)

### Task 2.2: Run category assignment
- [ ] Run against all 12 table YAMLs (5 KPI + 7 data)
- [ ] Validate output with `catalog/schema.py`
- [ ] Generate summary report: count per category (measure/dimension/time/identifier) per table
- [ ] Commit changes

### Task 2.3: Phase 2 checkpoint
- [ ] All 4,631 columns have a category
- [ ] Human review: check category distribution looks reasonable

---

## Phase 3: Aggregation, Filterable, Related Columns (FR-3, FR-4, FR-5)

### Task 3.1: Write aggregation + filterable enrichment script
- [ ] Create `scripts/enrich_aggregation.py` that assigns `typical_aggregation` to measure columns and `filterable` to dimension/identifier columns
- [ ] Rules: PnL/edge/slippage → SUM, price/TV/Greek → AVG, size/volume → SUM, etc.
- [ ] Write tests

### Task 3.2: Write related columns enrichment script
- [ ] Create `scripts/enrich_related.py` that reads `metadata/field_lineage.yaml` and populates `related_columns`
- [ ] Cap at 5 per column (schema.py constraint)
- [ ] Write tests

### Task 3.3: Run all enrichments and validate
- [ ] Run both scripts against all table YAMLs
- [ ] Validate with `catalog/schema.py`
- [ ] Commit changes

### Task 3.4: Phase 3 checkpoint
- [ ] All measures have typical_aggregation
- [ ] Key columns have related_columns
- [ ] Dimension/identifier columns have filterable flag
- [ ] Human review

---

## Phase 4: Description Verification (FR-6)

### Task 4.1: Write description verification tests
- [ ] Tests that check for known hallucination patterns in descriptions (references to non-existent columns, wrong formula references)
- [ ] Tests that cross-check description column references against actual column names in the same table

### Task 4.2: Launch source repo agents for description verification
- [ ] Batch 1: KPI table descriptions — agent reads kpi_computations.yaml + KPI repo AGENTS.md, flags suspicious descriptions
- [ ] Batch 2: Data table descriptions — agent reads data_loader_transforms.yaml + data-loader repo AGENTS.md, flags suspicious descriptions
- [ ] Collect flagged descriptions

### Task 4.3: Fix flagged descriptions
- [ ] Update flagged descriptions with source-grounded text
- [ ] Re-run verification tests
- [ ] Validate with catalog/schema.py
- [ ] Commit changes

### Task 4.4: Phase 4 checkpoint
- [ ] All descriptions verified against source
- [ ] Hallucinated references fixed
- [ ] Full test suite passes
- [ ] Human review: spot-check 10 description fixes

---

## Phase 5: Final Validation

### Task 5.1: Run full validation
- [ ] Run `catalog/schema.py` validation against all 12 table YAMLs
- [ ] Run all enrichment tests
- [ ] Run full project test suite
- [ ] Calculate existence ratio (target >95%)

### Task 5.2: Generate enrichment report
- [ ] Summary of changes: fields populated per table, formulas verified/added/fixed, categories assigned, descriptions fixed
- [ ] Existence ratio score
- [ ] Commit final state
