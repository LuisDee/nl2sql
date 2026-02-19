# Track 06: Metadata Enrichment — Implementation Plan

## Phase 1: Structured Extraction (Estimated: 6 tasks)

### Task 1.1: Extract KPI-Findings — Markettrade
- [ ] Read `kpi-findings` sections covering markettrade
- [ ] Extract every column name, description, formula, enum value, business rule
- [ ] Capture interval expansion pattern (9 intervals x metrics) with concrete column name list
- [ ] Capture cross-cutting: combo logic, WHITE portfolio handling
- [ ] Write to `catalog/enrichment/kpi_markettrade_extracted.yaml`

### Task 1.2: Extract KPI-Findings — Quotertrade, Brokertrade, Clicktrade, Otoswing
- [ ] Process each table's section from kpi-findings
- [ ] Capture unique columns per table (mid_tv, mark_to_mid_pnl for quotertrade; fee_method, instant_pnl_w_fees for brokertrade; transaction_id, algorithm for clicktrade; swing_edge, fired_at metrics for otoswing)
- [ ] Capture all formulas unique to each table
- [ ] Write to `catalog/enrichment/kpi_{table}_extracted.yaml` (4 files)

### Task 1.3: Extract KPI-Findings — Cross-Cutting Concepts
- [ ] Enums: trade_side values (BUY/SELL/66/83/BUYSELL_BUY/BUYSELL_SELL), fee_method values, portfolio values, algorithm values
- [ ] Interval expansion: standard 9 intervals + multiday intervals, which metrics expand
- [ ] Combo logic: how parent/leg rows are aggregated
- [ ] Pipeline flow: ingest → normalize → enrich → calculate → aggregate → mart
- [ ] Disambiguation table: question pattern → correct table
- [ ] Write to `catalog/enrichment/kpi_cross_cutting_extracted.yaml`

### Task 1.4: Extract Proto-Findings — KPI Tables (5 tables)
- [ ] For each of markettrade, quotertrade, brokertrade, clicktrade, otoswing (via swingdata)
- [ ] Extract proto field name → BQ column name mapping
- [ ] Extract nested types: VtCommon fields, FillInfo fields, OraTheoreticals fields
- [ ] Flag deprecated fields
- [ ] Capture git-traced additions
- [ ] Write to `catalog/enrichment/proto_kpi_extracted.yaml`

### Task 1.5: Extract Proto-Findings — Data Tables (theodata, marketdepth, swingdata)
- [ ] Extract all proto fields with types and comments
- [ ] Capture enum definitions: MarketState, CurrencyCode, ExchCode, PositionType
- [ ] Capture nested types: MarketLevel (bids/asks), Theoreticals fields
- [ ] Capture bitmask definitions: TransactionProperty, TransactionTag (clicktrade)
- [ ] Write to `catalog/enrichment/proto_data_extracted.yaml`

### Task 1.6: Extraction Completeness Count
- [ ] Count total columns extracted from kpi-findings per table
- [ ] Count total fields extracted from proto-findings per table
- [ ] Produce summary: `catalog/enrichment/extraction_summary.md`
- [ ] Verify no sections of source files were skipped

---

## Phase 2: Gap Analysis (Estimated: 2 tasks)

### Task 2.1: Diff Extractions Against Existing YAML
- [ ] For each of 13 tables, compare extracted columns vs. existing YAML columns
- [ ] Categorize every column: NEW | ENRICH | CONFLICT | UNCHANGED
- [ ] For ENRICH columns: note what's being added (formula? synonyms? example_values? business_rules?)
- [ ] For CONFLICT columns: note the discrepancy
- [ ] Write to `catalog/enrichment/gap_report.md`

### Task 2.2: Identify New Few-Shot Example Opportunities
- [ ] Review formulas and business rules for query patterns not yet in `examples/*.yaml`
- [ ] List 15-25 candidate Q→SQL pairs organized by table and complexity
- [ ] Flag routing-critical examples (kpi vs data disambiguation)
- [ ] Write to `catalog/enrichment/new_examples_candidates.md`

---

## Phase 3: YAML Merge — KPI Tables (Estimated: 5 tasks)

> Process one table at a time. After each table, run `pytest tests/test_yaml_catalog.py tests/test_catalog_loader.py` to verify YAML structure validity.

### Task 3.1: Enrich `catalog/kpi/markettrade.yaml`
- [ ] Expand thin column descriptions to 30-80 words with business context
- [ ] Add synonyms inline to descriptions AND to synonyms field
- [ ] Add `formula` field for all computed columns (instant_edge, instant_pnl, all slippage metrics)
- [ ] Add `example_values` for enum columns (algo, trade_aggressor_side, portfolio)
- [ ] Add `business_rules` for columns with special handling (WHITE portfolio, combo aggregation)
- [ ] Add `source` field mapping to proto message.field
- [ ] Document interval expansion columns exhaustively
- [ ] Run YAML validation tests

### Task 3.2: Enrich `catalog/kpi/quotertrade.yaml`
- [ ] Same enrichment pattern as 3.1
- [ ] Unique: mid_tv formula, mark_to_mid_pnl, instant_edge_mid_tv, nhr_adjustment
- [ ] Unique: KRX per-side quoting fields (lastTradePrice, marketVolumeBid/Ask, ourVolumeBid/Ask)
- [ ] Disambiguation note in description: "NOT raw quoter activity — for timestamps/levels use data.quotertrade"
- [ ] Run YAML validation tests

### Task 3.3: Enrich `catalog/kpi/brokertrade.yaml`
- [ ] Same enrichment pattern as 3.1
- [ ] Unique: fee_method enum values (Per Lot, Per Trade, No Fee, bp of Premium, etc.)
- [ ] Unique: instant_pnl_w_fees complex formula with fee_method branching
- [ ] Unique: market_or_mako_trade, market_mako_multiplier logic
- [ ] Unique: tv_offset_slippage formula and per-interval variants
- [ ] Unique: uuid, revision, note, aggressor, broker, source fields from proto
- [ ] Run YAML validation tests

### Task 3.4: Enrich `catalog/kpi/clicktrade.yaml`
- [ ] Same enrichment pattern as 3.1
- [ ] Unique: transaction_id, order_id from proto
- [ ] Unique: algorithm filtering rules (VTM_TAKEOUT, CLICK_ORDER, etc.)
- [ ] Unique: position_type exclusions, inst_type_name filter
- [ ] Unique: bitmask fields from proto (transactionProperties, tagBook, tagExchange, tagUser)
- [ ] Run YAML validation tests

### Task 3.5: Enrich `catalog/kpi/otoswing.yaml`
- [ ] Same enrichment pattern as 3.1
- [ ] Unique: swing_edge, swing_mid_tv formula
- [ ] Unique: routed_price, routed_size, avg_trade_price
- [ ] Unique: all fired_at variants (instant_edge_fired_at, delta/roll/vol/other_slippage_fired_at)
- [ ] Unique: takeoutCategory, takeoutType, baseOrderEventType from proto
- [ ] Unique: latency stats fields from proto
- [ ] Unique: fanout tracking fields
- [ ] Run YAML validation tests

---

## Phase 4: YAML Merge — Data Tables & Cross-Cutting (Estimated: 4 tasks)

### Task 4.1: Enrich Data Table YAMLs
- [ ] `catalog/data/theodata.yaml` — enrich from Theoreticals.proto (tv, delta, gamma, vega, theta, rho, vol, forward, tte, etc.)
- [ ] `catalog/data/marketdepth.yaml` — enrich from MarketDepth.proto (bids/asks levels, impliedBid/Ask, marketState enum)
- [ ] `catalog/data/quotertrade.yaml` — enrich from QuoterTrade.proto, add disambiguation vs kpi.quotertrade
- [ ] `catalog/data/markettrade.yaml` — enrich from MarketTrade.proto
- [ ] `catalog/data/clicktrade.yaml` — enrich from PosData.proto bitmask definitions
- [ ] `catalog/data/swingdata.yaml` — enrich from SwingData.proto (takeout reasons, latency, fanout)
- [ ] Run YAML validation tests

### Task 4.2: Enrich Dataset-Level YAMLs
- [ ] `catalog/kpi/_dataset.yaml` — add enum reference section (trade_side, fee_method, algorithm, portfolio values), interval expansion docs, combo aggregation rules
- [ ] `catalog/data/_dataset.yaml` — add enum reference section (MarketState, CurrencyCode, ExchCode, PositionType), proto source references

### Task 4.3: Enrich `catalog/_routing.yaml`
- [ ] Incorporate full disambiguation table from kpi-findings
- [ ] Add proto-based field presence differences per table
- [ ] Add data pipeline flow documentation (ingest → normalize → enrich → calculate → aggregate)
- [ ] Add interval expansion reference (which intervals exist, which metrics they expand)

### Task 4.4: Update `catalog_loader.py` (if needed)
- [ ] Ensure new optional fields (formula, business_rules, source, deprecated) don't break loading
- [ ] If `validate_table_yaml()` has strict field checking, update to accept new fields
- [ ] Run all existing tests to confirm backward compatibility

---

## Phase 5: Verification Loop — Triple Check (Estimated: 3 tasks)

### Task 5.1: Completeness Audit (Check 1)
- [ ] For each source file, produce a line-by-line extraction checklist
- [ ] Count: columns in kpi-findings → columns enriched in YAML. Must be 100%
- [ ] Count: proto fields in proto-findings → fields enriched in YAML. Must be 100%
- [ ] Count: formulas in kpi-findings → formula fields in YAML. Must be 100%
- [ ] Count: enum values in both sources → example_values in YAML. Must be 100%
- [ ] Write audit results to `catalog/enrichment/audit_completeness.md`

### Task 5.2: Cross-Reference Validation (Check 2)
- [ ] For columns that appear in BOTH kpi-findings AND proto-findings: verify descriptions are consistent
- [ ] For enums in proto-findings: verify they appear in correct YAML example_values
- [ ] For formulas in kpi-findings: verify YAML formula field matches verbatim
- [ ] For deprecated fields in proto-findings: verify YAML deprecated field is set
- [ ] Flag any discrepancies for resolution
- [ ] Write audit results to `catalog/enrichment/audit_cross_reference.md`

### Task 5.3: Embedding Readiness Audit (Check 3)
- [ ] Scan all enriched YAML descriptions: each must be 30-80 words
- [ ] Verify synonyms appear inline in description text (not just in synonyms field)
- [ ] Verify all categorical columns have example_values
- [ ] Verify no description simply restates the column name
- [ ] Verify all descriptions include units where applicable (bps, lots, USD, etc.)
- [ ] Write audit results to `catalog/enrichment/audit_embedding_readiness.md`

---

## Phase 6: Embedding Re-Population (Estimated: 3 tasks)

### Task 6.1: Update Schema Embedding Descriptions
- [ ] Update hardcoded descriptions in `scripts/run_embeddings.py` to match enriched YAML
- [ ] Add `title` field to `schema_embeddings` table schema (ALTER TABLE or recreate)
- [ ] Update `ML.GENERATE_EMBEDDING` call to include title column
- [ ] Update `_SCHEMA_SEARCH_SQL` in `tools/vector_search.py` if schema changed

### Task 6.2: Re-Populate Column Embeddings
- [ ] Run `python scripts/populate_embeddings.py` to re-populate from enriched YAML
- [ ] Verify row counts match expected (should increase if new columns were added)
- [ ] Verify embeddings generated (no rows with empty arrays)

### Task 6.3: Re-Embed Schema Embeddings
- [ ] Run `python scripts/run_embeddings.py --step populate-schema --step generate-embeddings`
- [ ] Verify all rows have non-empty embedding arrays
- [ ] Run test searches: `--step test-search` with synonym-heavy queries
- [ ] Compare search results before/after enrichment for quality improvement

---

## Phase 7: New Few-Shot Examples (Estimated: 2 tasks)

### Task 7.1: Write New Examples
- [ ] Add to `examples/kpi_examples.yaml`: slippage interval queries, fee-adjusted PnL, combo queries, fired-at metrics, mark-to-mid PnL
- [ ] Add to `examples/data_examples.yaml`: greeks queries using proto field names, market depth level queries, swing takeout reason queries
- [ ] Add to `examples/routing_examples.yaml`: new disambiguation cases from kpi-findings
- [ ] Each example: question, sql, tables_used, dataset, complexity, routing_signal
- [ ] Validate YAML structure
- [ ] Total: 15-25 new examples

### Task 7.2: Populate New Examples to BigQuery
- [ ] Run `python scripts/populate_embeddings.py` to MERGE new examples into query_memory
- [ ] Verify new rows appeared with embeddings
- [ ] Test few-shot retrieval with sample questions matching new examples

---

## Phase 8: Final Validation (Estimated: 2 tasks)

### Task 8.1: Run Full Test Suite
- [ ] `pytest tests/` — all existing tests must pass
- [ ] Specifically: `test_yaml_catalog.py`, `test_catalog_loader.py`, `test_metadata_loader.py`
- [ ] If any tests fail due to new YAML fields, fix the tests (new fields are optional/additive)

### Task 8.2: End-to-End Embedding Quality Check
- [ ] Run 10 sample questions through `vector_search_tables` and verify table routing accuracy
- [ ] Run 10 sample questions through `fetch_few_shot_examples` and verify example relevance
- [ ] Document results in `catalog/enrichment/embedding_quality_report.md`
- [ ] Compare to pre-enrichment baseline if available

---

## Execution Strategy

### Parallelism
- Phase 1 tasks (1.1-1.5) can run in parallel (independent source sections)
- Phase 3 tasks (3.1-3.5) can run in parallel (independent table files)
- Phase 4.1 subtasks can run in parallel (independent data table files)
- Phase 5 tasks (5.1-5.3) can run in parallel (independent audit dimensions)

### Sequential Dependencies
```
Phase 1 (Extract) → Phase 2 (Gap Analysis) → Phase 3+4 (Merge) → Phase 5 (Verify) → Phase 6 (Re-embed) → Phase 7 (Examples) → Phase 8 (Final)
```

### Risk Mitigation
- **Context window overflow**: Process one table at a time, use parallel agents for independent tables
- **Information loss**: Intermediate extraction files serve as audit trail
- **YAML breakage**: Run validation tests after each table merge
- **Embedding regression**: Compare search results before/after enrichment

### Files Created/Modified

**Created (intermediate, can be cleaned up after):**
- `catalog/enrichment/kpi_markettrade_extracted.yaml`
- `catalog/enrichment/kpi_quotertrade_extracted.yaml`
- `catalog/enrichment/kpi_brokertrade_extracted.yaml`
- `catalog/enrichment/kpi_clicktrade_extracted.yaml`
- `catalog/enrichment/kpi_otoswing_extracted.yaml`
- `catalog/enrichment/kpi_cross_cutting_extracted.yaml`
- `catalog/enrichment/proto_kpi_extracted.yaml`
- `catalog/enrichment/proto_data_extracted.yaml`
- `catalog/enrichment/extraction_summary.md`
- `catalog/enrichment/gap_report.md`
- `catalog/enrichment/new_examples_candidates.md`
- `catalog/enrichment/audit_completeness.md`
- `catalog/enrichment/audit_cross_reference.md`
- `catalog/enrichment/audit_embedding_readiness.md`
- `catalog/enrichment/embedding_quality_report.md`

**Modified:**
- `catalog/kpi/markettrade.yaml`
- `catalog/kpi/quotertrade.yaml`
- `catalog/kpi/brokertrade.yaml`
- `catalog/kpi/clicktrade.yaml`
- `catalog/kpi/otoswing.yaml`
- `catalog/kpi/_dataset.yaml`
- `catalog/data/theodata.yaml`
- `catalog/data/marketdepth.yaml`
- `catalog/data/quotertrade.yaml`
- `catalog/data/markettrade.yaml`
- `catalog/data/clicktrade.yaml`
- `catalog/data/swingdata.yaml`
- `catalog/data/_dataset.yaml`
- `catalog/_routing.yaml`
- `examples/kpi_examples.yaml`
- `examples/data_examples.yaml`
- `examples/routing_examples.yaml`
- `scripts/run_embeddings.py` (schema descriptions + title field)
- `scripts/populate_embeddings.py` (if schema changes needed)
- `nl2sql_agent/catalog_loader.py` (if validation needs updating)
