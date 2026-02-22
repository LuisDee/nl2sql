# Plan: Embedding Strategy & Glossary Collection

## Phase 1: Enriched Embedding Text + Payload Columns

### Task 1.1: Update build_embedding_text() with category + example_values (4e30b2a)
- [x] Write tests for new `build_embedding_text()` with category tag, filterable dimension values, and graceful degradation when fields are absent
- [x] Update `build_embedding_text()` in `scripts/populate_embeddings.py` to include `[category]` and selective `example_values`
- [x] Verify tests pass

### Task 1.2: Add payload columns to column_embeddings table + MERGE (2eed875)
- [x] Write tests for `populate_column_embeddings()` payload column handling (category, formula, example_values, related_columns, typical_aggregation, filterable)
- [x] Update `create_embedding_tables()` in `run_embeddings.py` to include 6 nullable payload columns in `column_embeddings` DDL (for fresh setups)
- [x] Add ALTER TABLE migration in `run_embeddings.py` to add payload columns to existing `column_embeddings` table (idempotent — skip if columns already exist)
- [x] Update `populate_column_embeddings()` MERGE in `populate_embeddings.py` to read YAML enrichment fields and populate payload columns
- [x] Verify tests pass

### Task 1.3: Phase 1 checkpoint
- [ ] Run full test suite
- [ ] Manual verification: inspect embedding_text output for sample columns (measure, filterable dimension, unenriched)

---

## Phase 2: Business Glossary YAML + Pydantic Validation

### Task 2.1: Create GlossaryEntrySchema Pydantic model
- [x] Write tests for `GlossaryEntrySchema` validation (required fields: name, definition, synonyms, related_columns; optional: category, sql_pattern)
- [x] Add `GlossaryEntrySchema` and `GlossarySchema` to `catalog/schema.py` (include `synonyms: list[str]` field for embedding text)
- [x] Verify tests pass

### Task 2.2a: Create catalog/glossary.yaml — high-priority concepts (10-15 entries)
- [x] Create `catalog/glossary.yaml` with first batch of high-priority concepts that traders actively ask about:
  - PnL variants: total PnL (SUM instant_pnl), net PnL (SUM instant_pnl_w_fees), mark-to-mid PnL, PnL disambiguation rules
  - Edge metrics: edge concept (spans edge_bps, instant_edge, edge_ticks), good fill (edge_bps > 0 threshold)
  - Slippage decomposition: delta_slippage + vega_slippage + gamma_slippage + theta_slippage + residual, interval naming pattern ({family}_{interval})
  - Trade type taxonomy: combo vs leg (is_parent flag), markettrade as superset, clicktrade/quotertrade/hedgetrade specialisations
  - Timestamp conventions: "the close" (exchange-specific timestamps), trade_date default to CURRENT_DATE()
- [x] Add CI validation test for glossary.yaml against Pydantic model in `tests/test_catalog_validation.py`
- [x] Verify tests pass

### Task 2.2b: Expand glossary.yaml — remaining concepts (10-15 entries)
- [ ] Add second batch of concepts:
  - ATM strike logic: where delta ~ 0.5, moneyness concepts
  - Delta/vol/greeks: delta hedging, implied vol vs realised vol, gamma exposure
  - Exchange-specific concepts: exchange codes (ICE, Eurex, NSE, OMX, KRX), exchange-specific session times
  - Interval expansion: the 9 standard intervals (1s, 5s, 10s, 30s, 1m, 5m, open, close, settle)
  - Combo structures: spreads, strangles, straddles, how aggregation differs at combo vs leg level
  - Named filters as concepts: calls_only, today_only, mako_trades_only
- [ ] Verify tests pass (CI validation covers full glossary)

### Task 2.3: Phase 2 checkpoint
- [ ] Run full test suite
- [ ] Manual verification: review glossary entries for accuracy and completeness

---

## Phase 3: Glossary Population Script + Pipeline Integration

### Task 3.1: Create scripts/populate_glossary.py
- [ ] Write tests for glossary embedding text builder (`{name}: {definition}. Also known as: {synonyms}`) and MERGE SQL generation
- [ ] Create `scripts/populate_glossary.py` (reads glossary.yaml, builds embedding text, MERGEs into glossary_embeddings)
- [ ] Verify tests pass

### Task 3.2: Integrate glossary into run_embeddings.py pipeline
- [ ] Write tests for new pipeline steps
- [ ] Update `create_embedding_tables()` with `glossary_embeddings` table DDL
- [ ] Add `populate-glossary` step to STEPS dict
- [ ] Update `generate_embeddings()` to include ML.GENERATE_EMBEDDING on `glossary_embeddings`
- [ ] Update `create_vector_indexes()` to include `glossary_embeddings` vector index
- [ ] Verify step order: create-tables → populate-columns → populate-glossary → generate-embeddings → create-indexes
- [ ] Verify tests pass

### Task 3.3: Phase 3 checkpoint
- [ ] Run full test suite
- [ ] Manual verification: confirm pipeline step order in --help output

---

## Phase 4: Vector Search CTE + Prompt Integration

### Task 4.1a: Add payload columns to column search CTE arm
- [ ] Write tests for enriched column CTE: results include payload fields (category, formula, example_values, related_columns, typical_aggregation, filterable), results include `'column' AS source` field
- [ ] Update column search arm of `_COLUMN_SEARCH_SQL` in `vector_search.py` to SELECT payload columns and add `'column' AS source`
- [ ] Update `ColumnSearchResult` TypedDict to include payload fields and source key
- [ ] Verify tests pass

### Task 4.1b: Add glossary UNION arm to vector search CTE
- [ ] Write tests for glossary CTE: glossary results returned with `source='glossary'`, per-source top-K ranking (top-30 columns + top-3 glossary)
- [ ] Add glossary UNION arm to `_COLUMN_SEARCH_SQL` with `'glossary' AS source`
- [ ] Verify tests pass

### Task 4.2: Format glossary results in tool output
- [ ] Write tests for `vector_search_columns()` return structure with glossary entries formatted as a dedicated Business Context section (separate from column schema context)
- [ ] Update `vector_search_columns()` to separate glossary rows from column rows and format glossary into `## Business Context` section
- [ ] Verify tests pass

### Task 4.3: Format payload fields in prompt builder
- [ ] Write tests for prompt builder: column context block includes Formula, Aggregation, Values, Related Columns when present in payload
- [ ] Update prompt builder to format payload fields into column context block (e.g., `column_name (type) [category]: description | Formula: ... | Aggregation: SUM | Values: [Call, Put]`)
- [ ] Verify tests pass

### Task 4.4: Phase 4 checkpoint
- [ ] Run full test suite
- [ ] Manual verification: confirm vector_search_columns() output includes column results with payload, glossary Business Context section, and prompt builder formats all payload fields
