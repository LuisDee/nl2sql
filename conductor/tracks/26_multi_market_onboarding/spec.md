# Track 26: Multi-Market Table Onboarding

## Context

The NL2SQL agent currently supports only OMX market data (12 tables across kpi/data layers). We have preview data + schemas for **9 markets** (arb, asx, brazil, eurex, euronext, ice, korea, nse, omx) covering **44 unique table types** and **250 table instances** total.

**Goal:** Onboard all market tables — copy schemas to `schemas/`, generate skeleton YAMLs, run enrichment pipeline, populate embeddings — so the agent can answer questions about ANY market's data.

## Scope

### In Scope
- Copy all 250 schema JSONs from preview zip into `schemas/{market}/`
- Generate skeleton catalog YAMLs for all new tables via `generate_skeleton.py`
- Update `table_registry.py` to support multi-market (per-exchange table lists)
- Run deterministic enrichment (categories, aggregation) on all new tables
- Port OMX metadata (descriptions, synonyms, business_rules) to overlapping tables in other markets
- Investigate and document new table types not in current OMX catalog
- Update `_dataset.yaml` files for each market layer
- Populate BQ `column_embeddings` and `schema_embeddings` for all markets
- Run coverage gate and iterate until all tables meet thresholds

### Out of Scope
- KPI layer for non-OMX markets (no KPI pipeline exists yet for other exchanges)
- Live BQ data loading (preview data only — no production BQ access needed)
- Profile columns (requires live BQ with actual data)
- Changes to the agent's runtime query execution (just metadata)

## Markets & Table Inventory

| Market | Tables | New Tables (not in OMX data) |
|--------|--------|------------------------------|
| omx_data | 26 | 19 new (only 7 currently registered) |
| arb_data | 23 | Same as OMX minus swingdata, plus brokertrade_data |
| asx_data | 26 | +oroswingdata, marketdataext, marketstate* |
| brazil_data | 39 | +boshorder, boshorderupdate, instrumentmetadata, multilevel_data, orderposdata, order*event, pcaporder, basevaldata, feedarbitrage |
| eurex_data | 30 | +oroswingdata, pcaporder, pcapquote, marketdataext |
| euronext_data | 31 | +order*event, pcaporder, pcapquote, marketdataext |
| ice_data | 28 | +oroswingdata, quoterpull |
| korea_data | 27 | +basevaldata, feedarbitrage, pcaporder |
| nse_data | 20 | +kpibaseval, makotonuvamapcapdata, nuvamatonsepcapdata |

## Table Categories

### Category A: OMX Overlap — Port Existing Metadata (7 tables × 9 markets)
markettrade, quotertrade, clicktrade, theodata, marketdata, marketdepth, swingdata
- These already have rich YAML catalogs for OMX
- Copy descriptions/synonyms/categories/business_rules to other markets
- Schemas are identical across markets (same proto sources)

### Category B: Registry Gaps — Already Documented (4 tables)
tradedata, oroswingdata, marketdataext, brokertrade(data)
- Proto docs exist in repos/cpp/AGENTS.md and repos/data-loader/AGENTS.md
- Can derive metadata from existing related tables

### Category C: Common New Tables — Present in Most Markets (12 tables)
algostartup, brokertrade_2, eodpostradessnap, instrumentreceival, instruments,
livestats, mako_underlyingtrade, marketdata_trades, marketdepth_snapshot,
marketstate, marketstate_open_close_times, norm_strike_voldata, posdata,
quoterdelete, quoterupdate, streammetadatamessage, theodata_snapshot
- Need schema analysis + description writing
- Some can borrow from related tables (theodata_snapshot←theodata, quoterdelete←quotertrade)

### Category D: Market-Specific Tables (11 tables)
pcaporder, pcapquote, quoterpull, basevaldata, feedarbitrage,
boshorder, boshorderupdate, instrumentmetadata, multilevel_data,
orderposdata, order*events, kpibaseval, mako/nuvama pcap tables

## Functional Requirements

- **FR-1:** All 44 unique table types have a skeleton YAML in `catalog/{market}/`
- **FR-2:** All tables with OMX equivalents have ported descriptions, synonyms, categories
- **FR-3:** Deterministic enrichment (categories, aggregation, filterable) runs on all tables
- **FR-4:** `table_registry.py` supports multi-market table lists
- **FR-5:** Each market has a `_dataset.yaml` with routing rules
- **FR-6:** Coverage gate passes for all tables (category≥95%, description=100%)
- **FR-7:** All new schemas committed to `schemas/{market}/`
- **FR-8:** Embeddings populated for all markets' column metadata

## Implementation Phases

### Phase 1: Infrastructure — Schema Copy + Registry Extension
1. Copy all schema JSONs from preview zip to `schemas/{market}/`
2. Extend `table_registry.py` with per-market ALL_TABLES
3. Extend config.py to support multiple exchanges (dataset_for_exchange helper)
4. Create `catalog/{market}/` directories + `_dataset.yaml` stubs

### Phase 2: OMX Gap Closure — Onboard All 26 OMX Tables
1. Generate skeletons for the 19 OMX tables not yet in catalog
2. Run enrichment pipeline on all 26 OMX tables
3. Write descriptions for new tables by analyzing preview data + schema
4. Iterate until coverage gate passes

### Phase 3: Cross-Market Port — Copy OMX Metadata to 8 Other Markets
1. For each overlapping table, copy OMX YAML → market YAML
2. Replace dataset placeholders with market-specific ones
3. Run enrichment pipeline on all markets
4. Verify with coverage gate

### Phase 4: New Table Documentation — Market-Specific Tables
1. Analyze preview data for tables unique to specific markets
2. Write descriptions based on column names, types, and data patterns
3. Run enrichment pipeline
4. Coverage gate verification

### Phase 5: Embeddings + Final Verification
1. Run `populate_embeddings.py` for all markets
2. Run `check_coverage.py --json` for full report
3. Final commit with all metadata

## Acceptance Criteria
- [ ] All 250 table instances have catalog YAMLs
- [ ] Coverage gate passes for all tables (category≥95%, description=100%)
- [ ] All schemas committed to schemas/ directory
- [ ] table_registry.py supports all 9 markets
- [ ] All tests pass (existing + new)
