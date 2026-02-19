# Track 06: Metadata Enrichment — Specification

## Overview

Systematically extract all business knowledge from two comprehensive source files — `kpi-findings` (KPI pipeline documentation, ~81KB) and `proto-findings` (protobuf schema definitions, ~29KB) — and merge it into the existing two-layer metadata system (YAML catalog + BigQuery vector embeddings). The goal is to leave zero extractable information on the table.

### Why This Matters

Research consistently shows that **column descriptions are the single highest-impact metadata field for NL2SQL accuracy** (+6.7pp on GPT-4o, +20% when columns are uninformative — "Synthetic SQL Column Descriptions", 2024). The BIRD leaderboard #1 system achieved a 10.28pp gap over competitors by fusing multiple metadata sources. Our current YAML catalog has descriptions for every column, but many are thin (restating the column name) and lack the business context, formulas, example values, and domain-specific synonyms that are present in the source files.

## Source Files

### `kpi-findings` (~81KB, 1000 lines)
Contains for all 5 KPI tables (markettrade, quotertrade, brokertrade, clicktrade, otoswing):
- **200+ columns per table** with interval expansion (9 intervals x multiple slippage metrics)
- **Exact formulas** for computed fields (instant_edge, instant_pnl, delta_slippage, etc.)
- **Enum values** for trade_side, fee_method, algorithm, portfolio
- **Cross-cutting business rules**: combo logic, WHITE portfolio special handling, data pipeline flow
- **Disambiguation guidance**: when to use which table
- **Source SQL files**: which pipeline scripts produce each column

### `proto-findings` (~29KB)
Contains for 7 tables (5 KPI + theodata + marketdepth + swingdata):
- **250+ protobuf field definitions** with types and comments
- **Nested message types**: VtCommon, FillInfo, OraTheoreticals, MarketLevel
- **Enum definitions**: BuySell (BUY=66, SELL=83), ExchCode, PositionType, CurrencyCode, MarketState
- **Bitmask definitions**: TransactionProperty, TransactionTag (clicktrade lifecycle tracking)
- **Deprecated field warnings**: BrokerTrade.key, QuoterTrade.lastUpdateTv
- **Git-traced additions**: rawMinEdge, baseSize (with commit hashes)

## What Gets Enriched

### Layer 1: YAML Catalog (15 files)

#### Per-Column Enrichment
For every column in every table YAML, merge from source files:

| Field | Current State | Target State |
|-------|--------------|--------------|
| `description` | Often thin ("Reference theoretical value") | Verbose, business-context-rich (30-80 words). Include what it means to a trader, when/why they'd query it, units, and how it's computed. Research shows verbose descriptions outperform concise ones for embeddings. |
| `synonyms` | Partially populated | Complete. Include all trader jargon, abbreviations, and alternative names found in source files. Synonyms must appear in the description text too (for embedding quality). |
| `example_values` | Sparse | Populated for all categorical/enum columns. Include enum values from proto definitions (BuySell, fee_method, algorithm, portfolio, etc.) |
| `formula` | Missing | New field. Exact computation formula from kpi-findings for all derived columns (instant_edge, instant_pnl, all slippage metrics). Helps the LLM understand column semantics and write correct aggregations. |
| `business_rules` | Missing | New field. Constraints, gotchas, NULL conditions, special handling (e.g., WHITE portfolio logic, combo aggregation rules). |
| `source` | Missing | New field. Proto message and field name (e.g., `MarketTrade.proto::tradePrice`). Useful for debugging and traceability. |
| `deprecated` | Missing | New field (boolean). Flag deprecated proto fields (BrokerTrade.key, QuoterTrade.lastUpdateTv). |

#### Per-Table Enrichment
- Expanded `description` with routing signals from kpi-findings disambiguation guide
- `pipeline_flow` field: which SQL scripts produce this table (normalized → raw_pivoted_join → calculations → combos)
- `unique_columns` field: columns that exist only in this table (not shared across KPI tables)

#### Dataset-Level Enrichment
- Expanded routing patterns with kpi-findings disambiguation
- Add enum reference section with all resolved enum values
- Add interval expansion documentation (9 intraday + multiday intervals)

#### Cross-Dataset (`_routing.yaml`)
- Incorporate full disambiguation table from kpi-findings
- Add proto-based field presence differences (e.g., brokertrade has `uuid`/`revision`/`fee_method`, others don't)

### Layer 2: BigQuery Vector Embeddings

#### `schema_embeddings` (~17 rows)
- **Re-embed all rows** with enriched descriptions that now include synonyms, routing signals, and business context inline
- **Add `title` field** per Google's recommendation for `RETRIEVAL_DOCUMENT` task type (e.g., `"kpi.markettrade - Exchange/Market Trade KPIs"`)
- Update `run_embeddings.py` to use the title field in `ML.GENERATE_EMBEDDING`

#### `column_embeddings` (~1000 rows)
- **Re-populate** from enriched YAML catalog using `populate_embeddings.py`
- Descriptions now contain synonyms, formulas, and business rules inline — better embedding quality

#### `query_memory` (30+ rows)
- **Add new few-shot examples** derived from the formula documentation (e.g., questions about slippage at different intervals, fee-adjusted PnL, combo aggregation)
- Target: 15-25 additional examples covering patterns not yet in the corpus
- Ensure diversity: each table covered, each complexity level, each routing edge case

### New: Embedding Description Composition

When composing the text that gets embedded (for `schema_embeddings` and `column_embeddings`), follow this research-backed pattern:

```
Title: {dataset}.{table} - {human_label}
Content: {rich_description}. Traders may refer to this as {synonyms}.
{business_rules}. Example values: {example_values}.
```

This ensures synonyms and business terms are captured in the embedding vector, not just in a separate YAML field the embedding model never sees.

## Extraction Methodology

### Phase 1: Structured Extraction (file → intermediate YAML)

Process each source file independently. For each, produce an intermediate extraction file:

```
catalog/enrichment/
├── kpi_findings_extracted.yaml    # All facts from kpi-findings, organized by table/column
└── proto_findings_extracted.yaml  # All facts from proto-findings, organized by table/column
```

These files are the audit trail — every fact from the source, structured but not yet merged.

**Extraction rules:**
- Every column mentioned in the source must appear in the extraction
- Every formula must be captured verbatim
- Every enum value must be listed
- Every business rule / special case must be recorded
- Every synonym / alternative name must be captured
- Cross-cutting concepts (combo logic, interval expansion, pipeline flow) go in a `_cross_cutting` section

### Phase 2: Gap Analysis (extraction → diff report)

Compare extracted facts against existing YAML catalog:

```
catalog/enrichment/
└── gap_report.md    # What's new, what conflicts, what's still missing
```

Categories:
- **New columns**: In source but not in YAML (add)
- **Enriched columns**: In YAML with thin descriptions (update)
- **Conflicting columns**: Description differs between source and YAML (flag for review)
- **Unchanged columns**: Already rich enough (skip)
- **New cross-cutting facts**: Business rules, enums, pipeline info (add to dataset/routing YAML)

### Phase 3: Merge (extraction + existing → enriched YAML)

Apply enrichments to the actual YAML catalog files. One table at a time, with verification after each.

**Merge rules:**
- Never delete existing information — only add or expand
- Preserve existing synonyms, add new ones
- Preserve existing descriptions, append business context
- New fields (formula, business_rules, source, deprecated) are additive
- `{project}` placeholders preserved in all FQNs

### Phase 4: Verification Loop (triple-check)

#### Check 1: Completeness Audit
For each source file, produce a column-by-column checklist:
- [ ] Column X from kpi-findings → mapped to YAML? → enriched?
- [ ] Formula Y → captured in `formula` field?
- [ ] Enum Z → captured in `example_values`?

Count: source columns extracted vs. YAML columns enriched. Must match.

#### Check 2: Cross-Reference Validation
- Every column in proto-findings that also appears in kpi-findings: do the descriptions agree?
- Every enum in proto-findings: does it appear in the correct YAML `example_values`?
- Every formula in kpi-findings: does the `formula` field in YAML match verbatim?

#### Check 3: Embedding Readiness Audit
- Every enriched description is 30-80 words (not too short for embedding quality, not too long for token limits)
- Every description includes synonyms inline (not just in the separate `synonyms` field)
- Every categorical column has `example_values` populated
- No description simply restates the column name

### Phase 5: Embedding Re-Population

After YAML is finalized and verified:
1. Run `populate_embeddings.py` to re-populate `column_embeddings` from enriched YAML
2. Update `schema_embeddings` descriptions in `run_embeddings.py` to match enriched table descriptions
3. Run `run_embeddings.py --step populate-schema --step generate-embeddings` to re-embed
4. Run test searches to verify embedding quality improvement

### Phase 6: New Few-Shot Examples

Add 15-25 new Q→SQL examples to `examples/*.yaml` covering:
- Slippage queries at specific intervals ("what was the 5-minute delta slippage?")
- Fee-adjusted PnL for brokertrade
- Combo aggregation patterns
- Fired-at metrics for otoswing
- Mark-to-mid PnL for quotertrade
- Questions using trader synonyms that map to specific columns
- Cross-table UNION ALL patterns

## Non-Functional Requirements

### NFR-1: No Information Loss
Every extractable fact from both source files must be traceable to a YAML field. The intermediate extraction files serve as the audit trail.

### NFR-2: Embedding Quality
Descriptions must follow the research-backed pattern: verbose, include synonyms inline, include business context and example values. The `title` field should be used for `RETRIEVAL_DOCUMENT` embeddings per Google's recommendation.

### NFR-3: Backward Compatibility
- Existing YAML structure preserved (no field renames or removals)
- New fields are additive (formula, business_rules, source, deprecated)
- `{project}` parameterization preserved
- All existing tests must still pass
- `catalog_loader.py` must handle new optional fields gracefully

### NFR-4: Idempotent Embedding Scripts
- `populate_embeddings.py` already uses MERGE — re-running is safe
- `run_embeddings.py` already uses CREATE OR REPLACE — re-running is safe
- No duplicate rows on re-run

## Acceptance Criteria

- [ ] `catalog/enrichment/kpi_findings_extracted.yaml` exists with all facts from kpi-findings
- [ ] `catalog/enrichment/proto_findings_extracted.yaml` exists with all facts from proto-findings
- [ ] `catalog/enrichment/gap_report.md` exists with diff analysis
- [ ] All 5 KPI table YAMLs enriched with descriptions, synonyms, example_values, formula, business_rules
- [ ] All data table YAMLs enriched where proto-findings provides new information
- [ ] `_dataset.yaml` files enriched with enum references and interval documentation
- [ ] `_routing.yaml` enriched with full disambiguation from kpi-findings
- [ ] Completeness audit: 100% of source columns mapped and enriched
- [ ] Cross-reference validation: no conflicts between kpi-findings and proto-findings
- [ ] Embedding readiness audit: all descriptions 30-80 words with inline synonyms
- [ ] `schema_embeddings` descriptions updated and re-embedded (with title field)
- [ ] `column_embeddings` re-populated from enriched YAML
- [ ] 15-25 new few-shot examples added to `examples/*.yaml`
- [ ] All existing tests pass
- [ ] Test vector searches show improved relevance for synonym-heavy queries

## Out of Scope

- Modifying tool code (vector_search.py, metadata_loader.py, etc.) — catalog changes only
- Adding new tables to the catalog (only enriching existing 13)
- Production deployment of re-embedded vectors
- Column-level vector search tool implementation (Track 03 note)
- Data profiling (NULL rates, distinct counts) — would require BQ access to live data

## Research References

- "Synthetic SQL Column Descriptions" (2024): Verbose descriptions outperform concise. +6.7pp accuracy.
- "Automatic Metadata Extraction for Text-to-SQL" (2025): Fused metadata = BIRD #1. 10.28pp gap.
- Pinterest Engineering (2024): Table documentation improved search hit rate 40% → 90%.
- CHESS (2024): Hierarchical retrieval with full catalog metadata. BIRD #1 at submission.
- Google Vertex AI docs: `title` field for `RETRIEVAL_DOCUMENT` improves embedding quality.
- DAIL-SQL (2024): Dual-similarity few-shot selection (question + SQL skeleton) = 86.2% Spider.
