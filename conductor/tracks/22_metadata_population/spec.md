# Spec: Metadata Population — Remaining Gaps

## Overview

Completes metadata population started in Track 24. Track 24 achieved 97.1% core enrichment (category, aggregation, filterable, formula, related_columns). This track fills the remaining gaps: BQ data profiling for `example_values`, `source` field population from proto definitions, and verification of existing `business_rules` and `synonyms` against source repos.

**Dependencies:** Track 24 (core enrichment complete), Track 23 (structural indexes available)

## Current State (Post Track 24)

| Field | Populated | Total | Coverage |
|-------|-----------|-------|----------|
| category | 4,631 | 4,631 | 100% |
| typical_aggregation | 2,782 | 2,782 | 100% (measures) |
| filterable | 1,562 | 1,562 | 100% (applicable) |
| formula | 975 | 975 | 100% (computed) |
| related_columns | 969 | 975 | 99.4% |
| example_values | 68 | 4,631 | 1.5% |
| comprehensive | 0 | 4,631 | 0% |
| synonyms | 2,262 | 4,631 | 48.8% |
| business_rules | 428 | 4,631 | 9.2% |
| source | 1,044 | 4,631 | 22.5% |

## Functional Requirements

### FR-1: BQ Data Profiling for example_values

**Target:** All STRING/categorical columns (dimension, identifier) across 12 tables
**Script:** `scripts/profile_columns.py`

For each column with `category` in (dimension, identifier):
- Query BQ: `APPROX_COUNT_DISTINCT` for cardinality, `APPROX_TOP_COUNT` for top values
- Apply tiered logic:

| Cardinality | Treatment | comprehensive |
|-------------|-----------|---------------|
| < 25 | Store ALL values | true |
| 25–250 | Store top 10 | false |
| 250+ | Skip | — |

- Update YAML with `example_values` list and `comprehensive` flag
- Partition filter: `WHERE trade_date = "2026-02-17"` (only available data)
- Batch queries per table to minimize BQ API calls

### FR-2: Source Field Population

**Input:** `metadata/proto_fields.yaml` (612 fields across 45 proto messages)
**Target:** All data-layer columns + KPI columns with proto origins

For each column in data table YAMLs:
- Look up in proto_fields.yaml by column name
- Set `source` to proto message + field path (e.g., `MarketTrade.trade_price`)
- For KPI columns: set `source` to KPI computation reference

### FR-3: Business Rules Verification

**Input:** `repos/kpi/AGENTS.md`, `metadata/kpi_computations.yaml`
**Target:** 428 columns with existing `business_rules` fields

Launch sub-agents to read KPI repo AGENTS.md and verify each business_rule is accurate.
Flag and fix any hallucinated business rules.

### FR-4: Synonyms Verification

**Input:** Source repo AGENTS.md cards, proto_fields.yaml
**Target:** 2,262 columns with existing `synonyms` fields

Launch sub-agents to verify synonyms against source repo documentation.
Flag synonyms that don't appear in any source context.

## Non-Functional Requirements

- All scripts are idempotent and deterministic
- BQ profiling uses partition filters (cost control)
- YAML modifications preserve formatting (surgical editing)
- All changes validated by catalog/schema.py

## Acceptance Criteria

1. example_values populated for all dim/id columns with cardinality <250
2. comprehensive flag set correctly based on cardinality tier
3. source field populated for all columns with proto origins
4. business_rules verified against KPI repo — hallucinations fixed
5. synonyms verified against source repos — hallucinations fixed
6. All modified YAMLs pass catalog/schema.py validation
7. Full test suite passes

## Out of Scope

- category/aggregation/filterable/formula/related_columns (done in Track 24)
- LLM-assisted gap filling for descriptions
- Embedding text updates (Track 19 scope)
