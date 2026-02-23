# Spec: Metadata Extraction from Source Repos

## Overview

Uses Track 23's structural indexes (`kpi_computations.yaml`, `proto_fields.yaml`, `data_loader_transforms.yaml`, `field_lineage.yaml`) and source repo AGENTS.md documentation to populate the YAML catalog's enrichment fields. Each enrichment is source-grounded, not LLM-generated.

**Dependencies:** Track 23 (structural indexes), Track 18 (defined enrichment field schema)

## Current State

- 4,631 columns across 12 tables (5 KPI = 4,074 cols, 7 data = 557 cols)
- Only 8% enriched: 318 `formula` fields (KPI only), 53 `example_values`
- 0% populated: `category`, `typical_aggregation`, `filterable`, `related_columns`, `comprehensive`
- Validation models ready in `catalog/schema.py`
- Source repo AGENTS.md cards available for CPP, data-library, data-loader, KPI repos

## Scope

**In scope (Tier 1+2):**
1. Formula extraction and verification — use `kpi_computations.yaml` to verify existing 318 formulas and fill gaps
2. Category assignment — heuristic rules based on field type, name patterns, proto context
3. Related columns — seed from `field_lineage.yaml` cross-layer relationships
4. Typical aggregation — assign to measure-category columns based on field semantics
5. Filterable flag — mark dimension/identifier columns commonly used in WHERE clauses
6. Description verification — cross-check existing descriptions against proto comments from `proto_fields.yaml`

**Out of scope (defer to Track 22):**
- BQ data profiling for `example_values` population (requires live BQ queries)
- LLM-assisted gap filling for remaining descriptions
- Tier 3 enrichment

## Functional Requirements

### FR-1: Formula Verification & Extraction

**Input:** `metadata/kpi_computations.yaml` (159 formulas across 5 trade types)
**Target:** All KPI table YAML files

For each KPI column that has a formula in `kpi_computations.yaml`:
- If the YAML already has a `formula` field: verify it matches the source. Update if different.
- If the YAML has no `formula` field: add it from the source.
- Formula text should be the SQL expression, not prose.

For intermediate calculations (mid_base_val, delta, raw_delta_mid_bv, etc.):
- Add formula from the `intermediate_calculations` section of kpi_computations.yaml.

**Validation:** Every formula in the YAML must trace back to `kpi_computations.yaml`.

### FR-2: Category Assignment (Heuristic)

**Target:** All 4,631 columns across all 12 tables

Assign `category` using deterministic rules:

| Rule | Category | Examples |
|------|----------|---------|
| Column name contains `timestamp`, `_ns`, `_date` | `time` | event_timestamp_ns, trade_date |
| Column name contains `_hash`, `_id`, `_key`, `_type`, `_name`, `_side` | `identifier` | instrument_hash, algo, trade_aggressor_side |
| Column name matches known dimension patterns (symbol, portfolio, currency, exchange) | `dimension` | symbol, portfolio, currency |
| Proto type is enum (from proto_fields.yaml) | `dimension` | option_type, inst_type |
| Column has `formula` or is numeric and not an identifier | `measure` | instant_edge, trade_price, delta |
| Default for unmatched | `dimension` | (conservative default) |

Rules are applied in priority order. The script must be deterministic and reproducible.

### FR-3: Related Columns

**Input:** `metadata/field_lineage.yaml` (30 lineage entries)
**Target:** Key columns in KPI and data table YAMLs

For each lineage entry:
- The gold-layer column gets `related_columns` pointing to its silver-layer inputs
- Silver-layer columns get `related_columns` pointing to related gold-layer outputs

Cap at 5 related columns per entry (per schema.py constraint).

### FR-4: Typical Aggregation

**Target:** All columns with `category: measure`

Assign `typical_aggregation` using field semantics:
- PnL/edge/slippage columns → `SUM` (additive across trades)
- Price/TV/delta/gamma/vega columns → `AVG` (not additive)
- Size/volume columns → `SUM`
- Ratio/multiplier columns → `AVG`

### FR-5: Filterable Flag

**Target:** All columns with `category: dimension` or `category: identifier`

Mark as `filterable: true` for columns commonly used in WHERE clauses:
- All `identifier` columns (symbol, instrument_hash, algo, trade_side, etc.)
- All `dimension` columns with low-to-medium cardinality (portfolio, currency, exchange, option_type)
- `trade_date` (always filtered on as partition key)

### FR-6: Description Verification

**Input:** `metadata/proto_fields.yaml` (proto comments), source repo AGENTS.md cards
**Target:** All columns with existing descriptions

For each column:
- Cross-check the YAML description against the proto comment (if available) and the source repo documentation
- Flag descriptions that reference column names or concepts that don't exist (hallucination indicators)
- Fix flagged descriptions using source-grounded context

Use sub-agents with source repo context to verify descriptions in batches.

## Implementation Strategy

All enrichment is done by Python scripts that read the structural indexes and modify the YAML files in place. Scripts are:
- Deterministic (same input → same output)
- Idempotent (safe to re-run)
- Validated by `catalog/schema.py` after every modification

Sub-agents with source repo expertise are used for:
- Description verification (can read actual source code for context)
- Formula spot-checking (can verify against KPI repo SQL)

## Acceptance Criteria

1. All KPI columns with formulas in `kpi_computations.yaml` have verified `formula` fields in YAML
2. All 4,631 columns have a `category` field assigned
3. Key columns (from field_lineage.yaml) have `related_columns` populated
4. All `measure` columns have `typical_aggregation` assigned
5. Dimension/identifier columns have `filterable` flag set
6. Existing descriptions verified against source — hallucinated references flagged and fixed
7. All modified YAMLs pass `catalog/schema.py` validation
8. Existence ratio >95%: every entity referenced in enriched YAML exists in BQ schema or structural indexes
