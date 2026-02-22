# Track Brief: Metadata Population, Verification & Data Profiling

> Deferred from Track 18 (YAML Schema Enrichment). Track 18 defined the schema and validation infrastructure; this track populates the fields with actual values and verifies all existing metadata.

## What This Track Delivers

Populate the enrichment fields defined in `catalog/schema.py` (category, typical_aggregation, filterable, example_values, comprehensive, formula, related_columns) across all 4,631 columns in 12 table YAMLs. Verify accuracy of ALL existing metadata (descriptions, synonyms, example_values, formulas, business_rules) against source code and live BQ data.

## Prerequisites

**This track requires access to source repositories that the NL2SQL agent codebase does not contain:**

1. **C++ trading repo** — Required to verify `formula` fields on computed KPI columns. The formulas describe how columns like `instant_edge`, `tv`, `delta_adjusted_*` are derived. The actual computation logic lives in C++ pricing/KPI pipeline code, not in BigQuery or Python.

2. **KPI pipeline repo** — Required to verify `business_rules` fields (e.g., "WHITE portfolio has special KPI handling: internal deflection adjustments are removed from TV calculations"). These rules are implemented in the KPI computation pipeline.

Without these, formula and business_rules fields can only be populated based on the existing YAML descriptions (which were themselves LLM-generated from proto files and may contain hallucinations).

## Source Requirements

### Part A: Heuristic Enrichment Script
- `scripts/enrich_columns.py` — auto-assigns `category` and `filterable` based on:
  - Type-based rules: TIMESTAMP/DATE → `time`, BOOLEAN → `dimension`
  - Name-pattern rules: `*_id/*_hash/*_key` → `identifier`, `*_pnl/*_edge` → `measure`
  - Interval-expanded patterns: `*_1s/*_5m/*_1h` → inherit from family
- Uses `ruamel.yaml` for round-trip-safe YAML editing (preserves formatting)
- Reports: assigned count, unassigned count, confidence breakdown per table

### Part B: BQ Data Profiling Script
- `scripts/profile_columns.py` — queries BQ to auto-populate `example_values`
- Tiered cardinality thresholds (from Track 18 design decisions):

| Cardinality | Treatment | `comprehensive` | In Embedding? |
|-------------|-----------|-----------------|---------------|
| < 25 | Store ALL values | `true` | Yes (all) |
| 25–250 | Store top 10 | `false` | Yes (top 5) |
| 250+ | Skip or top 5 | N/A | No |

- Uses `APPROX_TOP_COUNT` + `APPROX_COUNT_DISTINCT` per STRING column
- Outputs JSON with profiling results, flags boundary cases for human review

### Part C: Tiered Population Strategy
1. **Tier 1 (Human-curated, ~120-150 columns):** Columns from examples, shared_columns, enum_reference. Full treatment: category, formula, related_columns, typical_aggregation, example_values. Requires domain expert + C++ repo access.
2. **Tier 2 (Heuristic + BQ profiling, ~4,000 columns):** Pattern-matching auto-assignment + BQ profiling. No human review; Pydantic validation is the gate.
3. **Tier 3 (LLM-assisted, ~1,300 columns):** LLM assigns category, typical_aggregation, filterable. Does NOT generate formula or related_columns. Pydantic validated + 10-15% spot-check.

### Part D: Existing Metadata Verification
- Audit ALL existing `description` fields against C++ source and BQ schemas
- Verify `synonyms` lists are accurate (some were LLM-generated in Track 02/06)
- Verify `example_values` on columns that already have them (e.g., portfolio, algo)
- Verify `formula` fields on KPI columns against actual C++ computation logic
- Verify `business_rules` against KPI pipeline implementation
- Cross-reference `source` fields against proto definitions
- Flag and fix any hallucinated content

## Interface Contracts
- **Owned:**
  - `populated_metadata` — all 4,631 columns with category, typical_aggregation, filterable populated
  - `data_profiling_pipeline` — scripts/profile_columns.py for BQ cardinality analysis
  - `heuristic_enrichment_script` — scripts/enrich_columns.py for pattern-based auto-assignment
- **Consumed:**
  - `enriched_yaml_schema` — Track 18's Pydantic models and validation infrastructure
  - `yaml_catalog` — existing catalog files in catalog/{kpi,data}/*.yaml
  - `bigquery` — live BQ access for profiling

## Key Design Decisions (inherited from Track 18)
1. **4-category taxonomy:** dimension (GROUP BY/WHERE), measure (aggregate), time (date filter/ORDER BY), identifier (JOIN/dedup, never aggregate)
2. **Tiered cardinality:** <25 comprehensive, 25-250 illustrative, 250+ skip
3. **Pydantic as gate:** CI validates all enriched fields; runtime loader unchanged
4. **No YAML bloat:** example_values ≤25, related_columns ≤5, formula one-line

## Test Strategy
- **Unit:** Heuristic rules match expected columns
- **Unit:** Cardinality tier classification logic
- **Integration:** profile_columns.py against dev BQ (marker: `pytest -m integration`)
- **CI:** test_catalog_validation.py enforces Pydantic on every PR
- **Coverage report:** % columns with category, % with example_values, % with typical_aggregation

## Complexity: Large
## Estimated Phases: 5
