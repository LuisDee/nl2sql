# Spec: YAML Schema Enrichment & Data Profiling

## Overview

Enrich the existing YAML catalog (12 tables, 4,631 columns) with structured metadata fields that drive SQL generation behaviour. Add `category`, `typical_aggregation`, `filterable`, `example_values` (with `comprehensive` flag), `formula`, and `related_columns` to column definitions. Build a BQ data profiling pipeline to auto-populate `example_values` using tiered cardinality thresholds. Create Pydantic validation models in `catalog/schema.py` for CI enforcement.

## Functional Requirements

### FR-1: Column Category Taxonomy

Every column gets a `category` field — one of 4 values, each mapping to a SQL generation rule:

| Category | SQL Behaviour | Examples |
|----------|--------------|----------|
| `dimension` | GROUP BY, WHERE | portfolio, symbol, algo, trade_side |
| `measure` | Aggregated (see `typical_aggregation`) | instant_edge, instant_pnl, volume |
| `time` | WHERE date filtering, ORDER BY | trade_date, timestamp, expiry |
| `identifier` | JOINs, deduplication, never aggregated/grouped | instrument_hash, trade_id |

### FR-2: Additional Enrichment Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `category` | enum | Yes (enriched columns) | One of: dimension, measure, time, identifier |
| `typical_aggregation` | string | Measures only | SUM, AVG, WEIGHTED_AVG, COUNT, MIN, MAX |
| `filterable` | bool | No | Whether the column is useful as a WHERE predicate |
| `example_values` | list[str] | Categorical columns | Sample or exhaustive values |
| `comprehensive` | bool | When example_values present | `true` = exhaustive enum, `false` = illustrative samples |
| `formula` | string | Computed columns (Tier 1 only) | SQL/logical derivation of the column |
| `related_columns` | list[str] | Key columns (Tier 1 only) | Columns commonly used together |

All new fields are **optional** — existing YAML parsing continues to work unchanged.

### FR-3: Tiered Enrichment Strategy

#### Tier 1: Human-Curated (~68 columns identified, growing to ~120-150)
- Columns referenced in Q/SQL examples, shared_columns, and enum_reference sections
- Full manual treatment: category, formula, related_columns, typical_aggregation, example_values
- Quality bar: domain expert reviewed

#### Tier 2: Heuristic + BQ Profiling (~4,000 columns)
- Pattern-matching rules for auto-assignment:
  - Type-based: TIMESTAMP/DATE → `time`, BOOLEAN → `dimension`
  - Name-pattern: `*_id/*_hash/*_key` → `identifier`, `*_pnl/*_edge/*_volume` → `measure`
  - Interval-expanded columns inherit from their family
- BQ profiling auto-populates `example_values` using cardinality tiers
- No human review required; Pydantic validation is the gate

#### Tier 3: LLM-Assisted (~1,300 columns)
- Columns where heuristics aren't confident
- LLM generates: category, typical_aggregation, filterable
- LLM does **NOT** generate: formula, related_columns (left null — Tier 1 only)
- Pydantic validation + 10-15% spot-check; re-run if error rate >5%

### FR-4: BQ Data Profiling Pipeline

`scripts/profile_columns.py` — queries BQ to auto-populate `example_values`.

Tiered cardinality thresholds:

| Cardinality | Treatment | `comprehensive` | In Embedding? | Example Columns |
|-------------|-----------|-----------------|---------------|-----------------|
| < 25 | Store ALL values | `true` | Yes (all values) | trade_side, option_type_name, exchange |
| 25–250 | Store top 10 | `false` | Yes (top 5 only) | symbol, portfolio, term |
| 250+ | Store top 5 or skip | N/A | No | instrument_hash, order_id |

The script:
1. Queries `APPROX_TOP_COUNT` + `APPROX_COUNT_DISTINCT` for every STRING column
2. Classifies into cardinality tiers
3. Outputs enrichment data as JSON for merging into YAML
4. Flags boundary cases for human review

### FR-5: Pydantic Validation Models

`catalog/schema.py` — Pydantic models defining the enriched schema contract:
- `ColumnSchema` — validates all column fields (name, type, description, category, typical_aggregation, etc.)
- `TableSchema` — validates table-level structure
- Category enum constraint: `Literal["dimension", "measure", "time", "identifier"]`
- Aggregation enum: `Literal["SUM", "AVG", "WEIGHTED_AVG", "COUNT", "MIN", "MAX"]`

**Consumers:**
1. `tests/test_catalog_validation.py` — CI gate, validates all YAMLs on every PR
2. `scripts/validate_catalog.py` — standalone script for local development
3. `scripts/populate_embeddings.py` — optional import for type-safe field access

**NOT a consumer:** `catalog_loader.py` — stays as plain dict access at runtime.

### FR-6: Heuristic Enrichment Script

`scripts/enrich_columns.py` — auto-assigns category and other fields based on rules:

```python
RULES = {
    # Type-based
    'TIMESTAMP': {'category': 'time'},
    'DATE': {'category': 'time'},
    'BOOLEAN': {'category': 'dimension', 'filterable': True},
    # Name-pattern based
    r'_id$|_hash$|_key$': {'category': 'identifier'},
    r'^is_|^has_': {'category': 'dimension', 'filterable': True},
    r'_pnl|_edge|_size|_qty|_volume': {'category': 'measure'},
    r'_bps$': {'category': 'measure', 'typical_aggregation': 'AVG'},
    r'^instant_pnl': {'category': 'measure', 'typical_aggregation': 'SUM'},
    # Interval-expanded pattern
    r'_(1s|5s|10s|30s|1m|5m|10m|30m|1h)': {'category': 'measure'},
}
```

## Non-Functional Requirements

- **Backwards compatibility:** All new fields optional. No changes to runtime catalog loading
- **No YAML bloat:** `example_values` capped at 10 items, `formula` is one-line, `related_columns` max 5
- **Performance:** Profiling script completes in <5 minutes for all 12 tables
- **Validation speed:** Pydantic validation of all 4,631 columns completes in <10 seconds

## Acceptance Criteria

1. Every column across all 12 table YAMLs has a `category` field
2. All `measure` columns have `typical_aggregation`
3. Categorical columns (cardinality <250) have `example_values` with correct `comprehensive` flag
4. `catalog/schema.py` Pydantic models validate all enriched YAMLs without errors
5. `tests/test_catalog_validation.py` passes in CI
6. `scripts/profile_columns.py` queries BQ and outputs cardinality-tiered results
7. `scripts/enrich_columns.py` auto-assigns category for >80% of columns via heuristics
8. `scripts/validate_catalog.py` validates individual files or entire catalog
9. Tier 1 columns (~68+) have human-reviewed formula and related_columns
10. All 578+ existing tests continue to pass
11. Runtime catalog_loader.py unchanged — no new imports or validation at load time

## Out of Scope

- Embedding text changes (Track 19)
- Business glossary collection (Track 19)
- Metric definitions and named filters (Track 21)
- Few-shot example expansion (Track 20)
- LLM-assisted enrichment tooling (deferred — manual + heuristic sufficient for initial rollout)
- Changes to `catalog_loader.py` runtime behaviour
