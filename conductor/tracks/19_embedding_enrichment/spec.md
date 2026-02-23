# Spec: Embedding Strategy & Glossary Collection

## Overview

Redesigns the embedding text template to separate retrieval signals from generation context, and creates a business glossary collection for domain concepts that don't map to individual columns.

### Problem
1. **Embedding dilution:** Current `build_embedding_text()` uses only `name, type, layer, description, synonyms`. Missing `category` and `example_values` (defined by Track 18) means the embedding can't distinguish measures from dimensions or match on known enum values.
2. **No concept-level search:** Business concepts like "total PnL", "slippage decomposition", or "ATM strike" don't map to a single column. The agent has no way to resolve these via vector search — they're hardcoded in prompts or routing YAML.
3. **Generation context gap:** Fields like `formula`, `related_columns`, `typical_aggregation` are defined in the YAML schema (Track 18) but have no path from the YAML catalog to the LLM prompt after vector search. They need to travel as payload columns alongside embeddings.

### Solution
1. Enrich `build_embedding_text()` with `[category]` tag and selective `example_values` (filterable dimensions only, ≤5 values). Keep formula/related_columns/typical_aggregation out of embedding text — they travel as payload columns on the `column_embeddings` table and are returned by the vector search CTE for the prompt builder to format.
2. Create `catalog/glossary.yaml` with 20-30 business concept definitions, stored in a new `glossary_embeddings` BQ table, searched via a third UNION arm in the vector search CTE with per-source top-K ranking.

## Functional Requirements

### FR-1: Enriched Embedding Text Template + Payload Columns

**Embedding text** (aids retrieval — what goes into `embedding_text` column):
- `[category]` tag when `category` field is present (e.g., `[measure]`, `[dimension]`)
- `example_values` (≤5 values) only when `filterable=True` AND `category="dimension"` AND `example_values` is present
- Unchanged: name, type, layer, description, synonyms

**Payload columns** (generation context — stored on `column_embeddings` table, returned by CTE, NOT embedded):
- `category` (STRING, nullable)
- `formula` (STRING, nullable)
- `example_values` (ARRAY<STRING>, nullable)
- `related_columns` (ARRAY<STRING>, nullable)
- `typical_aggregation` (STRING, nullable)
- `filterable` (BOOL, nullable)

These columns are added to the `column_embeddings` table DDL and populated during the MERGE in `populate_column_embeddings()`. The vector search CTE SELECTs them alongside embedding results. The prompt builder formats them into the column context block sent to the LLM (e.g., `formula: ...`, `typical_aggregation: SUM`).

**Template output examples:**
- Measure: `markettrade.instant_pnl (FLOAT64, kpi): [measure]. Profit and loss at time of trade. Also known as: pnl, profit`
- Filterable dimension: `markettrade.exchange (STRING, kpi): [dimension]. Exchange where trade was executed. Also known as: venue. Values: ICE, Eurex, NSE`
- No enrichment yet: same as current format (graceful degradation)

### FR-2: Business Glossary YAML
- Create `catalog/glossary.yaml` with 20-30 business concept entries
- Each entry has: `name` (string), `definition` (text), `synonyms` (list of alternative names), `related_columns` (list of table.column refs), `category` (optional grouping, e.g., "performance", "risk", "execution")
- Pydantic model `GlossaryEntrySchema` in `catalog/schema.py` validates the glossary
- CI test validates glossary.yaml against the Pydantic model

### FR-3: Glossary Embeddings Table
- Create `glossary_embeddings` BQ table in metadata dataset
- Schema: `id`, `name`, `definition`, `synonyms` (ARRAY<STRING>), `related_columns` (ARRAY<STRING>), `category`, `embedding_text`, `embedding` (ARRAY<FLOAT64>), `updated_at`
- **Embedding text** (retrieval): `{name}: {definition}. Also known as: {synonyms}`
- **Payload** (generation context, not embedded): `related_columns`, `category`

### FR-4: Glossary Population Script
- Create `scripts/populate_glossary.py` following same MERGE pattern as `populate_column_embeddings()`
- Reads `catalog/glossary.yaml`, builds embedding text, MERGEs into `glossary_embeddings`
- MERGE key: `name` column (one row per concept)
- Sets `embedding = NULL` on update to trigger re-embedding

### FR-5: Enriched Vector Search CTE + Prompt Integration

**CTE changes:**
- Update `_COLUMN_SEARCH_SQL` in `vector_search.py` to add a third UNION arm searching `glossary_embeddings`
- **Per-source top-K ranking:** top 30 columns (existing) + top 3 glossary concepts. Glossary results are not crowded out by column matches.
- Column results include payload fields: `category`, `formula`, `example_values`, `related_columns`, `typical_aggregation`, `filterable`
- Glossary results include payload fields: `related_columns`, `category`

**Prompt integration:**
- Column results: formatted into existing schema context block with payload fields appended (e.g., `formula: SUM(x*y)`, `typical_aggregation: SUM`)
- Glossary results: formatted into a dedicated **Business Context** section in the tool output, separate from column schema. Contains concept name, definition, and related columns. Not mixed into column listings.
- The `vector_search_columns()` tool return type is updated to include a `glossary` key alongside `tables` and `examples`

### FR-6: Pipeline Integration

Step order in `run_embeddings.py`:
1. `create-dataset` — CREATE SCHEMA IF NOT EXISTS
2. `verify-model` — check embedding model exists
3. `create-tables` — create all tables including `glossary_embeddings`
4. `populate-schema` — load schema_embeddings from YAML
5. `populate-columns` — load column_embeddings from YAML (with new payload columns)
6. `populate-glossary` — load glossary_embeddings from glossary.yaml **(new step)**
7. `populate-symbols` — load symbol_exchange_map
8. `generate-embeddings` — ML.GENERATE_EMBEDDING on all 4 tables (schema, columns, glossary, queries)
9. `create-indexes` — create vector indexes including glossary_embeddings
10. `test-search` — verify search quality

## Non-Functional Requirements

- Backwards compatible: vector search works with old embeddings during migration (new payload columns are nullable, new UNION arm is additive)
- Same embedding model (`text-embedding-005`) and task types (`RETRIEVAL_DOCUMENT` for stored content)
- No system prompt changes — glossary results injected via tool output as Business Context section
- MERGE-based population is idempotent
- Existing MERGE mechanics handle embedding rebuild (new template → different embedding_text → MERGE updates → nulls embedding → next generate-embeddings picks it up)

## Acceptance Criteria

1. `build_embedding_text()` includes `[category]` when available, `example_values` for filterable dimensions only
2. `column_embeddings` table has payload columns (category, formula, example_values, related_columns, typical_aggregation, filterable) populated during MERGE
3. `catalog/glossary.yaml` has 20-30 entries with name/definition/synonyms/related_columns, validates against Pydantic model
4. `glossary_embeddings` table created and populated in BQ
5. Vector search CTE returns column results (with payload) and glossary results (with `source='glossary'`), ranked per-source (top-30 columns + top-3 glossary)
6. Glossary results are formatted into a dedicated Business Context section in the LLM prompt, not mixed into column schema context
7. All existing tests pass, new tests cover embedding text builder and glossary validation
8. Full pipeline runs: `run_embeddings.py --step all` succeeds with glossary integration

## Out of Scope

- Populating category/example_values in YAML (Track 22)
- Metric definitions and named filters (Track 21)
- Few-shot example expansion (Track 20)
- Prompt template changes beyond Business Context section injection
