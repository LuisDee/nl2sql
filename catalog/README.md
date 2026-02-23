# Catalog & Embeddings System

Two-layer metadata system that gives the NL2SQL agent context about what the data means.

**Layer 1 -- YAML Catalog** (this directory): Static, version-controlled metadata describing every dataset, table, column, and routing rule. Human-editable source of truth.

**Layer 2 -- BigQuery Embeddings**: Three BigQuery tables with vector embeddings derived from the YAML catalog. Powers semantic search at query time ("what was the edge?" -> routes to `kpi.markettrade`).

## Architecture

```
catalog/                          <-- Layer 1: YAML source of truth
├── _routing.yaml                 <-- Cross-dataset disambiguation rules
├── kpi/
│   ├── _dataset.yaml             <-- KPI dataset description + shared columns
│   ├── markettrade.yaml          <-- Per-table: columns, types, descriptions, synonyms
│   ├── quotertrade.yaml
│   ├── brokertrade.yaml
│   ├── clicktrade.yaml
│   └── otoswing.yaml
└── data/
    ├── _dataset.yaml             <-- Data dataset description
    ├── theodata.yaml
    ├── marketdata.yaml
    ├── marketdepth.yaml
    ├── swingdata.yaml
    ├── markettrade.yaml
    ├── quotertrade.yaml
    └── clicktrade.yaml

examples/                         <-- Validated Q->SQL pairs (few-shot memory)
├── kpi_examples.yaml
├── data_examples.yaml
└── routing_examples.yaml

scripts/                          <-- Embedding pipeline tooling
├── run_embeddings.py             <-- BQ infrastructure: tables, embeddings, indexes
└── populate_embeddings.py        <-- YAML -> BQ: populate column_embeddings & query_memory
```

### BigQuery Embedding Tables (`nl2sql_metadata` dataset)

| Table | What it stores | Row count | Used for |
|---|---|---|---|
| `schema_embeddings` | Dataset, table, and routing descriptions | ~17 | Route questions to the right table/dataset |
| `column_embeddings` | Column name + description + synonyms | ~1000+ | Map natural language terms to column names |
| `query_memory` | Question + validated SQL pairs | 30+ | Few-shot examples for SQL generation |

All three tables have an `embedding` column (`ARRAY<FLOAT64>`) populated by BigQuery ML's `text-embedding-005` model, enabling `VECTOR_SEARCH` with cosine distance.

---

## Parameterization: The `{project}` Convention

All YAML files use `{project}` as a placeholder for the GCP project ID. This is resolved at runtime by `catalog_loader.py`:

```yaml
# In a table YAML:
table:
  fqn: '{project}.nl2sql_omx_kpi.markettrade'    # NOT hardcoded
```

```yaml
# In an example YAML:
sql: |
  SELECT * FROM `{project}.nl2sql_omx_kpi.markettrade`
  WHERE trade_date = '2026-02-17'
```

Python scripts resolve the placeholder using `settings.gcp_project`:
```python
from nl2sql_agent.config import settings
resolved = fqn.replace("{project}", settings.gcp_project)
```

**Never hardcode** project IDs like `cloud-data-n-base-d4b3` or `melodic-stone-437916-t3` in catalog or example files.

---

## How to: Add a New Table

Example: adding a `voldata` table to the data dataset.

### 1. Create the table YAML

Create `catalog/data/voldata.yaml`:

```yaml
table:
  name: voldata
  dataset: nl2sql_omx_data
  fqn: '{project}.nl2sql_omx_data.voldata'
  layer: data
  description: >
    Volatility data for OMX options. Contains implied vol surfaces,
    skew parameters, and vol regime classifications.
  partition_field: trade_date
  cluster_fields:
    - symbol
  row_count_approx: 0
  columns:
    - name: trade_date
      type: DATE
      description: Date of the vol snapshot. Partition column.
      synonyms: [date]
    - name: symbol
      type: STRING
      description: Underlying instrument symbol.
      synonyms: [underlying, ticker]
    - name: implied_vol
      type: FLOAT64
      description: Annualised implied volatility as a decimal (0.25 = 25%).
      synonyms: [IV, vol, sigma, implied volatility]
    # ... add all columns
```

Required fields per table: `name`, `dataset`, `fqn`, `layer`, `description`, `partition_field`, `columns`.
Required fields per column: `name`, `type`, `description`.
Optional per column: `synonyms` (list), `example_values` (list), `range` (string).

### 2. Update the dataset YAML

Add `voldata` to `catalog/data/_dataset.yaml`:

```yaml
dataset:
  tables:
    - theodata
    - marketdata
    - marketdepth
    - swingdata
    - markettrade
    - quotertrade
    - clicktrade
    - voldata          # <-- add here
```

Add routing patterns:

```yaml
  routing:
    - patterns: ["implied vol", "volatility", "IV", "vol surface", "sigma"]
      table: voldata
      notes: "Canonical vol table. For vol questions, prefer this over theodata."
```

### 3. Add example queries

Add validated Q->SQL pairs to `examples/data_examples.yaml`:

```yaml
  - question: "What is the average implied vol for ERIC today?"
    sql: |
      SELECT
        symbol,
        ROUND(AVG(implied_vol), 4) AS avg_iv
      FROM `{project}.nl2sql_omx_data.voldata`
      WHERE trade_date = '2026-02-17'
        AND symbol = 'ERIC'
      GROUP BY symbol
    tables_used: [voldata]
    dataset: nl2sql_omx_data
    complexity: simple
    routing_signal: "implied vol question -> voldata"
    validated: false
    validated_by: ""
```

### 4. Populate embeddings

```bash
# Populate column_embeddings and query_memory from YAML
python scripts/populate_embeddings.py

# Regenerate BQ embeddings (only processes rows with empty embedding arrays)
python scripts/run_embeddings.py --step generate-embeddings
```

### 5. Update schema_embeddings

Edit the `populate_schema_embeddings()` function in `scripts/run_embeddings.py` to add a row for the new table in the DATA section STRUCT array:

```python
STRUCT('table', 'data', 'nl2sql_omx_data', 'voldata',
  'Volatility data for OMX options. Implied vol surfaces, IV, sigma, vol skew. Canonical table for volatility questions.'),
```

Then re-run:
```bash
python scripts/run_embeddings.py --step populate-schema
python scripts/run_embeddings.py --step generate-embeddings
```

### 6. Run tests

```bash
pytest tests/test_yaml_catalog.py tests/test_catalog_loader.py -v
```

Add the new table name to the parametrized test list in `tests/test_yaml_catalog.py` if needed.

---

## How to: Rerun Embeddings (Full Pipeline)

Run all steps in order:

```bash
python scripts/run_embeddings.py --step all
```

Or run individual steps:

```bash
# 1. Create metadata dataset (IF NOT EXISTS)
python scripts/run_embeddings.py --step create-dataset

# 2. Verify embedding model is accessible
python scripts/run_embeddings.py --step verify-model

# 3. Create/replace the 3 embedding tables (WARNING: drops existing data)
python scripts/run_embeddings.py --step create-tables

# 4. Populate schema_embeddings from hardcoded descriptions
python scripts/run_embeddings.py --step populate-schema

# 5. Generate vector embeddings for all rows with empty arrays
python scripts/run_embeddings.py --step generate-embeddings

# 6. Create TREE_AH vector indexes (requires >=5000 rows to activate)
python scripts/run_embeddings.py --step create-indexes

# 7. Run 5 vector search test cases
python scripts/run_embeddings.py --step test-search
```

### Incremental Updates (safe to repeat)

When you've added new YAML tables or examples:

```bash
# Load new YAML data into BQ (uses MERGE -- idempotent)
python scripts/populate_embeddings.py

# Generate embeddings only for rows that don't have them yet
python scripts/run_embeddings.py --step generate-embeddings
```

Both operations are **idempotent**: MERGE upserts existing rows and only generates embeddings where `ARRAY_LENGTH(embedding) = 0`.

---

## How to: Add Example Queries

1. Add entries to the appropriate file in `examples/`:
   - `kpi_examples.yaml` -- KPI performance questions (edge, PnL, slippage)
   - `data_examples.yaml` -- Raw data questions (theo, market data, depth)
   - `routing_examples.yaml` -- Disambiguation questions (cross-dataset, UNION ALL)

2. Each example requires:

```yaml
- question: "Natural language question as a user would ask it"
  sql: |
    SELECT ...
    FROM `{project}.dataset.table`     # Must use {project}
    WHERE trade_date = '2026-02-17'    # Always filter on partition
  tables_used: [table_name]            # List of tables referenced
  dataset: nl2sql_omx_kpi             # nl2sql_omx_kpi or nl2sql_omx_data
  complexity: simple                   # simple | medium | complex
  routing_signal: "why this table"     # Explains the routing decision
  validated: false                     # Set true after manual validation
  validated_by: ""                     # Name of person who validated
```

3. Load into BQ:
```bash
python scripts/populate_embeddings.py
python scripts/run_embeddings.py --step generate-embeddings
```

---

## How to: Switch Between Dev and Prod

All configuration is in `nl2sql_agent/.env`. Two things change between environments:

1. **Project & model references** — GCP project, Vertex AI connection, embedding model ref
2. **Dataset prefix** — dev datasets have `nl2sql_` prefix, prod datasets don't

**Dev** (`nl2sql_agent/.env`):
```env
GCP_PROJECT=melodic-stone-437916-t3
DATASET_PREFIX=nl2sql_
VERTEX_AI_CONNECTION=melodic-stone-437916-t3.europe-west2.vertex-ai-connection
EMBEDDING_MODEL_REF=melodic-stone-437916-t3.nl2sql.text_embedding_model
```
Computed datasets: `nl2sql_omx_kpi`, `nl2sql_omx_data`, `nl2sql_metadata`

**Prod** (`nl2sql_agent/.env`):
```env
GCP_PROJECT=cloud-data-n-base-d4b3
DATASET_PREFIX=
VERTEX_AI_CONNECTION=cloud-ai-d-base-a2df.europe-west2.vertex-ai-connection
EMBEDDING_MODEL_REF=cloud-ai-d-base-a2df.nl2sql.text_embedding_model
```
Computed datasets: `omx_kpi`, `omx_data`, `metadata`

You can also override individual datasets explicitly (`KPI_DATASET=custom_kpi`) — explicit values take precedence over prefix computation.

After switching, re-run the embedding pipeline to populate the target project:
```bash
python scripts/run_embeddings.py --step all
python scripts/populate_embeddings.py
python scripts/run_embeddings.py --step generate-embeddings
```

---

## How to: Add a New Market (Exchange)

All 10 exchanges share the same table schemas. Adding a new exchange (e.g., Brazil) requires:

### 1. Verify datasets exist in BQ

The agent expects two datasets per exchange: `{prefix}{exchange}_kpi` and `{prefix}{exchange}_data`.

```bash
# Dev example (prefix=nl2sql_):
bq ls --project_id=melodic-stone-437916-t3 nl2sql_brazil_kpi
bq ls --project_id=melodic-stone-437916-t3 nl2sql_brazil_data
```

### 2. Add exchange to registry

Add an entry in `catalog/_exchanges.yaml`:
```yaml
  brazil:
    aliases: [brazil, bovespa, b3, brazilian]
```

### 3. Verify table schemas match

New market tables should have the same columns as OMX. If schemas vary, create market-specific YAML overrides (not yet supported — all markets currently share OMX schema).

### 4. Add symbol mappings

Insert rows into the `symbol_exchange_map` BQ table so the agent can resolve trading symbols (e.g., "VALE3") to the correct exchange:

```bash
# Add to data/symbol_exchange_map.csv, then:
python scripts/run_embeddings.py --step populate-symbols
```

### 5. Populate embeddings

```bash
python scripts/populate_embeddings.py
python scripts/run_embeddings.py --step generate-embeddings
```

### 6. Test

Ask a question mentioning the new exchange:
> "What was the edge on market trades for bovespa yesterday?"

### Metadata is shared

The metadata dataset (`{prefix}metadata`) is shared across ALL markets. It contains:
- `schema_embeddings` — table-level routing (shared schema across exchanges)
- `column_embeddings` — column-level search (shared columns across exchanges)
- `query_memory` — cached queries (filtered by `dataset_name` column)
- `glossary_embeddings` — business terms (shared)
- `symbol_exchange_map` — per-exchange symbol lookup

No per-market metadata datasets are needed. The `dataset_name` column in `query_memory` + semantic cache provides exchange-level isolation.

---

## Metadata Protocol & Design Pattern

This system follows the **Retrieval-Augmented Generation (RAG) metadata protocol** with a two-layer architecture:

### Layer 1: Static Catalog (YAML)
- **Pattern**: Schema-as-Code / Data Catalog
- YAML files act as a version-controlled data dictionary, similar to dbt's `schema.yml` or Great Expectations' metadata layer
- Human-readable, diffable, reviewable in PRs
- Serves as the single source of truth for table/column semantics

### Layer 2: Vector Embeddings (BigQuery)
- **Pattern**: Semantic Retrieval / RAG Knowledge Base
- Text descriptions are embedded using `text-embedding-005` (768-dimensional vectors)
- `VECTOR_SEARCH` with cosine distance finds the most semantically relevant tables/columns/examples for a natural language question
- Three-table design follows the established NL2SQL metadata pattern:
  - **Schema embeddings**: "Which table should I query?" (table routing)
  - **Column embeddings**: "What does this column mean?" (column mapping)
  - **Query memory**: "Have I seen a similar question before?" (few-shot retrieval)

### Why This Pattern

1. **Separation of concerns**: Static metadata (YAML) is decoupled from runtime search (embeddings). You can update descriptions without regenerating embeddings, and regenerate embeddings without editing YAML.

2. **Idempotency**: All operations use MERGE or conditional updates. Safe to re-run at any time.

3. **Environment portability**: `{project}` parameterization means the same catalog works across dev/staging/prod with zero code changes.

4. **Protocol-based testability**: All BigQuery access goes through `BigQueryProtocol`, so embedding scripts and catalog loaders can be unit tested with `FakeBigQueryClient`.

5. **Embedding task types**: Uses `RETRIEVAL_DOCUMENT` for stored content and `RETRIEVAL_QUERY` for search queries, following Google's recommended asymmetric embedding pattern for retrieval tasks.

---

## Validation & Testing

```bash
# Run all catalog and loader tests
pytest tests/test_yaml_catalog.py tests/test_catalog_loader.py -v

# Run the full test suite
pytest tests/ -v
```

Tests verify:
- All 15 YAML files exist and parse correctly
- Required keys present in every table/column/example
- `{project}` placeholder used in all FQN and SQL fields
- `resolve_fqn()` and `resolve_example_sql()` work for both dev and prod project IDs
- Validation functions catch missing keys, invalid layers, invalid datasets
- 30+ example queries exist across the 3 example files

---

## Design Decision: Self-Contained Table YAMLs

Each table YAML file contains ALL columns for that table, including shared columns that appear across multiple KPI tables. This is intentional:

- Each file is independently editable — changing a column description for one table doesn't affect others
- Columns can diverge between tables over time (different descriptions, different business meanings)
- No risk of shared reference breaking multiple files
- Embedding generation treats each file as a standalone unit

The trade-off is file size (~200-376KB per KPI file). This is acceptable because these files are read by code (not humans) and cached in memory.
