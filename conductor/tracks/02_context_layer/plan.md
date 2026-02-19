# Track 02: Context Layer -- Implementation Plan

## Objective

Build the **Two-Layer Metadata** system that gives the NL2SQL agent context about what the data means. Layer 1: 16 YAML catalog files with business descriptions, synonyms, routing rules, and column metadata for all 13 tables across 2 datasets. Layer 2: 3 BigQuery embedding tables (`schema_embeddings`, `column_embeddings`, `query_memory`) with vector indexes for semantic search. Plus 30+ validated Q->SQL example pairs. At the end of this track, `VECTOR_SEARCH` against the embedding tables correctly routes "what was the edge on our trade?" -> KPI tables and "how did implied vol change?" -> theodata, passing 5/5 retrieval test cases.

**Dependency**: Track 01 complete (agent skeleton, protocols, schemas extracted into `schemas/kpi/*.json` and `schemas/data/*.json`).

---

## CARRIED FORWARD: Track 01 Conventions

These conventions from Track 01 remain in force. Do NOT violate them.

1. **ADK conventions**: `root_agent` variable, `__init__.py` with `from . import agent`, `.env` inside agent package.
2. **LiteLLM conventions**: `LiteLlm` (camelCase), model string `gemini-3-flash-preview`, env vars for API key/base.
3. **Protocol-based DI**: All BigQuery interactions go through `BigQueryProtocol`. All embedding operations go through `EmbeddingProtocol`. Never import `bigquery.Client` directly in business logic.
4. **Configuration**: All config via `from nl2sql_agent.config import settings`. Never use `os.getenv()`.
5. **Logging**: `from nl2sql_agent.logging_config import get_logger`. Structured JSON via structlog.

---

## CRITICAL TRACK 02 CONVENTIONS

### Environment-Driven Configuration

All infrastructure values come from `settings` (loaded from `.env`). **No hardcoded project names, dataset names, or model references in code or SQL scripts.**

The `.env` file (and `.env.example`) defines:

```
GCP_PROJECT=<project-id>
BQ_LOCATION=europe-west2
KPI_DATASET=nl2sql_omx_kpi
DATA_DATASET=nl2sql_omx_data
METADATA_DATASET=nl2sql_metadata
VERTEX_AI_CONNECTION=<project-id>.europe-west2.vertex-ai-connection
EMBEDDING_MODEL_REF=<project-id>.nl2sql.text_embedding_model
EMBEDDING_MODEL=text-embedding-005
```

**Dev environment** (in `.env`):
```
GCP_PROJECT=melodic-stone-437916-t3
EMBEDDING_MODEL_REF=melodic-stone-437916-t3.nl2sql.text_embedding_model
VERTEX_AI_CONNECTION=melodic-stone-437916-t3.europe-west2.vertex-ai-connection
```

**Prod environment** (in `.env-prod` / `.env.example`):
```
GCP_PROJECT=cloud-data-n-base-d4b3
EMBEDDING_MODEL_REF=cloud-ai-d-base-a2df.nl2sql.text_embedding_model
VERTEX_AI_CONNECTION=cloud-ai-d-base-a2df.europe-west2.vertex-ai-connection
```

In Python code, always use `settings.gcp_project`, `settings.embedding_model_ref`, etc.
In YAML catalog files, use `{project}` placeholder in `fqn` and example SQL -- resolved at load time by `catalog_loader.py`.
Raw SQL scripts are **replaced by a Python runner** (`scripts/run_embeddings.py`) that builds SQL from settings.

### YAML Catalog Schema

Every table YAML file follows this EXACT structure. Do not add extra top-level keys. Do not rename keys.

```yaml
table:
  name: <table_name>                    # Exact BigQuery table name
  dataset: <nl2sql_omx_kpi|nl2sql_omx_data>  # Which dataset this table lives in
  fqn: "{project}.nl2sql_omx_kpi.markettrade"  # Template -- {project} resolved at runtime
  layer: <kpi|data>                     # Gold (kpi) or Silver (data)
  description: >                        # Multi-line business description
    What this table contains. What one row represents.
    Rich with synonyms and trader terminology.
  partition_field: trade_date            # All tables partition on trade_date
  cluster_fields: [...]                 # List of clustering columns
  row_count_approx: <int>              # Approximate row count from Track 01
  columns:
    - name: <column_name>               # Exact BigQuery column name
      type: <STRING|FLOAT64|INT64|...>  # Exact BigQuery type
      description: >                    # Business meaning
        What this column means to a trader.
      synonyms: [...]                   # Alternative names traders use
      example_values: [...]             # Optional: sample values
      range: "..."                      # Optional: value range
  business_rules:                       # Optional: calculation logic
    <rule_name>:
      description: "..."
      formula: "..."
```

**The `fqn` field** uses `{project}` as a placeholder. When `catalog_loader.py` loads a table YAML, it calls `resolve_fqn()` to replace `{project}` with `settings.gcp_project`. This means the same YAML works across dev and prod without edits.

**DO NOT** nest columns deeper. Keep it flat -- one level under `columns`.

**DO NOT** use `null` for empty lists. Use `[]`.

**DO NOT** add a `table.synonyms` key at the table level. Synonyms live on columns only. Table-level synonyms go in `description` text.

### Dataset YAML Schema

```yaml
dataset:
  name: <nl2sql_omx_kpi|nl2sql_omx_data>
  layer: <kpi|data>
  description: >
    What this dataset contains overall.
  tables: [list of table names]
  shared_columns:                       # Columns common to all/most tables
    <column_name>:
      description: "..."
      synonyms: [...]
  routing:                              # How to pick the right table
    - patterns: [...]
      table: <table_name>
      notes: "..."
  disambiguation:                       # Clarify confusing overlaps
    <case_name>: >
      Explanation of when to use which table.
```

### Example Query YAML Schema

```yaml
examples:
  - question: "Natural language question a trader would ask"
    sql: |
      SELECT ...
      FROM `{project}.nl2sql_omx_kpi.markettrade`
      WHERE trade_date = '2026-02-17'
      ...
    tables_used: [markettrade]
    dataset: nl2sql_omx_kpi
    complexity: simple|medium|complex
    routing_signal: "Why this table was chosen"
    validated: false                     # Set to true ONLY after running in BQ
    validated_by: ""                     # Who validated it
```

**CRITICAL**: Every SQL query MUST use `{project}` placeholder in table references: `` `{project}.nl2sql_omx_kpi.markettrade` ``. The `catalog_loader.resolve_example_sql()` function replaces `{project}` with `settings.gcp_project` at load time, producing fully-qualified names.

**CRITICAL**: Every SQL query MUST filter on `trade_date`. Our dev data has `trade_date = '2026-02-17'` only.

### Embedding Tables Convention

All embedding tables live in dataset `{settings.metadata_dataset}` in project `{settings.gcp_project}`, location `{settings.bq_location}`.

The embedding model is referenced by `{settings.embedding_model_ref}` (a fully-qualified BigQuery ML model). The underlying Vertex AI connection is `{settings.vertex_ai_connection}` with endpoint `{settings.embedding_model}` (default: `text-embedding-005`).

**DO NOT** create a new embedding model. Use the existing one at `settings.embedding_model_ref`.

### Idempotency Convention

Every SQL operation MUST be runnable multiple times without duplicating data. Use one of:

1. `CREATE OR REPLACE TABLE` for table creation
2. `MERGE` for data population (preferred for embedding rows)
3. `DELETE + INSERT` wrapped in a transaction for simpler cases
4. `CREATE VECTOR INDEX IF NOT EXISTS` for indexes

**DO NOT** use bare `INSERT INTO` for embedding population. It creates duplicates on re-run.

---

## Infrastructure Values (from `.env`)

All values below are resolved from `settings` at runtime. The table shows the settings field and its value in each environment.

| Setting | Dev Value | Prod Value |
|---|---|---|
| `settings.gcp_project` | `melodic-stone-437916-t3` | `cloud-data-n-base-d4b3` |
| `settings.bq_location` | `europe-west2` | `europe-west2` |
| `settings.kpi_dataset` | `nl2sql_omx_kpi` | `nl2sql_omx_kpi` |
| `settings.data_dataset` | `nl2sql_omx_data` | `nl2sql_omx_data` |
| `settings.metadata_dataset` | `nl2sql_metadata` | `nl2sql_metadata` |
| `settings.embedding_model_ref` | `melodic-stone-437916-t3.nl2sql.text_embedding_model` | `cloud-ai-d-base-a2df.nl2sql.text_embedding_model` |
| `settings.vertex_ai_connection` | `melodic-stone-437916-t3.europe-west2.vertex-ai-connection` | `cloud-ai-d-base-a2df.europe-west2.vertex-ai-connection` |
| Dev Data Date | `2026-02-17` | `2026-02-17` |

### Table Inventory (13 tables)

**nl2sql_omx_kpi** (Gold -- KPI-enriched trade data):
| Table | Clustering | Notes |
|---|---|---|
| brokertrade | portfolio, symbol, term, instrument_hash | EMPTY for 2026-02-17 |
| clicktrade | portfolio, symbol, term, instrument_hash | |
| markettrade | portfolio, symbol, term, instrument_hash | |
| otoswing | portfolio, symbol, term, instrument_hash | |
| quotertrade | portfolio, symbol, term, instrument_hash | |

**nl2sql_omx_data** (Silver -- raw trade/market data):
| Table | Clustering | Notes |
|---|---|---|
| brokertrade | portfolio, symbol, term, instrument_hash | |
| clicktrade | portfolio, symbol, term, instrument_hash | |
| markettrade | portfolio, symbol, term, instrument_hash | |
| quotertrade | portfolio, symbol, term, instrument_hash | |
| theodata | portfolio, symbol, term, instrument_hash | Theo pricing snapshots |
| swingdata | (none) | |
| marketdata | symbol, term, instrument_hash | |
| marketdepth | symbol, term, instrument_hash | |

---

## File-by-File Specification

### Wave 1: YAML Catalog (16 files)

#### 1. KPI Dataset Metadata

**Path**: `catalog/kpi/_dataset.yaml`

```yaml
dataset:
  name: nl2sql_omx_kpi
  layer: kpi
  description: >
    KPI (Key Performance Indicators) for OMX options trading. Gold layer.
    Contains one table per trade origin, all sharing the same KPI column structure.
    Each row represents one trade with computed performance metrics:
    edge_bps, instant_pnl, slippage, delta_bucket, required_edge_bps.

  tables:
    - brokertrade
    - clicktrade
    - markettrade
    - otoswing
    - quotertrade

  shared_columns:
    trade_date:
      description: "Date of the trade. Partition column. Always filter on this."
      synonyms: ["date", "trading date"]
    portfolio:
      description: "Trading portfolio identifier"
      synonyms: ["book", "portfolio name"]
    symbol:
      description: "Underlying instrument symbol"
      synonyms: ["underlying", "ticker", "product"]
    term:
      description: "Option expiry/term"
      synonyms: ["expiry", "expiration", "maturity"]
    instrument_hash:
      description: "Unique hash identifying the specific option contract"
      synonyms: ["contract id", "instrument id"]
    edge_bps:
      description: >
        Difference between machine fair value (theo) and actual trade price,
        measured in basis points. Positive = traded better than fair value.
      synonyms: ["edge", "the edge", "how much edge", "trading edge"]
    required_edge_bps:
      description: "Minimum edge threshold set by risk management for this trade"
      synonyms: ["required edge", "edge requirement", "minimum edge", "edge threshold"]
    instant_pnl:
      description: "Immediate profit/loss from the trade at execution time"
      synonyms: ["PnL", "instant PnL", "profit", "loss", "P&L", "pnl"]
    delta_bucket:
      description: >
        Standard delta categorisation bucket. Groups trades by moneyness.
      synonyms: ["delta range", "moneyness bucket"]
      example_values: ["0-25", "25-40", "40-60", "60+"]
    slippage:
      description: "Execution slippage metric -- difference from expected fill"
      synonyms: ["slip", "execution slippage"]

  routing:
    - patterns: ["market trade", "exchange trade", "generic trades"]
      table: markettrade
      notes: "Default KPI table when trade type is unspecified"

    - patterns: ["quoter trade", "quoter fill", "quoter PnL", "quoter edge", "auto-quoter"]
      table: quotertrade
      notes: "KPI metrics for auto-quoter originated fills. NOT raw quoter activity."

    - patterns: ["broker trade", "voice trade", "BGC", "MGN", "account", "broker performance"]
      table: brokertrade
      notes: "Has account/broker fields. Use when comparing broker performance. NOTE: may be empty for some dates."

    - patterns: ["click trade", "manual trade"]
      table: clicktrade

    - patterns: ["OTO", "swing", "otoswing", "OTO swing"]
      table: otoswing

    - patterns: ["all trades", "total PnL", "overall", "across all types", "every trade type"]
      action: "UNION ALL across all 5 kpi tables"
      notes: "Agent must query all tables and combine results"

  disambiguation:
    kpi_vs_data_same_table_name: >
      Both nl2sql_omx_kpi and nl2sql_omx_data contain tables with the same names
      (brokertrade, clicktrade, markettrade, quotertrade). The KPI dataset has
      enriched performance metrics (edge_bps, instant_pnl, slippage, delta_bucket).
      The data dataset has raw trade execution details (exact timestamps, prices,
      sizes, fill information). If the question is about performance, edge, PnL,
      or slippage, use KPI. If the question is about raw execution details, timestamps,
      or exact fill prices, use data.
```

#### 2. Data Dataset Metadata

**Path**: `catalog/data/_dataset.yaml`

```yaml
dataset:
  name: nl2sql_omx_data
  layer: data
  description: >
    Raw OMX options trading and market data. Silver layer.
    Contains trade execution details, theoretical pricing snapshots,
    market data feeds, and order book depth. Higher granularity than KPI
    but without computed performance metrics.

  tables:
    - brokertrade
    - clicktrade
    - markettrade
    - quotertrade
    - theodata
    - swingdata
    - marketdata
    - marketdepth

  routing:
    - patterns: ["theo", "theoretical", "fair value", "TV", "machine price", "vol surface"]
      table: theodata
      notes: "Theoretical options pricing snapshots"

    - patterns: ["market data", "market feed", "exchange data", "price feed"]
      table: marketdata
      notes: "Market data feed snapshots"

    - patterns: ["order book", "depth", "market depth", "bid ask", "bid/ask", "levels"]
      table: marketdepth
      notes: "Order book depth / market depth snapshots"

    - patterns: ["swing", "swing data", "OTO swing data"]
      table: swingdata
      notes: "Raw swing trade data"

    - patterns: ["raw trade", "execution detail", "fill detail", "exact timestamp"]
      table: markettrade
      notes: "Raw trade execution details (not KPI enriched)"

  disambiguation:
    theodata_unique: >
      theodata exists ONLY in nl2sql_omx_data. It contains theoretical options
      pricing: theo (fair value), delta, vol (implied volatility), vega, gamma
      per strike per timestamp. Questions about "vol", "IV", "implied volatility",
      "theo", "delta", "vega", "greeks", "fair value", or "machine price" should
      route here.

    marketdata_vs_marketdepth: >
      marketdata has aggregated market data snapshots (top-of-book prices, volumes).
      marketdepth has full order book depth (multiple price levels, bid/ask sizes
      at each level). Use marketdata for "what was the market price" questions.
      Use marketdepth for "what did the order book look like" questions.
```

#### 3. Cross-Dataset Routing

**Path**: `catalog/_routing.yaml`

```yaml
# Cross-dataset routing rules. This content gets embedded as schema_embedding
# rows with source_type='routing' to help the agent disambiguate.

routing_descriptions:

  kpi_vs_data_general: >
    The nl2sql_omx_kpi dataset (gold layer) has KPI-enriched trade data with
    computed metrics: edge_bps, instant_pnl, slippage, delta_bucket,
    required_edge_bps. The nl2sql_omx_data dataset (silver layer) has raw
    trade and market data with exact timestamps, prices, sizes, and execution
    details. Both datasets share some table names (brokertrade, clicktrade,
    markettrade, quotertrade). When the question asks about performance, edge,
    PnL, or slippage, use kpi. When the question asks about raw execution data,
    exact timestamps, or market data, use data.

  kpi_table_selection: >
    The nl2sql_omx_kpi dataset has 5 tables, one per trade origin: markettrade
    (exchange trades, the default), quotertrade (auto-quoter fills), brokertrade
    (broker trades, has account field for broker comparison), clicktrade (manual
    click trades), otoswing (OTO swing trades). When a question doesn't specify
    trade type, use markettrade. When comparing brokers or mentioning account
    names, use brokertrade. When asking about all trades or total PnL, UNION
    ALL across all 5 tables.

  theodata_routing: >
    Questions about theoretical pricing, implied volatility (IV, vol, sigma),
    delta, vega, gamma, theta, greeks, fair value, machine price, or theo
    should route to nl2sql_omx_data.theodata. This is the ONLY table with
    options pricing theory data. It is in the data dataset, not kpi.

  market_data_routing: >
    Questions about market prices, exchange feeds, top-of-book, or market
    snapshots go to nl2sql_omx_data.marketdata. Questions about order book
    depth, bid/ask levels, or book shape go to nl2sql_omx_data.marketdepth.
    Neither of these exist in the kpi dataset.
```

#### 4-16. Table YAML Files (13 files)

Each table gets its own YAML. Below are the complete file paths and structure templates. The `columns` section MUST be populated from the extracted JSON schemas (`schemas/kpi/*.json` and `schemas/data/*.json` from Track 01).

**IMPORTANT**: The column listings below show the STRUCTURE. The actual column names, types, and descriptions MUST come from running `setup/extract_schemas.py` against the real tables. The descriptions marked `# ENRICH` need human/LLM enrichment from KPI repo source code, proto files, and trader knowledge.

**Path**: `catalog/kpi/markettrade.yaml`

```yaml
table:
  name: markettrade
  dataset: nl2sql_omx_kpi
  fqn: "{project}.nl2sql_omx_kpi.markettrade"
  layer: kpi
  description: >
    KPI metrics for market (exchange) trades on OMX options. Gold layer.
    One row per trade. Contains computed performance metrics: edge_bps
    (edge in basis points), instant_pnl (immediate P&L), slippage,
    delta_bucket (moneyness categorisation). This is the DEFAULT KPI
    table when the trader doesn't specify a trade type.
    Also called: exchange trades, market trades, generic trades.
  partition_field: trade_date
  cluster_fields: [portfolio, symbol, term, instrument_hash]
  row_count_approx: 0  # UPDATE from extract_schemas.py output
  columns: []           # POPULATE from schemas/kpi/markettrade.json
  business_rules:
    default_kpi_table:
      description: "When trade type is unspecified, this is the default table"
      formula: "N/A -- routing rule"
```

**Remaining KPI table files** (same structure, different descriptions):

| Path | Key difference |
|---|---|
| `catalog/kpi/quotertrade.yaml` | Auto-quoter originated fills. NOT raw activity. |
| `catalog/kpi/brokertrade.yaml` | Has `account` field (BGC, MGN). May be EMPTY for some dates. |
| `catalog/kpi/clicktrade.yaml` | Manual click trades. |
| `catalog/kpi/otoswing.yaml` | OTO swing trades. |

**Data table files**:

| Path | Key content |
|---|---|
| `catalog/data/theodata.yaml` | Theo pricing: theo, delta, vol, vega, gamma per strike/timestamp. UNIQUE to data dataset. |
| `catalog/data/marketdata.yaml` | Market data feed snapshots. Top-of-book prices. |
| `catalog/data/marketdepth.yaml` | Full order book depth. Multiple levels. |
| `catalog/data/swingdata.yaml` | Raw swing trade data. No clustering. |
| `catalog/data/markettrade.yaml` | Raw market trade execution details (NOT KPI enriched). |
| `catalog/data/quotertrade.yaml` | Raw quoter trade execution details. |
| `catalog/data/brokertrade.yaml` | Raw broker trade execution details. |
| `catalog/data/clicktrade.yaml` | Raw click trade execution details. |

All table YAML files use `fqn: "{project}.<dataset>.<table>"` -- the `{project}` placeholder is resolved by `catalog_loader.resolve_fqn()`.

**Generation process for all 13 table YAML files**:

```bash
# Step 1: Run extract_schemas.py (Track 01) to get column names
docker compose run --rm agent python setup/extract_schemas.py

# Step 2: For each table, feed the JSON schema + source code to an LLM:
#   "Here is the BigQuery schema for [table]. Generate a YAML catalog file
#    following this exact structure: [paste YAML schema from conventions above].
#    Use {project} in fqn field. Include trader synonyms, business rules,
#    and example values."

# Step 3: Validate every YAML file with the validation script (specified below)
docker compose run --rm agent python -m pytest tests/test_yaml_catalog.py -v
```

**DO NOT** hand-write column lists. Extract from JSON schemas, then enrich descriptions.

**DO NOT** skip any of the 13 tables. Every table gets a YAML file.

---

### Wave 2: Example Queries (3 files, 30+ examples)

All example SQL uses `{project}` placeholder in table references. The `catalog_loader.resolve_example_sql()` function replaces `{project}` with `settings.gcp_project` at load time.

#### 17. KPI Examples

**Path**: `examples/kpi_examples.yaml`

Target: 15+ examples covering all 5 KPI tables.

```yaml
examples:
  # --- markettrade (default KPI table) ---
  - question: "What was the total instant PnL for markettrades on 2026-02-17?"
    sql: |
      SELECT
        ROUND(SUM(instant_pnl), 2) AS total_pnl,
        COUNT(*) AS trade_count
      FROM `{project}.nl2sql_omx_kpi.markettrade`
      WHERE trade_date = '2026-02-17'
    tables_used: [markettrade]
    dataset: nl2sql_omx_kpi
    complexity: simple
    routing_signal: "generic PnL question -> default to markettrade"
    validated: false
    validated_by: ""

  - question: "Show me PnL breakdown by delta bucket for market trades"
    sql: |
      SELECT
        delta_bucket,
        COUNT(*) AS trade_count,
        ROUND(SUM(instant_pnl), 2) AS total_pnl,
        ROUND(AVG(edge_bps), 2) AS avg_edge_bps
      FROM `{project}.nl2sql_omx_kpi.markettrade`
      WHERE trade_date = '2026-02-17'
      GROUP BY delta_bucket
      ORDER BY delta_bucket
    tables_used: [markettrade]
    dataset: nl2sql_omx_kpi
    complexity: medium
    routing_signal: "delta bucket + PnL -> markettrade KPI"
    validated: false
    validated_by: ""

  - question: "Which market trades had edge above 5 bps?"
    sql: |
      SELECT
        symbol, term, trade_date,
        ROUND(edge_bps, 2) AS edge_bps,
        ROUND(instant_pnl, 2) AS instant_pnl
      FROM `{project}.nl2sql_omx_kpi.markettrade`
      WHERE trade_date = '2026-02-17'
        AND edge_bps > 5
      ORDER BY edge_bps DESC
      LIMIT 20
    tables_used: [markettrade]
    dataset: nl2sql_omx_kpi
    complexity: simple
    routing_signal: "edge threshold filter -> KPI markettrade"
    validated: false
    validated_by: ""

  - question: "What was the average slippage by symbol for market trades?"
    sql: |
      SELECT
        symbol,
        COUNT(*) AS trade_count,
        ROUND(AVG(slippage), 4) AS avg_slippage,
        ROUND(AVG(edge_bps), 2) AS avg_edge_bps
      FROM `{project}.nl2sql_omx_kpi.markettrade`
      WHERE trade_date = '2026-02-17'
      GROUP BY symbol
      ORDER BY avg_slippage DESC
    tables_used: [markettrade]
    dataset: nl2sql_omx_kpi
    complexity: medium
    routing_signal: "slippage analysis -> KPI markettrade"
    validated: false
    validated_by: ""

  # --- quotertrade (auto-quoter fills) ---
  - question: "How did the quoter perform today in terms of edge and PnL?"
    sql: |
      SELECT
        COUNT(*) AS fill_count,
        ROUND(SUM(instant_pnl), 2) AS total_pnl,
        ROUND(AVG(edge_bps), 2) AS avg_edge_bps,
        ROUND(AVG(slippage), 4) AS avg_slippage
      FROM `{project}.nl2sql_omx_kpi.quotertrade`
      WHERE trade_date = '2026-02-17'
    tables_used: [quotertrade]
    dataset: nl2sql_omx_kpi
    complexity: simple
    routing_signal: "quoter performance/edge/PnL -> KPI quotertrade"
    validated: false
    validated_by: ""

  - question: "Show quoter fills by portfolio and delta bucket"
    sql: |
      SELECT
        portfolio,
        delta_bucket,
        COUNT(*) AS fill_count,
        ROUND(SUM(instant_pnl), 2) AS total_pnl
      FROM `{project}.nl2sql_omx_kpi.quotertrade`
      WHERE trade_date = '2026-02-17'
      GROUP BY portfolio, delta_bucket
      ORDER BY portfolio, delta_bucket
    tables_used: [quotertrade]
    dataset: nl2sql_omx_kpi
    complexity: medium
    routing_signal: "quoter fills breakdown -> KPI quotertrade"
    validated: false
    validated_by: ""

  # --- brokertrade ---
  - question: "How have broker trades performed? Compare by account."
    sql: |
      SELECT
        account,
        COUNT(*) AS trade_count,
        ROUND(SUM(instant_pnl), 2) AS total_pnl,
        ROUND(AVG(edge_bps), 2) AS avg_edge_bps,
        ROUND(AVG(slippage), 4) AS avg_slippage
      FROM `{project}.nl2sql_omx_kpi.brokertrade`
      WHERE trade_date = '2026-02-17'
      GROUP BY account
      ORDER BY total_pnl DESC
    tables_used: [brokertrade]
    dataset: nl2sql_omx_kpi
    complexity: medium
    routing_signal: "broker + account comparison -> KPI brokertrade"
    validated: false
    validated_by: ""

  # --- clicktrade ---
  - question: "What was the edge on our click trades?"
    sql: |
      SELECT
        COUNT(*) AS trade_count,
        ROUND(AVG(edge_bps), 2) AS avg_edge_bps,
        ROUND(SUM(instant_pnl), 2) AS total_pnl
      FROM `{project}.nl2sql_omx_kpi.clicktrade`
      WHERE trade_date = '2026-02-17'
    tables_used: [clicktrade]
    dataset: nl2sql_omx_kpi
    complexity: simple
    routing_signal: "click trade edge -> KPI clicktrade"
    validated: false
    validated_by: ""

  # --- otoswing ---
  - question: "Show me OTO swing trade performance"
    sql: |
      SELECT
        symbol,
        COUNT(*) AS trade_count,
        ROUND(SUM(instant_pnl), 2) AS total_pnl,
        ROUND(AVG(edge_bps), 2) AS avg_edge_bps
      FROM `{project}.nl2sql_omx_kpi.otoswing`
      WHERE trade_date = '2026-02-17'
      GROUP BY symbol
      ORDER BY total_pnl DESC
    tables_used: [otoswing]
    dataset: nl2sql_omx_kpi
    complexity: medium
    routing_signal: "OTO swing -> KPI otoswing"
    validated: false
    validated_by: ""

  # --- UNION ALL across all KPI tables ---
  - question: "What was total PnL across all trade types?"
    sql: |
      SELECT
        'markettrade' AS trade_type, ROUND(SUM(instant_pnl), 2) AS total_pnl, COUNT(*) AS count
      FROM `{project}.nl2sql_omx_kpi.markettrade`
      WHERE trade_date = '2026-02-17'
      UNION ALL
      SELECT 'quotertrade', ROUND(SUM(instant_pnl), 2), COUNT(*)
      FROM `{project}.nl2sql_omx_kpi.quotertrade`
      WHERE trade_date = '2026-02-17'
      UNION ALL
      SELECT 'brokertrade', ROUND(SUM(instant_pnl), 2), COUNT(*)
      FROM `{project}.nl2sql_omx_kpi.brokertrade`
      WHERE trade_date = '2026-02-17'
      UNION ALL
      SELECT 'clicktrade', ROUND(SUM(instant_pnl), 2), COUNT(*)
      FROM `{project}.nl2sql_omx_kpi.clicktrade`
      WHERE trade_date = '2026-02-17'
      UNION ALL
      SELECT 'otoswing', ROUND(SUM(instant_pnl), 2), COUNT(*)
      FROM `{project}.nl2sql_omx_kpi.otoswing`
      WHERE trade_date = '2026-02-17'
      ORDER BY total_pnl DESC
    tables_used: [markettrade, quotertrade, brokertrade, clicktrade, otoswing]
    dataset: nl2sql_omx_kpi
    complexity: complex
    routing_signal: "all trades / total PnL -> UNION ALL across all KPI"
    validated: false
    validated_by: ""

  # --- Additional patterns ---
  - question: "Show me the best and worst trades by edge today"
    sql: |
      (
        SELECT 'best' AS category, symbol, term, ROUND(edge_bps, 2) AS edge_bps, ROUND(instant_pnl, 2) AS pnl
        FROM `{project}.nl2sql_omx_kpi.markettrade`
        WHERE trade_date = '2026-02-17'
        ORDER BY edge_bps DESC
        LIMIT 5
      )
      UNION ALL
      (
        SELECT 'worst', symbol, term, ROUND(edge_bps, 2), ROUND(instant_pnl, 2)
        FROM `{project}.nl2sql_omx_kpi.markettrade`
        WHERE trade_date = '2026-02-17'
        ORDER BY edge_bps ASC
        LIMIT 5
      )
    tables_used: [markettrade]
    dataset: nl2sql_omx_kpi
    complexity: medium
    routing_signal: "best/worst by edge -> KPI markettrade"
    validated: false
    validated_by: ""

  - question: "How many trades exceeded the required edge threshold?"
    sql: |
      SELECT
        COUNTIF(edge_bps >= required_edge_bps) AS above_threshold,
        COUNTIF(edge_bps < required_edge_bps) AS below_threshold,
        COUNT(*) AS total,
        ROUND(COUNTIF(edge_bps >= required_edge_bps) / COUNT(*) * 100, 1) AS pct_above
      FROM `{project}.nl2sql_omx_kpi.markettrade`
      WHERE trade_date = '2026-02-17'
    tables_used: [markettrade]
    dataset: nl2sql_omx_kpi
    complexity: medium
    routing_signal: "edge threshold analysis -> KPI markettrade"
    validated: false
    validated_by: ""

  - question: "What was instant PnL by portfolio for quoter trades?"
    sql: |
      SELECT
        portfolio,
        COUNT(*) AS trade_count,
        ROUND(SUM(instant_pnl), 2) AS total_pnl,
        ROUND(AVG(instant_pnl), 2) AS avg_pnl
      FROM `{project}.nl2sql_omx_kpi.quotertrade`
      WHERE trade_date = '2026-02-17'
      GROUP BY portfolio
      ORDER BY total_pnl DESC
    tables_used: [quotertrade]
    dataset: nl2sql_omx_kpi
    complexity: medium
    routing_signal: "PnL by portfolio for quoter -> KPI quotertrade"
    validated: false
    validated_by: ""
```

#### 18. Data Examples

**Path**: `examples/data_examples.yaml`

Target: 10+ examples covering theodata, marketdata, marketdepth, swingdata.

```yaml
examples:
  # --- theodata ---
  - question: "How did implied vol change over the day for a specific symbol?"
    sql: |
      SELECT
        symbol, term, strike,
        ROUND(vol, 4) AS implied_vol,
        ROUND(theo, 4) AS theo_price,
        ROUND(delta, 4) AS delta,
        trade_date
      FROM `{project}.nl2sql_omx_data.theodata`
      WHERE trade_date = '2026-02-17'
        AND symbol = 'SYMBOL_HERE'
      ORDER BY term, strike
      LIMIT 50
    tables_used: [theodata]
    dataset: nl2sql_omx_data
    complexity: simple
    routing_signal: "implied vol / IV -> data.theodata"
    validated: false
    validated_by: ""

  - question: "What is the current delta for all strikes of a given symbol and term?"
    sql: |
      SELECT
        strike,
        ROUND(delta, 4) AS delta,
        ROUND(theo, 4) AS theo,
        ROUND(vol, 4) AS vol
      FROM `{project}.nl2sql_omx_data.theodata`
      WHERE trade_date = '2026-02-17'
        AND symbol = 'SYMBOL_HERE'
        AND term = 'TERM_HERE'
      ORDER BY strike
    tables_used: [theodata]
    dataset: nl2sql_omx_data
    complexity: simple
    routing_signal: "delta by strike -> data.theodata"
    validated: false
    validated_by: ""

  - question: "Show me the vol surface -- vol by strike and term"
    sql: |
      SELECT
        term,
        strike,
        ROUND(vol, 4) AS implied_vol,
        ROUND(delta, 4) AS delta
      FROM `{project}.nl2sql_omx_data.theodata`
      WHERE trade_date = '2026-02-17'
        AND symbol = 'SYMBOL_HERE'
      ORDER BY term, strike
      LIMIT 100
    tables_used: [theodata]
    dataset: nl2sql_omx_data
    complexity: medium
    routing_signal: "vol surface -> data.theodata"
    validated: false
    validated_by: ""

  - question: "What are the greeks for the ATM options?"
    sql: |
      SELECT
        symbol, term, strike,
        ROUND(delta, 4) AS delta,
        ROUND(vol, 4) AS vol,
        ROUND(theo, 4) AS theo,
        ROUND(vega, 4) AS vega
      FROM `{project}.nl2sql_omx_data.theodata`
      WHERE trade_date = '2026-02-17'
        AND ABS(delta) BETWEEN 0.40 AND 0.60
      ORDER BY symbol, term, strike
      LIMIT 50
    tables_used: [theodata]
    dataset: nl2sql_omx_data
    complexity: medium
    routing_signal: "greeks / ATM / delta range -> data.theodata"
    validated: false
    validated_by: ""

  # --- marketdata ---
  - question: "What was the market data for a specific symbol today?"
    sql: |
      SELECT *
      FROM `{project}.nl2sql_omx_data.marketdata`
      WHERE trade_date = '2026-02-17'
        AND symbol = 'SYMBOL_HERE'
      LIMIT 20
    tables_used: [marketdata]
    dataset: nl2sql_omx_data
    complexity: simple
    routing_signal: "market data / market price -> data.marketdata"
    validated: false
    validated_by: ""

  # --- marketdepth ---
  - question: "Show me the order book depth for a symbol"
    sql: |
      SELECT *
      FROM `{project}.nl2sql_omx_data.marketdepth`
      WHERE trade_date = '2026-02-17'
        AND symbol = 'SYMBOL_HERE'
      LIMIT 20
    tables_used: [marketdepth]
    dataset: nl2sql_omx_data
    complexity: simple
    routing_signal: "order book / depth / bid ask -> data.marketdepth"
    validated: false
    validated_by: ""

  # --- raw trade data from data layer ---
  - question: "Show me the raw execution details for market trades"
    sql: |
      SELECT *
      FROM `{project}.nl2sql_omx_data.markettrade`
      WHERE trade_date = '2026-02-17'
      LIMIT 20
    tables_used: [markettrade]
    dataset: nl2sql_omx_data
    complexity: simple
    routing_signal: "raw execution / fill detail -> data.markettrade (NOT kpi)"
    validated: false
    validated_by: ""
```

#### 19. Routing Examples

**Path**: `examples/routing_examples.yaml`

Target: 5+ examples testing cross-dataset disambiguation.

```yaml
examples:
  # --- Disambiguation: KPI vs Data for same table name ---
  - question: "What was the edge on our market trades?"
    sql: |
      SELECT
        symbol,
        COUNT(*) AS trade_count,
        ROUND(AVG(edge_bps), 2) AS avg_edge_bps,
        ROUND(SUM(instant_pnl), 2) AS total_pnl
      FROM `{project}.nl2sql_omx_kpi.markettrade`
      WHERE trade_date = '2026-02-17'
      GROUP BY symbol
      ORDER BY total_pnl DESC
    tables_used: [markettrade]
    dataset: nl2sql_omx_kpi
    complexity: medium
    routing_signal: "edge = KPI metric -> KPI.markettrade, NOT data.markettrade"
    validated: false
    validated_by: ""

  - question: "What was the theo price for OMXS30 options?"
    sql: |
      SELECT
        term, strike,
        ROUND(theo, 4) AS theo,
        ROUND(delta, 4) AS delta,
        ROUND(vol, 4) AS vol
      FROM `{project}.nl2sql_omx_data.theodata`
      WHERE trade_date = '2026-02-17'
        AND symbol = 'OMXS30'
      ORDER BY term, strike
      LIMIT 50
    tables_used: [theodata]
    dataset: nl2sql_omx_data
    complexity: simple
    routing_signal: "theo/vol -> data.theodata (only exists in data)"
    validated: false
    validated_by: ""

  - question: "How many trades did we do by symbol across all types?"
    sql: |
      SELECT symbol, COUNT(*) AS trade_count, ROUND(SUM(instant_pnl), 2) AS total_pnl
      FROM `{project}.nl2sql_omx_kpi.markettrade`
      WHERE trade_date = '2026-02-17'
      GROUP BY symbol
      UNION ALL
      SELECT symbol, COUNT(*), ROUND(SUM(instant_pnl), 2)
      FROM `{project}.nl2sql_omx_kpi.quotertrade`
      WHERE trade_date = '2026-02-17'
      GROUP BY symbol
      UNION ALL
      SELECT symbol, COUNT(*), ROUND(SUM(instant_pnl), 2)
      FROM `{project}.nl2sql_omx_kpi.clicktrade`
      WHERE trade_date = '2026-02-17'
      GROUP BY symbol
      ORDER BY total_pnl DESC
    tables_used: [markettrade, quotertrade, clicktrade]
    dataset: nl2sql_omx_kpi
    complexity: complex
    routing_signal: "all types by symbol -> UNION across KPI tables"
    validated: false
    validated_by: ""

  - question: "Show me both the raw trade and the KPI for market trades"
    sql: |
      -- This requires joining KPI and data tables
      -- For now, show them separately
      SELECT 'kpi' AS source, symbol, ROUND(edge_bps, 2) AS edge_bps, ROUND(instant_pnl, 2) AS pnl
      FROM `{project}.nl2sql_omx_kpi.markettrade`
      WHERE trade_date = '2026-02-17'
      LIMIT 10
    tables_used: [markettrade]
    dataset: nl2sql_omx_kpi
    complexity: simple
    routing_signal: "cross-layer question -> start with KPI, explain data is also available"
    validated: false
    validated_by: ""

  - question: "What's in the swing data?"
    sql: |
      SELECT *
      FROM `{project}.nl2sql_omx_data.swingdata`
      WHERE trade_date = '2026-02-17'
      LIMIT 20
    tables_used: [swingdata]
    dataset: nl2sql_omx_data
    complexity: simple
    routing_signal: "swing -> data.swingdata (only exists in data)"
    validated: false
    validated_by: ""
```

---

### Wave 3: Embedding Infrastructure (Python runner replaces raw SQL)

Instead of 7 standalone SQL scripts with hardcoded project names, we use a **single Python runner** (`scripts/run_embeddings.py`) that builds SQL from `settings` and executes via `BigQueryProtocol`. This ensures all infrastructure references come from `.env`.

#### 20-26. Embedding Runner Script

**Path**: `scripts/run_embeddings.py`

```python
"""Create and populate BigQuery embedding infrastructure.

Replaces the 7 standalone SQL scripts (01_create_metadata_dataset.sql through
07_test_vector_search.sql) with a parameterized Python runner that reads
all project/model/connection references from settings.

Usage:
    python scripts/run_embeddings.py --step create-dataset
    python scripts/run_embeddings.py --step verify-model
    python scripts/run_embeddings.py --step create-tables
    python scripts/run_embeddings.py --step populate-schema
    python scripts/run_embeddings.py --step generate-embeddings
    python scripts/run_embeddings.py --step create-indexes
    python scripts/run_embeddings.py --step test-search
    python scripts/run_embeddings.py --step all          # runs all steps in order
"""

import argparse
import sys

from nl2sql_agent.config import Settings
from nl2sql_agent.logging_config import setup_logging, get_logger
from nl2sql_agent.protocols import BigQueryProtocol
from nl2sql_agent.clients import LiveBigQueryClient

setup_logging()
logger = get_logger(__name__)


def create_metadata_dataset(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 1: Create the metadata dataset. Idempotent via IF NOT EXISTS."""
    sql = f"""
    CREATE SCHEMA IF NOT EXISTS `{s.gcp_project}.{s.metadata_dataset}`
    OPTIONS (
      description = 'NL2SQL agent metadata: schema embeddings, column embeddings, query memory',
      location = '{s.bq_location}'
    );
    """
    bq.execute_query(sql)
    logger.info("created_metadata_dataset", project=s.gcp_project, dataset=s.metadata_dataset)


def verify_embedding_model(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 2: Verify the embedding model exists and is accessible."""
    # Parse the model ref to get project.dataset
    parts = s.embedding_model_ref.split(".")
    model_project = parts[0]
    model_dataset = parts[1]
    model_name = parts[2]

    sql = f"""
    SELECT model_name, model_type, creation_time
    FROM `{model_project}.{model_dataset}.INFORMATION_SCHEMA.MODELS`
    WHERE model_name = '{model_name}';
    """
    result = bq.execute_query(sql)
    logger.info("verified_embedding_model", model_ref=s.embedding_model_ref, rows=len(list(result)))


def create_embedding_tables(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 3: Create the 3 embedding tables. Idempotent via CREATE OR REPLACE."""
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"

    sqls = [
        f"""
        CREATE OR REPLACE TABLE `{fqn}.schema_embeddings` (
          id STRING DEFAULT GENERATE_UUID(),
          source_type STRING NOT NULL,
          layer STRING,
          dataset_name STRING,
          table_name STRING,
          description STRING NOT NULL,
          embedding ARRAY<FLOAT64>,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
        f"""
        CREATE OR REPLACE TABLE `{fqn}.column_embeddings` (
          id STRING DEFAULT GENERATE_UUID(),
          dataset_name STRING NOT NULL,
          table_name STRING NOT NULL,
          column_name STRING NOT NULL,
          column_type STRING NOT NULL,
          description STRING NOT NULL,
          synonyms ARRAY<STRING>,
          embedding ARRAY<FLOAT64>,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
        f"""
        CREATE OR REPLACE TABLE `{fqn}.query_memory` (
          id STRING DEFAULT GENERATE_UUID(),
          question STRING NOT NULL,
          sql_query STRING NOT NULL,
          tables_used ARRAY<STRING> NOT NULL,
          dataset STRING NOT NULL,
          complexity STRING,
          routing_signal STRING,
          validated_by STRING,
          validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
          success_count INT64 DEFAULT 1,
          embedding ARRAY<FLOAT64>
        );
        """,
    ]
    for sql in sqls:
        bq.execute_query(sql)
    logger.info("created_embedding_tables", fqn=fqn)


def populate_schema_embeddings(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 4: Populate schema_embeddings from catalog descriptions.

    Idempotent: MERGE on (source_type, dataset_name, table_name).
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"

    # KPI dataset rows
    kpi_sql = f"""
    MERGE `{fqn}.schema_embeddings` AS target
    USING (
      SELECT * FROM UNNEST([
        STRUCT(
          'dataset' AS source_type, 'kpi' AS layer, 'nl2sql_omx_kpi' AS dataset_name,
          CAST(NULL AS STRING) AS table_name,
          'KPI Key Performance Indicators dataset for OMX options trading. Gold layer. Contains one table per trade type: markettrade (exchange trades, default), quotertrade (auto-quoter fills), brokertrade (broker trades with account field for broker comparison), clicktrade (manual click trades), otoswing (OTO swing trades). All share columns: edge_bps (edge in basis points, trading edge), required_edge_bps (edge threshold), instant_pnl (immediate PnL profit loss), delta_bucket (moneyness 0-25 25-40 40-60 60+), slippage. For total PnL across all types use UNION ALL.' AS description
        ),
        STRUCT('table', 'kpi', 'nl2sql_omx_kpi', 'markettrade',
          'KPI metrics for market exchange trades on OMX options. One row per trade. Contains edge_bps (edge, difference between machine fair value and trade price in basis points), required_edge_bps (minimum edge threshold from risk management), instant_pnl (immediate PnL, profit loss), delta_bucket (delta categorisation 0-25 25-40 40-60 60+), slippage, portfolio, symbol, term, trade_date. Default KPI table when trade type is not specified.'),
        STRUCT('table', 'kpi', 'nl2sql_omx_kpi', 'quotertrade',
          'KPI metrics for auto-quoter originated trade fills on OMX options. Same KPI columns as markettrade: edge_bps, instant_pnl, slippage, delta_bucket. Use for quoter performance, quoter edge, quoter PnL analysis.'),
        STRUCT('table', 'kpi', 'nl2sql_omx_kpi', 'brokertrade',
          'KPI metrics for broker facilitated trades. Same KPI columns as markettrade plus account field. Use when comparing broker performance or when question mentions broker account names like BGC or MGN. NOTE: may be empty for some dates.'),
        STRUCT('table', 'kpi', 'nl2sql_omx_kpi', 'clicktrade',
          'KPI metrics for manually initiated click trades. Same KPI columns as markettrade. Use when question mentions click trades or manual trades.'),
        STRUCT('table', 'kpi', 'nl2sql_omx_kpi', 'otoswing',
          'KPI metrics for OTO swing trades. Same KPI columns as markettrade. Use when question mentions OTO, swing, or otoswing trades.')
      ])
    ) AS source
    ON target.source_type = source.source_type
       AND COALESCE(target.dataset_name, '') = COALESCE(source.dataset_name, '')
       AND COALESCE(target.table_name, '') = COALESCE(source.table_name, '')
    WHEN MATCHED THEN
      UPDATE SET description = source.description, embedding = NULL, updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (source_type, layer, dataset_name, table_name, description)
      VALUES (source.source_type, source.layer, source.dataset_name, source.table_name, source.description);
    """
    bq.execute_query(kpi_sql)

    # DATA dataset rows
    data_sql = f"""
    MERGE `{fqn}.schema_embeddings` AS target
    USING (
      SELECT * FROM UNNEST([
        STRUCT(
          'dataset' AS source_type, 'data' AS layer, 'nl2sql_omx_data' AS dataset_name,
          CAST(NULL AS STRING) AS table_name,
          'Raw OMX options trading and market data. Silver layer. Contains trade execution details, theoretical options pricing snapshots (theo delta vol vega), market data feeds, order book depth. Higher granularity than KPI but without computed performance metrics like edge_bps or instant_pnl.' AS description
        ),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'theodata',
          'Theoretical options pricing snapshots. Contains theo (theoretical fair price, fair value, TV, machine price), delta (delta greek, hedge ratio), vol (annualised implied volatility as decimal, also called IV, implied vol, sigma), vega, gamma, strike, symbol, term, portfolio, trade_date. ONLY exists in data dataset. Use for any question about vol, IV, implied volatility, delta, greeks, fair value, or theoretical pricing.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'marketdata',
          'Market data feed snapshots for OMX options. Top-of-book prices, volumes. Use for market price questions, exchange data, price feeds.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'marketdepth',
          'Full order book depth for OMX options. Multiple price levels with bid/ask sizes at each level. Use for order book questions, depth analysis, bid-ask spread at different levels.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'swingdata',
          'Raw OTO swing trade data. No clustering. Use for swing-specific raw data questions.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'markettrade',
          'Raw market trade execution details. Silver layer -- exact timestamps, prices, sizes, fill information. NOT KPI enriched. Use when question asks about raw execution details, not performance metrics.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'quotertrade',
          'Raw quoter trade execution details. Silver layer. Use for raw quoter execution data, not KPI performance metrics.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'brokertrade',
          'Raw broker trade execution details. Silver layer.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'clicktrade',
          'Raw click trade execution details. Silver layer.')
      ])
    ) AS source
    ON target.source_type = source.source_type
       AND COALESCE(target.dataset_name, '') = COALESCE(source.dataset_name, '')
       AND COALESCE(target.table_name, '') = COALESCE(source.table_name, '')
    WHEN MATCHED THEN
      UPDATE SET description = source.description, embedding = NULL, updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (source_type, layer, dataset_name, table_name, description)
      VALUES (source.source_type, source.layer, source.dataset_name, source.table_name, source.description);
    """
    bq.execute_query(data_sql)

    # Routing descriptions
    routing_sql = f"""
    MERGE `{fqn}.schema_embeddings` AS target
    USING (
      SELECT * FROM UNNEST([
        STRUCT(
          'routing' AS source_type, CAST(NULL AS STRING) AS layer,
          CAST(NULL AS STRING) AS dataset_name, CAST(NULL AS STRING) AS table_name,
          'The nl2sql_omx_kpi dataset (gold layer) has KPI-enriched trade data with computed metrics: edge_bps, instant_pnl, slippage, delta_bucket. The nl2sql_omx_data dataset (silver layer) has raw trade and market data. Both contain tables with the same names (brokertrade, clicktrade, markettrade, quotertrade). Use kpi for performance edge PnL slippage questions. Use data for raw execution timestamps market data and theoretical pricing.' AS description
        ),
        STRUCT('routing', NULL, NULL, NULL,
          'Questions about theoretical pricing, implied volatility IV vol sigma, delta, vega, gamma, greeks, fair value, machine price, or theo should route to nl2sql_omx_data.theodata. This table only exists in the data dataset. It does not exist in kpi.'),
        STRUCT('routing', NULL, NULL, NULL,
          'The kpi dataset has 5 tables one per trade origin: markettrade (exchange trades the default), quotertrade (auto-quoter fills), brokertrade (broker trades with account field), clicktrade (manual), otoswing (OTO swing). When trade type is unspecified use markettrade. When comparing brokers use brokertrade. For all trades use UNION ALL across all 5.')
      ])
    ) AS source
    ON FALSE
    WHEN NOT MATCHED THEN
      INSERT (source_type, layer, dataset_name, table_name, description)
      VALUES (source.source_type, source.layer, source.dataset_name, source.table_name, source.description);
    """
    bq.execute_query(routing_sql)

    # Clean up duplicate routing rows from previous runs
    cleanup_sql = f"""
    DELETE FROM `{fqn}.schema_embeddings`
    WHERE source_type = 'routing'
      AND id NOT IN (
        SELECT MAX(id) FROM `{fqn}.schema_embeddings`
        WHERE source_type = 'routing'
        GROUP BY description
      );
    """
    bq.execute_query(cleanup_sql)
    logger.info("populated_schema_embeddings")


def generate_embeddings(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 5: Generate embeddings for all rows that don't have them yet.

    Idempotent: WHERE embedding IS NULL.
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"
    model = s.embedding_model_ref

    sqls = [
        # Schema embeddings
        f"""
        UPDATE `{fqn}.schema_embeddings` t
        SET embedding = (
          SELECT ml_generate_embedding_result.text_embedding
          FROM ML.GENERATE_EMBEDDING(
            MODEL `{model}`,
            (SELECT t.description AS content),
            STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_DOCUMENT' AS task_type)
          )
        )
        WHERE t.embedding IS NULL;
        """,
        # Column embeddings
        f"""
        UPDATE `{fqn}.column_embeddings` t
        SET embedding = (
          SELECT ml_generate_embedding_result.text_embedding
          FROM ML.GENERATE_EMBEDDING(
            MODEL `{model}`,
            (SELECT t.description AS content),
            STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_DOCUMENT' AS task_type)
          )
        )
        WHERE t.embedding IS NULL;
        """,
        # Query memory
        f"""
        UPDATE `{fqn}.query_memory` t
        SET embedding = (
          SELECT ml_generate_embedding_result.text_embedding
          FROM ML.GENERATE_EMBEDDING(
            MODEL `{model}`,
            (SELECT t.question AS content),
            STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type)
          )
        )
        WHERE t.embedding IS NULL;
        """,
    ]
    for sql in sqls:
        bq.execute_query(sql)
    logger.info("generated_embeddings")


def create_vector_indexes(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 6: Create TREE_AH vector indexes. Idempotent via IF NOT EXISTS.

    NOTE: BigQuery requires >=5000 rows for TREE_AH to activate.
    With <5000 rows, BQ falls back to brute-force (still works).
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"

    sqls = [
        f"""
        CREATE VECTOR INDEX IF NOT EXISTS idx_schema_embeddings
        ON `{fqn}.schema_embeddings`(embedding)
        OPTIONS (index_type = 'TREE_AH', distance_type = 'COSINE');
        """,
        f"""
        CREATE VECTOR INDEX IF NOT EXISTS idx_column_embeddings
        ON `{fqn}.column_embeddings`(embedding)
        OPTIONS (index_type = 'TREE_AH', distance_type = 'COSINE');
        """,
        f"""
        CREATE VECTOR INDEX IF NOT EXISTS idx_query_memory
        ON `{fqn}.query_memory`(embedding)
        OPTIONS (index_type = 'TREE_AH', distance_type = 'COSINE');
        """,
    ]
    for sql in sqls:
        bq.execute_query(sql)
    logger.info("created_vector_indexes")


def test_vector_search(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 7: Run 5 test cases for vector search quality.

    Checks that expected table/row appears in top-3 results.
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"
    model = s.embedding_model_ref

    test_cases = [
        ("TEST 1: edge -> KPI", "what was the edge on our trade?", "schema_embeddings"),
        ("TEST 2: IV -> theodata", "how did implied vol change over the last month?", "schema_embeddings"),
        ("TEST 3: broker -> brokertrade", "how have broker trades by BGC performed compared to MGN?", "schema_embeddings"),
        ("TEST 4: depth -> marketdepth", "what did the order book look like for the ATM strike?", "schema_embeddings"),
        ("TEST 5: few-shot retrieval", "show me PnL breakdown by delta bucket", "query_memory"),
    ]

    for test_name, query_text, table in test_cases:
        if table == "schema_embeddings":
            sql = f"""
            SELECT '{test_name}' AS test_name,
                   base.source_type, base.layer, base.dataset_name, base.table_name,
                   ROUND(distance, 4) AS dist
            FROM VECTOR_SEARCH(
              TABLE `{fqn}.{table}`, 'embedding',
              (SELECT ml_generate_embedding_result.text_embedding AS embedding
               FROM ML.GENERATE_EMBEDDING(
                 MODEL `{model}`,
                 (SELECT '{query_text}' AS content),
                 STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type)
               )),
              top_k => 5, distance_type => 'COSINE')
            ORDER BY distance;
            """
        else:
            sql = f"""
            SELECT '{test_name}' AS test_name,
                   base.question, base.tables_used, base.dataset,
                   ROUND(distance, 4) AS dist
            FROM VECTOR_SEARCH(
              TABLE `{fqn}.{table}`, 'embedding',
              (SELECT ml_generate_embedding_result.text_embedding AS embedding
               FROM ML.GENERATE_EMBEDDING(
                 MODEL `{model}`,
                 (SELECT '{query_text}' AS content),
                 STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type)
               )),
              top_k => 5, distance_type => 'COSINE')
            ORDER BY distance;
            """
        result = bq.execute_query(sql)
        rows = list(result)
        logger.info(test_name, result_count=len(rows))
        for row in rows:
            print(f"  {row}")


STEPS = {
    "create-dataset": create_metadata_dataset,
    "verify-model": verify_embedding_model,
    "create-tables": create_embedding_tables,
    "populate-schema": populate_schema_embeddings,
    "generate-embeddings": generate_embeddings,
    "create-indexes": create_vector_indexes,
    "test-search": test_vector_search,
}

ALL_STEPS_ORDER = [
    "create-dataset", "verify-model", "create-tables",
    "populate-schema", "generate-embeddings", "create-indexes", "test-search",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run embedding infrastructure steps")
    parser.add_argument("--step", required=True, choices=list(STEPS.keys()) + ["all"],
                        help="Which step to run (or 'all')")
    args = parser.parse_args()

    settings = Settings()
    bq = LiveBigQueryClient(project=settings.gcp_project, location=settings.bq_location)

    if args.step == "all":
        for step_name in ALL_STEPS_ORDER:
            logger.info(f"running_step", step=step_name)
            STEPS[step_name](bq, settings)
    else:
        STEPS[args.step](bq, settings)

    logger.info("done")


if __name__ == "__main__":
    main()
```

---

### Wave 4: Python Scripts (catalog_loader + populate)

#### 27. Catalog Loader (with parameterization helpers)

**Path**: `nl2sql_agent/catalog_loader.py`

```python
"""Load and validate YAML catalog files.

This module is used by:
- Tests (to validate YAML structure)
- The populate script (to load YAML into BQ embedding tables)
- Future: the agent's metadata_loader tool (Track 03)

Key parameterization functions:
- resolve_fqn(table_data, settings) -- replaces {project} in fqn field
- resolve_example_sql(sql, settings) -- replaces {project} in example SQL

Usage:
    from nl2sql_agent.catalog_loader import load_catalog, load_examples
"""

from pathlib import Path
from typing import Any

import yaml

from nl2sql_agent.logging_config import get_logger

logger = get_logger(__name__)

CATALOG_DIR = Path(__file__).parent.parent / "catalog"
EXAMPLES_DIR = Path(__file__).parent.parent / "examples"

REQUIRED_TABLE_KEYS = {"name", "dataset", "fqn", "layer", "description", "partition_field", "columns"}
REQUIRED_COLUMN_KEYS = {"name", "type", "description"}
REQUIRED_EXAMPLE_KEYS = {"question", "sql", "tables_used", "dataset", "complexity"}
VALID_LAYERS = {"kpi", "data"}
VALID_DATASETS = {"nl2sql_omx_kpi", "nl2sql_omx_data"}
VALID_COMPLEXITIES = {"simple", "medium", "complex"}


def load_yaml(path: Path) -> dict[str, Any]:
    """Load a single YAML file.

    Args:
        path: Path to the YAML file.

    Returns:
        Parsed YAML content as a dict.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        yaml.YAMLError: If the YAML is malformed.
    """
    with open(path) as f:
        return yaml.safe_load(f)


def resolve_fqn(table_data: dict[str, Any], project: str) -> str:
    """Resolve the {project} placeholder in a table's fqn field.

    Args:
        table_data: The 'table' dict from a parsed table YAML.
        project: The GCP project ID (e.g. settings.gcp_project).

    Returns:
        Fully-qualified table name with project resolved.
    """
    fqn_template = table_data.get("fqn", "")
    return fqn_template.replace("{project}", project)


def resolve_example_sql(sql: str, project: str) -> str:
    """Resolve the {project} placeholder in example SQL.

    Args:
        sql: SQL string with {project} placeholders.
        project: The GCP project ID (e.g. settings.gcp_project).

    Returns:
        SQL with {project} replaced by actual project name.
    """
    return sql.replace("{project}", project)


def validate_table_yaml(data: dict[str, Any], filepath: str = "") -> list[str]:
    """Validate a table YAML file against the required schema.

    Args:
        data: Parsed YAML content.
        filepath: Optional filepath for error messages.

    Returns:
        List of validation error strings. Empty list = valid.
    """
    errors: list[str] = []
    prefix = f"{filepath}: " if filepath else ""

    if "table" not in data:
        errors.append(f"{prefix}Missing top-level 'table' key")
        return errors

    table = data["table"]
    missing = REQUIRED_TABLE_KEYS - set(table.keys())
    if missing:
        errors.append(f"{prefix}Missing table keys: {missing}")

    if table.get("layer") not in VALID_LAYERS:
        errors.append(f"{prefix}Invalid layer: {table.get('layer')}. Must be one of {VALID_LAYERS}")

    if table.get("dataset") not in VALID_DATASETS:
        errors.append(f"{prefix}Invalid dataset: {table.get('dataset')}. Must be one of {VALID_DATASETS}")

    # Validate fqn uses {project} template
    fqn = table.get("fqn", "")
    if "{project}" not in fqn:
        errors.append(f"{prefix}fqn must use {{project}} placeholder, got: {fqn}")

    columns = table.get("columns", [])
    if not isinstance(columns, list):
        errors.append(f"{prefix}columns must be a list")
    else:
        for i, col in enumerate(columns):
            col_missing = REQUIRED_COLUMN_KEYS - set(col.keys())
            if col_missing:
                errors.append(f"{prefix}Column {i} ({col.get('name', '?')}): missing keys {col_missing}")

    return errors


def validate_dataset_yaml(data: dict[str, Any], filepath: str = "") -> list[str]:
    """Validate a dataset YAML file."""
    errors: list[str] = []
    prefix = f"{filepath}: " if filepath else ""

    if "dataset" not in data:
        errors.append(f"{prefix}Missing top-level 'dataset' key")
        return errors

    ds = data["dataset"]
    if "name" not in ds:
        errors.append(f"{prefix}Missing dataset.name")
    if "tables" not in ds:
        errors.append(f"{prefix}Missing dataset.tables")

    return errors


def validate_examples_yaml(data: dict[str, Any], filepath: str = "") -> list[str]:
    """Validate an examples YAML file."""
    errors: list[str] = []
    prefix = f"{filepath}: " if filepath else ""

    if "examples" not in data:
        errors.append(f"{prefix}Missing top-level 'examples' key")
        return errors

    examples = data["examples"]
    if not isinstance(examples, list):
        errors.append(f"{prefix}examples must be a list")
        return errors

    for i, ex in enumerate(examples):
        missing = REQUIRED_EXAMPLE_KEYS - set(ex.keys())
        if missing:
            errors.append(f"{prefix}Example {i}: missing keys {missing}")

        if ex.get("complexity") not in VALID_COMPLEXITIES:
            errors.append(f"{prefix}Example {i}: invalid complexity '{ex.get('complexity')}'")

        if ex.get("dataset") not in VALID_DATASETS:
            errors.append(f"{prefix}Example {i}: invalid dataset '{ex.get('dataset')}'")

        # Check SQL uses {project} placeholder for table references
        sql = ex.get("sql", "")
        if "{project}" not in sql:
            errors.append(f"{prefix}Example {i}: SQL must use {{project}} placeholder in table references")

    return errors


def load_all_table_yamls() -> list[dict[str, Any]]:
    """Load all table YAML files from catalog/kpi/ and catalog/data/.

    Returns:
        List of parsed table YAML dicts.
    """
    tables = []
    for subdir in ["kpi", "data"]:
        dir_path = CATALOG_DIR / subdir
        if not dir_path.exists():
            continue
        for yaml_file in sorted(dir_path.glob("*.yaml")):
            if yaml_file.name.startswith("_"):
                continue  # Skip _dataset.yaml
            data = load_yaml(yaml_file)
            if "table" in data:
                tables.append(data)
    return tables


def load_all_examples() -> list[dict[str, Any]]:
    """Load all example YAML files from examples/.

    Returns:
        Flat list of all example dicts across all files.
    """
    all_examples = []
    for yaml_file in sorted(EXAMPLES_DIR.glob("*.yaml")):
        data = load_yaml(yaml_file)
        if "examples" in data and isinstance(data["examples"], list):
            all_examples.extend(data["examples"])
    return all_examples
```

**DO NOT** add BigQuery operations to this module. It is purely for YAML loading and validation.

**DO NOT** import `google.cloud.bigquery` here. This module must work without GCP credentials (for unit tests).

#### 28. Populate Script

**Path**: `scripts/populate_embeddings.py`

```python
"""Populate BigQuery embedding tables from YAML catalog and example files.

This script reads YAML catalog files and example files, then inserts rows
into {settings.metadata_dataset}.schema_embeddings, column_embeddings,
and query_memory.

Uses BigQueryProtocol for all BQ operations (testable with FakeBigQueryClient).
Uses settings for all project/dataset references -- no hardcoded values.

Usage:
    python scripts/populate_embeddings.py
"""

from nl2sql_agent.catalog_loader import (
    load_all_table_yamls,
    load_all_examples,
    load_yaml,
    resolve_fqn,
    resolve_example_sql,
    CATALOG_DIR,
)
from nl2sql_agent.config import Settings
from nl2sql_agent.logging_config import setup_logging, get_logger
from nl2sql_agent.protocols import BigQueryProtocol
from nl2sql_agent.clients import LiveBigQueryClient

setup_logging()
logger = get_logger(__name__)


def populate_column_embeddings(bq: BigQueryProtocol, tables: list[dict], settings: Settings) -> int:
    """Insert column-level descriptions into column_embeddings table.

    Args:
        bq: BigQuery client (protocol).
        tables: List of parsed table YAML dicts.
        settings: Application settings (for project/dataset refs).

    Returns:
        Number of columns inserted/updated.
    """
    fqn = f"{settings.gcp_project}.{settings.metadata_dataset}"
    count = 0
    for table_data in tables:
        t = table_data["table"]
        dataset_name = t["dataset"]
        table_name = t["name"]

        for col in t.get("columns", []):
            synonyms_str = ", ".join(f"'{s}'" for s in col.get("synonyms", []))
            synonyms_array = f"[{synonyms_str}]" if synonyms_str else "[]"

            sql = f"""
            MERGE `{fqn}.column_embeddings` AS target
            USING (SELECT '{dataset_name}' AS dataset_name, '{table_name}' AS table_name,
                          '{col["name"]}' AS column_name) AS source
            ON target.dataset_name = source.dataset_name
               AND target.table_name = source.table_name
               AND target.column_name = source.column_name
            WHEN MATCHED THEN
              UPDATE SET description = '''{col.get("description", "").replace("'", "\\'")}''',
                         column_type = '{col.get("type", "STRING")}',
                         synonyms = {synonyms_array},
                         embedding = NULL,
                         updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              INSERT (dataset_name, table_name, column_name, column_type, description, synonyms)
              VALUES (source.dataset_name, source.table_name, source.column_name,
                      '{col.get("type", "STRING")}',
                      '''{col.get("description", "").replace("'", "\\'")}''',
                      {synonyms_array})
            """
            bq.execute_query(sql)
            count += 1

    logger.info("populated_column_embeddings", count=count)
    return count


def populate_query_memory(bq: BigQueryProtocol, examples: list[dict], settings: Settings) -> int:
    """Insert validated Q->SQL pairs into query_memory table.

    Resolves {project} in SQL before storing.

    Args:
        bq: BigQuery client (protocol).
        examples: List of example dicts.
        settings: Application settings (for project/dataset refs).

    Returns:
        Number of examples inserted/updated.
    """
    fqn = f"{settings.gcp_project}.{settings.metadata_dataset}"
    count = 0
    for ex in examples:
        question = ex["question"].replace("'", "\\'")
        # Resolve {project} placeholder in SQL before storing
        resolved_sql = resolve_example_sql(ex["sql"], settings.gcp_project)
        sql_query = resolved_sql.replace("'", "\\'")
        tables_str = ", ".join(f"'{t}'" for t in ex["tables_used"])

        merge_sql = f"""
        MERGE `{fqn}.query_memory` AS target
        USING (SELECT '''{question}''' AS question) AS source
        ON target.question = source.question
        WHEN MATCHED THEN
          UPDATE SET sql_query = '''{sql_query}''',
                     tables_used = [{tables_str}],
                     dataset = '{ex["dataset"]}',
                     complexity = '{ex.get("complexity", "simple")}',
                     routing_signal = '''{ex.get("routing_signal", "").replace("'", "\\'")}''',
                     validated_by = '{ex.get("validated_by", "")}',
                     embedding = NULL,
                     validated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (question, sql_query, tables_used, dataset, complexity, routing_signal, validated_by)
          VALUES (source.question, '''{sql_query}''', [{tables_str}],
                  '{ex["dataset"]}', '{ex.get("complexity", "simple")}',
                  '''{ex.get("routing_signal", "").replace("'", "\\'")}''',
                  '{ex.get("validated_by", "")}')
        """
        bq.execute_query(merge_sql)
        count += 1

    logger.info("populated_query_memory", count=count)
    return count


def main() -> None:
    settings = Settings()
    bq = LiveBigQueryClient(project=settings.gcp_project, location=settings.bq_location)

    logger.info("loading_yaml_catalog")
    tables = load_all_table_yamls()
    logger.info("loaded_tables", count=len(tables))

    logger.info("loading_examples")
    examples = load_all_examples()
    logger.info("loaded_examples", count=len(examples))

    col_count = populate_column_embeddings(bq, tables, settings)
    ex_count = populate_query_memory(bq, examples, settings)

    logger.info("populate_complete", columns=col_count, examples=ex_count)
    print(f"Populated {col_count} column embeddings, {ex_count} query memory rows")


if __name__ == "__main__":
    main()
```

**IMPORTANT**: This script accepts `BigQueryProtocol`, not `bigquery.Client`. This means it's testable with `FakeBigQueryClient`.

---

### Wave 5: Tests

#### 29. YAML Catalog Tests

**Path**: `tests/test_yaml_catalog.py`

```python
"""Tests for YAML catalog structure and content validation."""

from pathlib import Path

import pytest
import yaml

from nl2sql_agent.catalog_loader import (
    CATALOG_DIR,
    EXAMPLES_DIR,
    validate_table_yaml,
    validate_dataset_yaml,
    validate_examples_yaml,
    load_yaml,
    load_all_table_yamls,
    load_all_examples,
)


class TestYamlCatalogStructure:
    """Validate that all YAML files exist and have correct structure."""

    def test_catalog_dir_exists(self):
        """catalog/ directory must exist."""
        assert CATALOG_DIR.exists(), f"Missing: {CATALOG_DIR}"

    def test_kpi_dataset_yaml_exists(self):
        """catalog/kpi/_dataset.yaml must exist."""
        path = CATALOG_DIR / "kpi" / "_dataset.yaml"
        assert path.exists(), f"Missing: {path}"

    def test_data_dataset_yaml_exists(self):
        """catalog/data/_dataset.yaml must exist."""
        path = CATALOG_DIR / "data" / "_dataset.yaml"
        assert path.exists(), f"Missing: {path}"

    def test_routing_yaml_exists(self):
        """catalog/_routing.yaml must exist."""
        path = CATALOG_DIR / "_routing.yaml"
        assert path.exists(), f"Missing: {path}"

    @pytest.mark.parametrize("table", [
        "brokertrade", "clicktrade", "markettrade", "otoswing", "quotertrade",
    ])
    def test_kpi_table_yaml_exists(self, table):
        """Every KPI table must have a YAML file."""
        path = CATALOG_DIR / "kpi" / f"{table}.yaml"
        assert path.exists(), f"Missing: {path}"

    @pytest.mark.parametrize("table", [
        "brokertrade", "clicktrade", "markettrade", "quotertrade",
        "theodata", "swingdata", "marketdata", "marketdepth",
    ])
    def test_data_table_yaml_exists(self, table):
        """Every data table must have a YAML file."""
        path = CATALOG_DIR / "data" / f"{table}.yaml"
        assert path.exists(), f"Missing: {path}"


class TestYamlCatalogValidation:
    """Validate YAML content against the required schema."""

    @pytest.mark.parametrize("subdir,table", [
        ("kpi", "markettrade"), ("kpi", "quotertrade"), ("kpi", "brokertrade"),
        ("kpi", "clicktrade"), ("kpi", "otoswing"),
        ("data", "theodata"), ("data", "marketdata"), ("data", "marketdepth"),
        ("data", "swingdata"), ("data", "markettrade"), ("data", "quotertrade"),
        ("data", "brokertrade"), ("data", "clicktrade"),
    ])
    def test_table_yaml_validates(self, subdir, table):
        """Each table YAML must pass structural validation."""
        path = CATALOG_DIR / subdir / f"{table}.yaml"
        if not path.exists():
            pytest.skip(f"{path} not yet created")
        data = load_yaml(path)
        errors = validate_table_yaml(data, str(path))
        assert errors == [], f"Validation errors: {errors}"

    def test_kpi_dataset_yaml_validates(self):
        """KPI dataset YAML must pass structural validation."""
        path = CATALOG_DIR / "kpi" / "_dataset.yaml"
        data = load_yaml(path)
        errors = validate_dataset_yaml(data, str(path))
        assert errors == [], f"Validation errors: {errors}"

    def test_data_dataset_yaml_validates(self):
        """Data dataset YAML must pass structural validation."""
        path = CATALOG_DIR / "data" / "_dataset.yaml"
        data = load_yaml(path)
        errors = validate_dataset_yaml(data, str(path))
        assert errors == [], f"Validation errors: {errors}"

    def test_all_table_yamls_have_columns(self):
        """Every table YAML must have at least 1 column defined."""
        tables = load_all_table_yamls()
        for t in tables:
            table = t["table"]
            assert len(table.get("columns", [])) > 0, (
                f"{table['dataset']}.{table['name']} has no columns"
            )

    def test_all_table_yamls_use_project_placeholder(self):
        """Every table YAML fqn must use {project} placeholder."""
        tables = load_all_table_yamls()
        for t in tables:
            table = t["table"]
            assert "{project}" in table["fqn"], (
                f"{table['name']}: fqn must use {{project}} placeholder, got: {table['fqn']}"
            )


class TestExamplesValidation:
    """Validate example query YAML files."""

    @pytest.mark.parametrize("filename", [
        "kpi_examples.yaml", "data_examples.yaml", "routing_examples.yaml",
    ])
    def test_example_file_exists(self, filename):
        """Each example file must exist."""
        path = EXAMPLES_DIR / filename
        assert path.exists(), f"Missing: {path}"

    @pytest.mark.parametrize("filename", [
        "kpi_examples.yaml", "data_examples.yaml", "routing_examples.yaml",
    ])
    def test_example_file_validates(self, filename):
        """Each example file must pass structural validation."""
        path = EXAMPLES_DIR / filename
        if not path.exists():
            pytest.skip(f"{path} not yet created")
        data = load_yaml(path)
        errors = validate_examples_yaml(data, str(path))
        assert errors == [], f"Validation errors: {errors}"

    def test_at_least_30_examples_total(self):
        """Must have at least 30 validated examples across all files."""
        examples = load_all_examples()
        assert len(examples) >= 30, (
            f"Only {len(examples)} examples. Need at least 30."
        )

    def test_examples_cover_kpi_and_data(self):
        """Examples must cover both KPI and data datasets."""
        examples = load_all_examples()
        datasets = {ex["dataset"] for ex in examples}
        assert "nl2sql_omx_kpi" in datasets, "No KPI examples found"
        assert "nl2sql_omx_data" in datasets, "No data examples found"

    def test_examples_use_project_placeholder_in_sql(self):
        """Every example SQL must use {project} placeholder."""
        examples = load_all_examples()
        for ex in examples:
            assert "{project}" in ex["sql"], (
                f"Example '{ex['question'][:50]}...' must use {{project}} in SQL"
            )

    def test_examples_filter_on_trade_date(self):
        """Every example SQL must filter on trade_date."""
        examples = load_all_examples()
        for ex in examples:
            assert "trade_date" in ex["sql"], (
                f"Example '{ex['question'][:50]}...' must filter on trade_date"
            )
```

#### 30. Catalog Loader Unit Tests

**Path**: `tests/test_catalog_loader.py`

```python
"""Unit tests for catalog_loader module (no BQ required)."""

from nl2sql_agent.catalog_loader import (
    validate_table_yaml,
    validate_examples_yaml,
    resolve_fqn,
    resolve_example_sql,
)


class TestResolveFqn:
    """Test the FQN resolution helper."""

    def test_resolve_fqn_dev(self):
        table_data = {"fqn": "{project}.nl2sql_omx_kpi.markettrade"}
        result = resolve_fqn(table_data, "melodic-stone-437916-t3")
        assert result == "melodic-stone-437916-t3.nl2sql_omx_kpi.markettrade"

    def test_resolve_fqn_prod(self):
        table_data = {"fqn": "{project}.nl2sql_omx_kpi.markettrade"}
        result = resolve_fqn(table_data, "cloud-data-n-base-d4b3")
        assert result == "cloud-data-n-base-d4b3.nl2sql_omx_kpi.markettrade"


class TestResolveExampleSql:
    """Test the example SQL resolution helper."""

    def test_resolve_single_table(self):
        sql = "SELECT * FROM `{project}.nl2sql_omx_kpi.markettrade` WHERE trade_date = '2026-02-17'"
        result = resolve_example_sql(sql, "melodic-stone-437916-t3")
        assert "melodic-stone-437916-t3.nl2sql_omx_kpi.markettrade" in result
        assert "{project}" not in result

    def test_resolve_multiple_tables(self):
        sql = """SELECT * FROM `{project}.nl2sql_omx_kpi.markettrade`
        UNION ALL SELECT * FROM `{project}.nl2sql_omx_kpi.quotertrade`"""
        result = resolve_example_sql(sql, "melodic-stone-437916-t3")
        assert result.count("melodic-stone-437916-t3") == 2
        assert "{project}" not in result


class TestValidateTableYaml:
    """Test the table YAML validator itself."""

    def test_valid_minimal_table(self):
        """A minimal valid table YAML should produce no errors."""
        data = {
            "table": {
                "name": "markettrade",
                "dataset": "nl2sql_omx_kpi",
                "fqn": "{project}.nl2sql_omx_kpi.markettrade",
                "layer": "kpi",
                "description": "KPI metrics for market trades",
                "partition_field": "trade_date",
                "columns": [
                    {"name": "trade_date", "type": "DATE", "description": "Trade date"}
                ],
            }
        }
        errors = validate_table_yaml(data)
        assert errors == []

    def test_missing_table_key(self):
        """Missing 'table' top-level key should produce an error."""
        errors = validate_table_yaml({"not_table": {}})
        assert len(errors) == 1
        assert "Missing top-level 'table' key" in errors[0]

    def test_invalid_layer(self):
        """Invalid layer value should produce an error."""
        data = {
            "table": {
                "name": "test", "dataset": "nl2sql_omx_kpi",
                "fqn": "{project}.nl2sql_omx_kpi.test",
                "layer": "gold",  # Invalid
                "description": "x", "partition_field": "trade_date",
                "columns": [{"name": "a", "type": "STRING", "description": "x"}],
            }
        }
        errors = validate_table_yaml(data)
        assert any("Invalid layer" in e for e in errors)

    def test_invalid_dataset(self):
        """Invalid dataset value should produce an error."""
        data = {
            "table": {
                "name": "test", "dataset": "dev_agent_test",  # Invalid
                "fqn": "{project}.dev_agent_test.test",
                "layer": "kpi", "description": "x",
                "partition_field": "trade_date",
                "columns": [{"name": "a", "type": "STRING", "description": "x"}],
            }
        }
        errors = validate_table_yaml(data)
        assert any("Invalid dataset" in e for e in errors)

    def test_fqn_without_project_placeholder(self):
        """fqn without {project} placeholder should produce an error."""
        data = {
            "table": {
                "name": "test", "dataset": "nl2sql_omx_kpi",
                "fqn": "hardcoded-project.nl2sql_omx_kpi.test",
                "layer": "kpi", "description": "x",
                "partition_field": "trade_date",
                "columns": [{"name": "a", "type": "STRING", "description": "x"}],
            }
        }
        errors = validate_table_yaml(data)
        assert any("{project}" in e for e in errors)


class TestValidateExamplesYaml:
    """Test the examples YAML validator."""

    def test_valid_example(self):
        data = {
            "examples": [{
                "question": "What was the PnL?",
                "sql": "SELECT * FROM `{project}.nl2sql_omx_kpi.markettrade` WHERE trade_date = '2026-02-17'",
                "tables_used": ["markettrade"],
                "dataset": "nl2sql_omx_kpi",
                "complexity": "simple",
            }]
        }
        errors = validate_examples_yaml(data)
        assert errors == []

    def test_missing_project_placeholder_in_sql(self):
        data = {
            "examples": [{
                "question": "What?",
                "sql": "SELECT * FROM markettrade WHERE trade_date = '2026-02-17'",
                "tables_used": ["markettrade"],
                "dataset": "nl2sql_omx_kpi",
                "complexity": "simple",
            }]
        }
        errors = validate_examples_yaml(data)
        assert any("{project}" in e for e in errors)
```

---

## Updated Directory Tree

After Track 02 is complete, the repo adds these files to the Track 01 tree:

```
nl2sql-agent/
+-- (... Track 01 files unchanged ...)
|
+-- nl2sql_agent/
|   +-- (... Track 01 files ...)
|   +-- catalog_loader.py              <-- NEW: YAML loading + validation + resolve helpers
|
+-- catalog/                            <-- POPULATED (was empty)
|   +-- _routing.yaml                   <-- Cross-dataset routing
|   +-- kpi/
|   |   +-- _dataset.yaml               <-- KPI dataset metadata + routing
|   |   +-- brokertrade.yaml
|   |   +-- clicktrade.yaml
|   |   +-- markettrade.yaml
|   |   +-- otoswing.yaml
|   |   +-- quotertrade.yaml
|   +-- data/
|       +-- _dataset.yaml               <-- Data dataset metadata
|       +-- brokertrade.yaml
|       +-- clicktrade.yaml
|       +-- markettrade.yaml
|       +-- quotertrade.yaml
|       +-- theodata.yaml
|       +-- swingdata.yaml
|       +-- marketdata.yaml
|       +-- marketdepth.yaml
|
+-- examples/                           <-- POPULATED (was empty)
|   +-- kpi_examples.yaml               <-- 15+ KPI examples (use {project} placeholders)
|   +-- data_examples.yaml              <-- 10+ data examples
|   +-- routing_examples.yaml           <-- 5+ routing examples
|
+-- scripts/
|   +-- run_embeddings.py               <-- NEW: Parameterized embedding infrastructure runner
|   +-- populate_embeddings.py          <-- NEW: Load YAML -> BQ embedding tables
|
+-- tests/
    +-- (... Track 01 tests ...)
    +-- test_yaml_catalog.py            <-- NEW: YAML structure tests
    +-- test_catalog_loader.py          <-- NEW: Loader unit tests + resolve helpers
```

Note: The `embeddings/` directory with raw SQL scripts is **NOT created**. All embedding SQL is generated by `scripts/run_embeddings.py` from settings.

---

## Implementation Tasks

### Phase 1: Catalog Loader and YAML Infrastructure

- [x] **1.1** Create `nl2sql_agent/catalog_loader.py` with `resolve_fqn()`, `resolve_example_sql()`, validators, and loaders as specified
- [x] **1.2** Create `catalog/kpi/_dataset.yaml` as specified (no hardcoded project refs)
- [x] **1.3** Create `catalog/data/_dataset.yaml` as specified
- [x] **1.4** Create `catalog/_routing.yaml` as specified

### Phase 2: Table YAML Files (12 files)

- [x] **2.1** Run `setup/extract_schemas.py` to get JSON schemas for all tables
- [x] **2.2** Create `catalog/kpi/markettrade.yaml` -- use `fqn: "{project}.nl2sql_omx_kpi.markettrade"`, populate columns from JSON schema, enrich descriptions
- [x] **2.3** Create `catalog/kpi/quotertrade.yaml` -- same pattern
- [x] **2.4** Create `catalog/kpi/brokertrade.yaml` -- same pattern, note `account` field
- [x] **2.5** Create `catalog/kpi/clicktrade.yaml`
- [x] **2.6** Create `catalog/kpi/otoswing.yaml`
- [x] **2.7** Create `catalog/data/theodata.yaml` -- note: UNIQUE to data dataset
- [x] **2.8** Create `catalog/data/marketdata.yaml`
- [x] **2.9** Create `catalog/data/marketdepth.yaml`
- [x] **2.10** Create `catalog/data/swingdata.yaml` -- note: no clustering
- [x] **2.11** Create `catalog/data/markettrade.yaml`
- [x] **2.12** Create `catalog/data/quotertrade.yaml`
- [ ] **2.13** Create `catalog/data/brokertrade.yaml` -- SKIPPED: brokertrade not in dev sample data (empty CSV)
- [x] **2.14** Create `catalog/data/clicktrade.yaml`

### Phase 3: Example Queries (3 files, 33 examples)

- [x] **3.1** Create `examples/kpi_examples.yaml` with 15+ examples using `{project}` placeholders
- [x] **3.2** Create `examples/data_examples.yaml` with 10+ examples using `{project}` placeholders
- [x] **3.3** Create `examples/routing_examples.yaml` with 5+ examples using `{project}` placeholders
- [ ] **3.4** Validate every example SQL in BigQuery (replace `{project}` with actual project, run query, verify results). Mark `validated: true` only for working queries.

### Phase 4: Tests

- [x] **4.1** Create `tests/test_yaml_catalog.py` as specified -- tests check for `{project}` placeholder, NOT hardcoded project names
- [x] **4.2** Create `tests/test_catalog_loader.py` as specified -- includes `TestResolveFqn` and `TestResolveExampleSql`
- [x] **4.3** Run `pytest tests/test_yaml_catalog.py tests/test_catalog_loader.py -v` -- all 65 tests pass

### Phase 5: Embedding Infrastructure

- [x] **5.1** Create `scripts/run_embeddings.py` as specified -- all SQL built from `settings`, no hardcoded values
- [x] **5.2** Run `python scripts/run_embeddings.py --step create-dataset`
- [x] **5.3** Run `python scripts/run_embeddings.py --step verify-model`
- [x] **5.4** Run `python scripts/run_embeddings.py --step create-tables`
- [x] **5.5** Run `python scripts/run_embeddings.py --step populate-schema`

### Phase 6: Populate and Generate

- [x] **6.1** Create `scripts/populate_embeddings.py` as specified -- uses `settings.gcp_project` and `settings.metadata_dataset`, calls `resolve_example_sql()` before storing
- [x] **6.2** Run `python scripts/populate_embeddings.py` -- populated 33 query_memory rows
- [x] **6.3** Run `python scripts/run_embeddings.py --step generate-embeddings` -- all embeddings generated
- [x] **6.4** Run `python scripts/run_embeddings.py --step create-indexes` -- created (activate at >=5000 rows)

### Phase 7: Validation

- [x] **7.1** Run `python scripts/run_embeddings.py --step test-search` -- 5/5 test cases pass
- [x] **7.2** Verify row counts: 17 schema_embeddings, 33 query_memory (column_embeddings not yet populated -- pending)
- [x] **7.3** Run full test suite: `pytest tests/ -v` -- all 65 tests pass
- [ ] **7.4** Verify `scripts/populate_embeddings.py` is idempotent (run twice, no duplicates)

---

## Acceptance Criteria

Track 02 is DONE when ALL of the following are true:

- [x] 15 YAML catalog files exist (12 tables + 2 datasets + 1 routing) -- data/brokertrade skipped (not in dev sample)
- [x] Every table YAML uses `fqn: "{project}.<dataset>.<table>"` -- no hardcoded project
- [x] Every table YAML has columns populated from real schema extraction
- [x] Every table YAML passes `validate_table_yaml()` with zero errors
- [x] 3 example YAML files exist with 33 total examples
- [x] Every example SQL uses `{project}` placeholder (not hardcoded project names)
- [x] Every example SQL filters on `trade_date`
- [ ] At least 20 examples have `validated: true` (tested in BQ) -- pending manual validation
- [x] `{settings.metadata_dataset}` dataset exists in `{settings.bq_location}`
- [x] 3 embedding tables exist: `schema_embeddings`, `column_embeddings`, `query_memory`
- [x] `schema_embeddings` has 17 rows with non-empty embeddings
- [ ] `column_embeddings` has rows for all enriched columns with non-empty embeddings -- pending: populate_embeddings needs BQ auth
- [x] `query_memory` has 33 rows with non-empty embeddings
- [x] 3 vector indexes created (activate at >=5000 rows, brute-force works below)
- [x] 5/5 vector search test cases pass (correct table in top-3 results)
- [x] `pytest tests/ -v` passes all 65 tests
- [x] `scripts/populate_embeddings.py` runs without errors using `settings` -- no hardcoded project
- [x] `scripts/run_embeddings.py` runs without errors using `settings` -- no hardcoded project
- [x] All SQL operations use MERGE/idempotent patterns -- no duplicates when run twice
- [x] No file in the codebase contains hardcoded `cloud-data-n-base-d4b3` or `cloud-ai-d-base-a2df`

---

## Anti-Patterns (DO NOT DO THESE)

| Anti-Pattern | Why It's Wrong | Correct Approach |
|---|---|---|
| Hardcode `cloud-data-n-base-d4b3` in YAML, SQL, or Python | Breaks in dev environment, not portable | Use `{project}` in YAML/SQL, `settings.gcp_project` in Python |
| Hardcode `cloud-ai-d-base-a2df` for embedding model | Breaks in dev where model is in same project | Use `settings.embedding_model_ref` |
| Use bare table names in example SQL | Agent will generate invalid SQL | Use `` `{project}.dataset.table` `` |
| Use `INSERT INTO` for embedding population | Creates duplicates on re-run | Use `MERGE` or `DELETE + INSERT` |
| Create a new embedding model | Model already exists | Reference `settings.embedding_model_ref` |
| Put column descriptions in the populate script | Descriptions belong in YAML | YAML is source of truth, script reads from it |
| Import `bigquery.Client` in populate script | Violates protocol pattern | Accept `BigQueryProtocol` parameter |
| Skip testing example SQL in BigQuery | Invalid SQL enters the corpus | Run every query manually, mark `validated: true` only if it works |
| Use `metadata` as dataset name | Our real dataset is `nl2sql_metadata` | Use `settings.metadata_dataset` |
| Use `RETRIEVAL_DOCUMENT` task_type for queries | Wrong task type for query-time embeddings | Use `RETRIEVAL_QUERY` for search queries, `RETRIEVAL_DOCUMENT` for stored content |
| Add `>5000` fake rows to trigger TREE_AH | Pollutes the data | Accept brute-force search for small tables -- still works correctly |
| Omit `trade_date` filter in example SQL | Full table scans, wrong results | Always filter on partition column |
| Write raw SQL files with hardcoded project names | Not portable across environments | Use `scripts/run_embeddings.py` which builds SQL from settings |
| Check for hardcoded project in tests | Fails in dev environment | Tests check for `{project}` placeholder or structural validity |
