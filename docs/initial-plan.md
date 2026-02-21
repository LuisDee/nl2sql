# NL2SQL Agent: Enhanced Implementation Plan

## Architecture Summary

The NL2SQL agent is a **sub-agent** under a root Gemini conversational agent, not a standalone app. Traders talk to Gemini naturally. When a data question comes in, the root agent delegates to `nl2sql_agent`, which owns the entire pipeline: route → metadata → few-shot → generate SQL → validate → execute → learn.

```
Root Agent (mako_assistant — Gemini 2.5 Flash)
│   Handles: greetings, clarifications, follow-ups, non-data questions
│
├── nl2sql_agent (LlmAgent sub-agent)
│   Owns: the full data query pipeline
│   Tools:
│   ├── vector_search_tables      → route question to right table(s)
│   ├── vector_search_columns     → identify relevant columns
│   ├── fetch_few_shot_examples   → retrieve validated Q→SQL pairs
│   ├── load_yaml_metadata        → load YAML catalog for matched tables
│   ├── generate_sql              → LLM call with full context → SQL
│   ├── dry_run_sql               → BigQuery dry run validation
│   ├── execute_sql               → BigQuery read-only execution
│   └── save_validated_query      → learning loop
│
└── (future: compliance_agent, report_agent, etc.)
```

The root agent never sees SQL. The sub-agent never handles small talk. Clean separation.

---

## Tables In Scope

| Dataset | Table | Dev Table Name | Contents |
|---------|-------|---------------|----------|
| `volbox` | `theodata` | `dev_agent_test.theodata` | Theo pricing snapshots (theo, delta, vol, vega per strike/timestamp) |
| `kpi` | `markettrade` | `dev_agent_test.kpi_markettrade` | KPI for market/exchange trades |
| `kpi` | `quotertrade` | `dev_agent_test.kpi_quotertrade` | KPI for quoter fills |
| `kpi` | `brokertrade` | `dev_agent_test.kpi_brokertrade` | KPI for broker trades (has account: BGC, MGN) |
| `kpi` | `clicktrade` | `dev_agent_test.kpi_clicktrade` | KPI for click trades |
| `kpi` | `otoswing` | `dev_agent_test.kpi_otoswing` | KPI for OTO swing trades |
| `quoter` | `quotertrade` | `dev_agent_test.quoter_quotertrade` | Raw quoter activity (levels, sizes, timestamps) |

**Routing challenge:** `kpi.quotertrade` (KPI metrics) vs `quoter.quotertrade` (raw activity) are different tables. `kpi.*` tables share the same KPI columns but split by trade origin. The agent must distinguish all of these.

---

## Repo Structure

```
nl2sql-agent/
├── pyproject.toml
├── .env.example
├── README.md
├── AGENTS.md                        # Root-level agent documentation
│
├── agent/                           # ADK agent definitions
│   ├── __init__.py
│   ├── root_agent.py                # Root Gemini agent with sub-agent delegation
│   └── nl2sql/
│       ├── __init__.py
│       ├── agent.py                  # NL2SQL sub-agent definition
│       ├── prompts.py                # System instructions, SQL generation prompts
│       └── tools/
│           ├── __init__.py
│           ├── vector_search.py      # VECTOR_SEARCH against embedding tables
│           ├── metadata_loader.py    # Load YAML catalog files
│           ├── sql_generator.py      # LLM-based SQL generation with context
│           ├── sql_validator.py      # BigQuery dry run validation
│           ├── sql_executor.py       # BigQuery execution (read-only)
│           └── learning_loop.py      # Save validated queries → query_memory
│
├── catalog/                          # YAML metadata (Layer 1)
│   ├── _routing.yaml                 # KPI dataset routing rules
│   ├── volbox/
│   │   └── theodata.yaml
│   ├── kpi/
│   │   ├── _dataset.yaml             # Shared KPI column definitions
│   │   ├── markettrade.yaml
│   │   ├── quotertrade.yaml
│   │   ├── brokertrade.yaml
│   │   ├── clicktrade.yaml
│   │   └── otoswing.yaml
│   └── quoter/
│       └── quotertrade.yaml
│
├── examples/                         # Validated Q→SQL pairs (highest-ROI asset)
│   ├── theodata_examples.yaml
│   ├── kpi_examples.yaml
│   └── quoter_examples.yaml
│
├── embeddings/                       # SQL scripts for Layer 2
│   ├── 01_create_metadata_schema.sql
│   ├── 02_create_embedding_model.sql
│   ├── 03_create_embedding_tables.sql
│   ├── 04_populate_schema_embeddings.sql
│   ├── 05_populate_column_embeddings.sql
│   ├── 06_populate_query_memory.sql
│   ├── 07_generate_embeddings.sql
│   ├── 08_create_vector_indexes.sql
│   └── 09_test_vector_search.sql
│
├── setup/                            # One-time infra setup
│   ├── 01_create_dev_dataset.sql
│   ├── 02_copy_sample_data.sql
│   ├── 03_verify_data.sql
│   └── extract_schemas.py
│
└── eval/                             # Evaluation framework
    ├── gold_queries.yaml             # Gold-standard Q→SQL→result triples
    ├── run_eval.py                   # Accuracy measurement
    └── results/                      # Eval run outputs
```

---

## Phase A: Foundation

**Dependencies:** None. Start here.

### A.1 — Repo scaffolding

Create the directory structure above. Initialise with:

```bash
mkdir -p nl2sql-agent/{agent/nl2sql/tools,catalog/{volbox,kpi,quoter},examples,embeddings,setup,eval/results}
cd nl2sql-agent
git init
```

Create `pyproject.toml`:

```toml
[project]
name = "nl2sql-agent"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "google-adk",
    "google-cloud-bigquery",
    "google-cloud-aiplatform",
    "pyyaml",
    "python-dotenv",
]

[project.optional-dependencies]
dev = ["pytest", "pytest-asyncio"]
```

Create `.env.example`:

```env
GCP_PROJECT=your-project-id
BQ_LOCATION=EU
DEV_DATASET=dev_agent_test
METADATA_DATASET=metadata
VERTEX_AI_CONNECTION=EU.vertex-ai-connection
EMBEDDING_MODEL=text-embedding-005
SQL_GEN_MODEL=gemini-2.5-flash
SQL_GEN_MODEL_COMPLEX=gemini-2.5-pro
```

### A.2 — Dev dataset: create and populate

**File: `setup/01_create_dev_dataset.sql`**

```sql
CREATE SCHEMA IF NOT EXISTS dev_agent_test
OPTIONS (
  description = 'NL2SQL agent development sandbox — thin slices of production tables',
  location = 'EU'
);
```

**File: `setup/02_copy_sample_data.sql`**

Pick one liquid symbol. Replace `YOUR_SYMBOL` throughout.

```sql
-- volbox.theodata
CREATE OR REPLACE TABLE dev_agent_test.theodata AS
SELECT *
FROM volbox.theodata
WHERE symbol = 'YOUR_SYMBOL'
  AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

-- kpi tables (all 5 trade types)
CREATE OR REPLACE TABLE dev_agent_test.kpi_markettrade AS
SELECT *
FROM kpi.markettrade
WHERE symbol = 'YOUR_SYMBOL'
  AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

CREATE OR REPLACE TABLE dev_agent_test.kpi_quotertrade AS
SELECT *
FROM kpi.quotertrade
WHERE symbol = 'YOUR_SYMBOL'
  AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

CREATE OR REPLACE TABLE dev_agent_test.kpi_brokertrade AS
SELECT *
FROM kpi.brokertrade
WHERE symbol = 'YOUR_SYMBOL'
  AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

CREATE OR REPLACE TABLE dev_agent_test.kpi_clicktrade AS
SELECT *
FROM kpi.clicktrade
WHERE symbol = 'YOUR_SYMBOL'
  AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

CREATE OR REPLACE TABLE dev_agent_test.kpi_otoswing AS
SELECT *
FROM kpi.otoswing
WHERE symbol = 'YOUR_SYMBOL'
  AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

-- quoter.quotertrade (raw activity — different from kpi.quotertrade)
CREATE OR REPLACE TABLE dev_agent_test.quoter_quotertrade AS
SELECT *
FROM quoter.quotertrade
WHERE symbol = 'YOUR_SYMBOL'
  AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);
```

**File: `setup/03_verify_data.sql`**

```sql
-- Row counts
SELECT 'theodata' as tbl, COUNT(*) as rows FROM dev_agent_test.theodata
UNION ALL SELECT 'kpi_markettrade', COUNT(*) FROM dev_agent_test.kpi_markettrade
UNION ALL SELECT 'kpi_quotertrade', COUNT(*) FROM dev_agent_test.kpi_quotertrade
UNION ALL SELECT 'kpi_brokertrade', COUNT(*) FROM dev_agent_test.kpi_brokertrade
UNION ALL SELECT 'kpi_clicktrade', COUNT(*) FROM dev_agent_test.kpi_clicktrade
UNION ALL SELECT 'kpi_otoswing', COUNT(*) FROM dev_agent_test.kpi_otoswing
UNION ALL SELECT 'quoter_quotertrade', COUNT(*) FROM dev_agent_test.quoter_quotertrade;

-- Date range check
SELECT 'theodata' as tbl, MIN(DATE(timestamp)) as min_dt, MAX(DATE(timestamp)) as max_dt
FROM dev_agent_test.theodata
UNION ALL
SELECT 'kpi_markettrade', MIN(trade_date), MAX(trade_date)
FROM dev_agent_test.kpi_markettrade;

-- Sample values (inspect what strikes, option_types, delta_buckets look like)
SELECT DISTINCT strike FROM dev_agent_test.theodata ORDER BY strike LIMIT 20;
SELECT DISTINCT option_type FROM dev_agent_test.theodata;
SELECT DISTINCT delta_bucket FROM dev_agent_test.kpi_markettrade;

-- Check if kpi tables have the account field (needed for broker routing)
SELECT column_name, data_type
FROM dev_agent_test.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'kpi_brokertrade'
ORDER BY ordinal_position;
```

**Action after running:** Record the actual column names from INFORMATION_SCHEMA. Some column names may differ from what we've assumed (e.g., `trade_price` might be `price`, `edge_bps` might be `edge`). Everything downstream depends on getting the real column names right here.

### A.3 — Auto-extract schemas

**File: `setup/extract_schemas.py`**

```python
"""Extract schemas from dev_agent_test and save as JSON + basic YAML templates."""

import json
import yaml
from google.cloud import bigquery

PROJECT = "your-project-id"
DATASET = "dev_agent_test"
TABLES = [
    "theodata",
    "kpi_markettrade",
    "kpi_quotertrade",
    "kpi_brokertrade",
    "kpi_clicktrade",
    "kpi_otoswing",
    "quoter_quotertrade",
]

client = bigquery.Client(project=PROJECT)

for table_name in TABLES:
    table_ref = f"{PROJECT}.{DATASET}.{table_name}"
    table = client.get_table(table_ref)

    schema_json = [
        {
            "name": field.name,
            "type": field.field_type,
            "mode": field.mode,
            "description": field.description or "",
        }
        for field in table.schema
    ]

    # Save raw JSON schema
    with open(f"schemas/{table_name}.json", "w") as f:
        json.dump(schema_json, f, indent=2)

    # Generate YAML template (to be enriched manually)
    yaml_template = {
        "table": {
            "name": table_name,
            "dataset": DATASET,
            "description": f"TODO: Add description for {table_name}",
            "row_count_approx": table.num_rows,
            "columns": [
                {
                    "name": col["name"],
                    "type": col["type"],
                    "description": col["description"] or "TODO",
                    "synonyms": [],
                }
                for col in schema_json
            ],
        }
    }

    with open(f"catalog_templates/{table_name}.yaml", "w") as f:
        yaml.dump(yaml_template, f, default_flow_style=False, sort_keys=False)

    print(f"✓ {table_name}: {table.num_rows} rows, {len(schema_json)} columns")
```

Run this. It gives you raw JSON schemas (source of truth) and YAML templates with TODOs to fill in.

### A.4 — Agent skeleton (delegation works, no tools yet)

**File: `agent/__init__.py`**

```python
from .root_agent import root_agent
```

**File: `agent/root_agent.py`**

```python
from google.adk.agents import LlmAgent
from .nl2sql.agent import nl2sql_agent

root_agent = LlmAgent(
    name="mako_assistant",
    model="gemini-2.5-flash",
    description="Mako Group trading assistant.",
    instruction=(
        "You are a helpful assistant for Mako Group traders. "
        "For any questions about trading data, performance, KPIs, "
        "theo/vol analysis, quoter activity, edge, slippage, PnL, "
        "or anything that requires querying a database, delegate to nl2sql_agent. "
        "For general questions, greetings, or clarifications, answer directly. "
        "When presenting query results, format them clearly. "
        "If the trader's question is ambiguous, ask a clarifying question."
    ),
    sub_agents=[nl2sql_agent],
)
```

**File: `agent/nl2sql/__init__.py`**

```python
from .agent import nl2sql_agent
```

**File: `agent/nl2sql/agent.py`** (skeleton — tools added in Phase C)

```python
from google.adk.agents import LlmAgent
from .prompts import NL2SQL_SYSTEM_PROMPT

nl2sql_agent = LlmAgent(
    name="nl2sql_agent",
    model="gemini-2.5-flash",
    description=(
        "Answers questions about Mako trading data by querying BigQuery. "
        "Handles theo/vol/delta analysis, KPI/PnL queries, quoter activity, "
        "broker performance, edge/slippage analysis across all trading desks. "
        "Routes to the correct table based on question context."
    ),
    instruction=NL2SQL_SYSTEM_PROMPT,
    tools=[],  # Added in Phase C
)
```

**File: `agent/nl2sql/prompts.py`** (initial skeleton)

```python
NL2SQL_SYSTEM_PROMPT = """You are a SQL expert for Mako Group, an options market-making firm.

Your job is to answer natural language questions about trading data by generating
and executing BigQuery SQL queries.

## Tool Usage Order
1. vector_search_tables — find which table(s) are relevant to the question
2. load_yaml_metadata — load detailed schema + business rules for those tables
3. fetch_few_shot_examples — find similar past validated queries
4. generate_sql — produce SQL using the metadata and examples as context
5. dry_run_sql — validate the SQL (syntax, permissions, cost)
6. execute_sql — run the validated SQL and return results

## Critical Routing Rules
- "broker performance", "BGC", "MGN", "account" → kpi_brokertrade
- "quoter fills", "quoter PnL", "quoter edge" → kpi_quotertrade
- "click trade" → kpi_clicktrade
- "OTO", "swing" → kpi_otoswing
- "market trade", "exchange trade", or generic "trades" → kpi_markettrade
- "all trades", "total PnL across all types" → UNION ALL across ALL kpi_* tables
- "quoter activity", "what levels were we quoting", "quoter sizes" → quoter_quotertrade (raw activity, NOT kpi)
- "theo", "vol", "IV", "delta", "vega", "implied volatility", "fair value" → theodata

## Dataset
All tables are in: dev_agent_test

## SQL Rules
- Always use BigQuery SQL dialect
- Always filter on date partition first (DATE(timestamp) or trade_date)
- Always add LIMIT unless the user explicitly asks for all rows
- Use ROUND() for decimal outputs
- Never write to the database
"""
```

### A.5 — Test delegation works

```bash
pip install google-adk
adk web agent/
```

Open the ADK web UI. Ask "Hello, what can you help me with?" — should get a conversational response from root agent. Ask "What was the edge on strike 98?" — should see `transfer_to_agent(agent_name='nl2sql_agent')` in the trace, even though the sub-agent has no tools yet.

**Deliverable:** Repo scaffolded, dev data loaded, schemas extracted, agent skeleton delegates correctly.

---

## Phase B: Context Layer

**Dependencies:** Phase A complete.

### Wave 1 — YAML Catalog

This is where the LLM learns what your data means.

#### B.1.1 — Generate initial YAML from existing sources

For each table, feed relevant source material to Claude/Gemini and ask it to generate a YAML metadata file.

**For KPI tables — prompt template:**

```
I have source code from a KPI calculation repo for an options market-making firm.
This code computes trading performance metrics and writes results to BigQuery
tables, one per trade type (markettrade, quotertrade, brokertrade, clicktrade, otoswing).

Below is the source code. Generate a YAML metadata file for the [TABLE_NAME] table.

Include:
- table description (what it contains, what one row represents)
- column descriptions with business meaning
- synonyms traders would use for each column
- example_values for enum-like columns
- business rules (calculation formulas, bucketing logic)
- value ranges where relevant

The actual schema columns from BigQuery are:
[PASTE OUTPUT OF schemas/kpi_markettrade.json]

Output in this YAML format:
table:
  name: ...
  dataset: dev_agent_test
  description: ...
  columns:
    - name: ...
      type: ...
      description: ...
      synonyms: [...]
      example_values: [...] # optional
      range: [...] # optional
  business_rules:
    rule_name:
      description: ...
      formula: ...

[PASTE KPI REPO SOURCE FILES]
```

**For theodata — use proto file + volbox transform code:**

Same approach, but emphasise: synonym mapping (vol/IV/implied vol/sigma), value formats (decimal percentages), the 16:25 hedge time, delta bucket definitions.

**For quoter.quotertrade — use quoter proto + transform code:**

Emphasise: this is RAW activity data, not KPI metrics. Focus on timestamp precision, what levels/sizes mean, portfolio field.

#### B.1.2 — Create the KPI dataset routing file

**File: `catalog/kpi/_dataset.yaml`**

```yaml
dataset:
  name: kpi
  description: >
    KPI (Key Performance Indicators) for all options trade types.
    One table per trade origin, all sharing the same KPI column structure.

  routing:
    - patterns: ["market trade", "exchange trade", "generic trades"]
      table: kpi_markettrade
      notes: "Default KPI table when trade type is unspecified"

    - patterns: ["quoter trade", "quoter fill", "quoter PnL", "quoter edge"]
      table: kpi_quotertrade
      notes: "KPI metrics for auto-quoter originated fills. NOT raw quoter activity."

    - patterns: ["broker trade", "voice trade", "BGC", "MGN", "account", "broker performance"]
      table: kpi_brokertrade
      notes: "Has account/broker fields. Use when comparing broker performance."

    - patterns: ["click trade"]
      table: kpi_clicktrade

    - patterns: ["OTO", "swing", "otoswing"]
      table: kpi_otoswing

    - patterns: ["all trades", "total PnL", "overall", "across all"]
      action: "UNION ALL across all 5 kpi tables"
      notes: "Agent must query all tables and combine results"

  shared_columns:
    edge_bps:
      description: "Difference between machine fair value and actual trade price, in basis points"
      synonyms: ["edge", "the edge", "how much edge"]
    required_edge_bps:
      description: "Minimum edge threshold set by risk management"
      synonyms: ["required edge", "edge requirement", "minimum edge"]
    instant_pnl:
      description: "Immediate PnL from the trade"
      synonyms: ["PnL", "instant PnL", "profit", "loss"]
    delta_bucket:
      description: "Standard delta categorisation"
      categories:
        "0-25 delta": "ABS(delta) < 0.25 — deep OTM"
        "25-40 delta": "0.25 <= ABS(delta) < 0.40"
        "40-60 delta": "0.40 <= ABS(delta) < 0.60 — ATM"
        "60+ delta": "ABS(delta) >= 0.60 — deep ITM"
    slippage:
      description: "Execution slippage metric"
      synonyms: ["slip", "slippage"]

  disambiguation:
    kpi_quotertrade_vs_quoter_quotertrade: >
      kpi.quotertrade has KPI metrics (edge, PnL, slippage) for quoter fills.
      quoter.quotertrade has raw activity data (timestamps, levels, sizes).
      If the question asks about performance/edge/PnL → kpi_quotertrade.
      If the question asks about quoting levels/activity/times → quoter_quotertrade.
```

#### B.1.3 — Create the routing description that goes into embeddings

**File: `catalog/_routing.yaml`**

```yaml
# This file defines how the agent distinguishes between similar-sounding tables.
# Its content gets embedded as a schema_embedding row with source_type='routing'.

routing_descriptions:

  kpi_vs_quoter: >
    The kpi dataset and quoter dataset both have a quotertrade table but they
    contain different data. kpi.quotertrade (dev_agent_test.kpi_quotertrade)
    has KPI performance metrics: edge_bps, instant_pnl, slippage, delta_bucket.
    quoter.quotertrade (dev_agent_test.quoter_quotertrade) has raw execution
    activity: exact timestamps, price levels, sizes, sides. Use kpi when the
    question is about performance, edge, PnL, or slippage. Use quoter when
    the question is about what we were quoting, at what levels, at what times.

  kpi_table_selection: >
    The kpi dataset has 5 tables, one per trade origin: markettrade (exchange
    trades, the default), quotertrade (auto-quoter fills), brokertrade (broker
    trades, has account field for BGC/MGN comparison), clicktrade (manual click
    trades), otoswing (OTO swing trades). When a question doesn't specify
    trade type, use markettrade. When comparing brokers or mentioning account
    names, use brokertrade. When asking about all trades or total PnL, UNION
    ALL across all 5 tables.
```

#### B.1.4 — Review and iterate

For each YAML file:
- [ ] Column names match actual BigQuery schema (from A.3 output)
- [ ] Descriptions are accurate (check against source code)
- [ ] Synonyms reflect what traders actually say
- [ ] Business rules match the KPI repo calculations
- [ ] Sample values match what you see in dev_agent_test

Send to Natalie P / Natalie U for trader review. Key ask: "Are these descriptions right? What other words do you use for these things?"

**Deliverable:** 8 YAML catalog files + 2 routing/dataset files, all matching actual schema.

---

### Wave 2 — Example Queries

The single highest-ROI step. Each example is a proven pattern the agent can adapt.

#### B.2.1 — Write examples by request type

**File: `examples/theodata_examples.yaml`**

Write 8-10 examples covering:
- Theo/delta/vol/vega over time for a specific strike (Request #1)
- Comparing multiple strikes
- Filtering by option type (calls vs puts)
- Time-windowed queries (last week, last month, specific date)
- Highest/lowest values

**File: `examples/kpi_examples.yaml`**

Write 12-15 examples covering:
- Single trade lookup by strike + price (Request #5)
- Aggregated PnL by delta bucket (Request #5)
- Edge analysis (trades above/below required edge)
- Broker comparison by account (Request #5 — routes to kpi_brokertrade)
- Cross-KPI-table UNION ALL for total PnL
- Filtering by trade type (routes to correct kpi table)

**File: `examples/quoter_examples.yaml`**

Write 5-8 examples covering:
- Daily trade counts (Request #2)
- Quoting levels at specific times (Request #2)
- Biggest trades by size
- Activity in a specific time window

#### B.2.2 — Test every example against dev data

**This is non-negotiable.** Run every SQL query in BigQuery console against `dev_agent_test`.

For each example:
- [ ] Executes without error
- [ ] Returns sensible results (not empty, not millions of rows)
- [ ] Column names in SQL match actual table columns
- [ ] Filters work correctly (dates, strikes, option types)
- [ ] Aggregations produce reasonable numbers

Fix any that fail. Only validated queries enter the corpus.

#### B.2.3 — Adjust SQL to use actual column names

After A.3, you know the real column names. If `edge_bps` is actually `edge` in the table, update every example. If `trade_date` is actually `date`, update. This alignment is critical.

#### B.2.4 — Add complexity and routing tags

Each example should have:
```yaml
- question: "How have broker trades by BGC performed vs MGN in PnL and slippage?"
  sql: |
    SELECT account, COUNT(*), ROUND(SUM(instant_pnl), 2), ROUND(AVG(slippage), 4)
    FROM dev_agent_test.kpi_brokertrade
    WHERE account IN ('BGC', 'MGN')
    AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY account
  tables_used: [kpi_brokertrade]
  complexity: medium
  routing_signal: "mentions broker + account names → kpi_brokertrade"
  validated_by: luis
```

The `routing_signal` tag helps during debugging — if the agent picks the wrong table, you can see what signal it should have caught.

**Deliverable:** 30+ validated examples in 3 YAML files, all tested against dev data.

---

### Wave 3 — Embeddings

#### B.3.1 — Create metadata dataset

**File: `embeddings/01_create_metadata_schema.sql`**

```sql
CREATE SCHEMA IF NOT EXISTS metadata
OPTIONS (
  description = 'NL2SQL agent metadata, embeddings, and query memory',
  location = 'EU'
);
```

#### B.3.2 — Create embedding model connection

**File: `embeddings/02_create_embedding_model.sql`**

```sql
-- Requires a Vertex AI connection already created.
-- If not: bq mk --connection --connection_type=CLOUD_RESOURCE \
--   --project_id=PROJECT --location=EU vertex-ai-connection
-- Then grant Vertex AI User role to the connection's service account.

CREATE MODEL IF NOT EXISTS metadata.embedding_model
REMOTE WITH CONNECTION `EU.vertex-ai-connection`
OPTIONS (ENDPOINT = 'text-embedding-005');
```

#### B.3.3 — Create embedding tables

**File: `embeddings/03_create_embedding_tables.sql`**

```sql
-- Table and dataset descriptions (schema routing)
CREATE OR REPLACE TABLE metadata.schema_embeddings (
  id STRING DEFAULT GENERATE_UUID(),
  source_type STRING NOT NULL,          -- 'table', 'dataset', 'routing'
  dataset_name STRING,
  table_name STRING,
  description TEXT NOT NULL,
  embedding ARRAY<FLOAT64>,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Column-level descriptions (column routing)
CREATE OR REPLACE TABLE metadata.column_embeddings (
  id STRING DEFAULT GENERATE_UUID(),
  dataset_name STRING NOT NULL,
  table_name STRING NOT NULL,
  column_name STRING NOT NULL,
  description TEXT NOT NULL,
  synonyms ARRAY<STRING>,
  embedding ARRAY<FLOAT64>,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Validated query memory (few-shot retrieval + learning loop)
CREATE OR REPLACE TABLE metadata.query_memory (
  id STRING DEFAULT GENERATE_UUID(),
  question TEXT NOT NULL,
  sql_query TEXT NOT NULL,
  tables_used ARRAY<STRING> NOT NULL,
  complexity STRING,
  routing_signal STRING,
  validated_by STRING,
  validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  success_count INT64 DEFAULT 1,
  embedding ARRAY<FLOAT64>
);
```

#### B.3.4 — Populate schema embeddings

**File: `embeddings/04_populate_schema_embeddings.sql`**

Insert one row per table, one row for the KPI dataset, and rows for routing descriptions. The description text is what gets embedded — make it rich with synonyms and context.

```sql
-- KPI dataset routing
INSERT INTO metadata.schema_embeddings (source_type, dataset_name, description)
VALUES ('dataset', 'kpi', 'KPI Key Performance Indicators dataset for options trading. Contains one table per trade type: markettrade (exchange trades, default), quotertrade (auto-quoter fills), brokertrade (broker trades with account field for BGC MGN comparison), clicktrade (manual click trades), otoswing (OTO swing trades). All share columns: edge_bps (edge in basis points), required_edge_bps, instant_pnl, delta_bucket, slippage. For total PnL across all types use UNION ALL.');

-- Routing disambiguation
INSERT INTO metadata.schema_embeddings (source_type, description)
VALUES ('routing', 'kpi.quotertrade has KPI performance metrics (edge, PnL, slippage) for quoter fills. quoter.quotertrade has raw execution activity (exact timestamps, price levels, sizes, sides). Use kpi for performance questions. Use quoter for activity and quoting level questions.');

-- Individual tables (one per table — use the descriptions from your YAML catalog)
INSERT INTO metadata.schema_embeddings (source_type, dataset_name, table_name, description)
VALUES
('table', 'dev_agent_test', 'theodata',
 'Theoretical options pricing snapshots from volbox. Contains theo (theoretical fair price, also called fair value, TV, machine price), delta (delta greek, hedge ratio, 0 to 1 for calls, -1 to 0 for puts), vol (annualised implied volatility as decimal, also called IV, implied vol, sigma, 0.25 = 25%), vega (vega greek, change in theo per 1% vol change), strike (strike price, K), symbol (underlying), option_type (C call, P put), timestamp. Updated every 5 seconds during market hours. Partitioned by DATE(timestamp).'),

('table', 'dev_agent_test', 'kpi_markettrade',
 'KPI metrics for market exchange trades. One row per trade. Contains edge_bps (edge, difference between machine fair value and trade price in basis points), required_edge_bps (minimum edge threshold from risk management), instant_pnl (immediate PnL, profit loss), delta_bucket (delta categorisation 0-25 25-40 40-60 60+), trade_price, theo, delta, slippage, strike, option_type, symbol, trade_date. Default KPI table when trade type is not specified.'),

('table', 'dev_agent_test', 'kpi_brokertrade',
 'KPI metrics for broker facilitated trades. Same KPI columns as markettrade plus account and broker fields. Accounts include BGC, MGN. Use when comparing broker performance or when question mentions broker account names.'),

('table', 'dev_agent_test', 'kpi_quotertrade',
 'KPI metrics for auto-quoter originated trade fills. Same KPI columns as markettrade. Use for quoter performance, edge, PnL analysis. NOT for raw quoter activity or quoting levels — use quoter_quotertrade for that.'),

('table', 'dev_agent_test', 'kpi_clicktrade',
 'KPI metrics for manually initiated click trades. Same KPI columns as markettrade.'),

('table', 'dev_agent_test', 'kpi_otoswing',
 'KPI metrics for OTO swing trades. Same KPI columns as markettrade.'),

('table', 'dev_agent_test', 'quoter_quotertrade',
 'Raw quoter trade activity from the quoter system. Contains exact timestamps, strike, symbol, option_type, price, size, side, portfolio. Use for granular quoter activity: what levels we were quoting, when trades happened, trade sizes at specific times. This is raw execution data, NOT KPI performance metrics.');
```

#### B.3.5 — Populate query memory from examples

Write a Python script or manual inserts to load all 30+ examples from the YAML files into `metadata.query_memory`.

#### B.3.6 — Generate all embeddings

**File: `embeddings/07_generate_embeddings.sql`**

```sql
-- Embed schema descriptions
UPDATE metadata.schema_embeddings
SET embedding = (
  SELECT text_embedding FROM
  ML.GENERATE_EMBEDDING(
    MODEL metadata.embedding_model,
    (SELECT description as content),
    STRUCT('RETRIEVAL_DOCUMENT' AS task_type, TRUE AS flatten_json_output)
  )
)
WHERE embedding IS NULL;

-- Embed column descriptions
UPDATE metadata.column_embeddings
SET embedding = (
  SELECT text_embedding FROM
  ML.GENERATE_EMBEDDING(
    MODEL metadata.embedding_model,
    (SELECT description as content),
    STRUCT('RETRIEVAL_DOCUMENT' AS task_type, TRUE AS flatten_json_output)
  )
)
WHERE embedding IS NULL;

-- Embed query memory questions
UPDATE metadata.query_memory
SET embedding = (
  SELECT text_embedding FROM
  ML.GENERATE_EMBEDDING(
    MODEL metadata.embedding_model,
    (SELECT question as content),
    STRUCT('RETRIEVAL_DOCUMENT' AS task_type, TRUE AS flatten_json_output)
  )
)
WHERE embedding IS NULL;
```

#### B.3.7 — Create vector indexes

**File: `embeddings/08_create_vector_indexes.sql`**

```sql
CREATE VECTOR INDEX IF NOT EXISTS idx_schema_emb
ON metadata.schema_embeddings(embedding)
OPTIONS (index_type = 'TREE_AH', distance_type = 'COSINE');

CREATE VECTOR INDEX IF NOT EXISTS idx_column_emb
ON metadata.column_embeddings(embedding)
OPTIONS (index_type = 'TREE_AH', distance_type = 'COSINE');

CREATE VECTOR INDEX IF NOT EXISTS idx_query_emb
ON metadata.query_memory(embedding)
OPTIONS (index_type = 'TREE_AH', distance_type = 'COSINE');
```

#### B.3.8 — Test vector search

**File: `embeddings/09_test_vector_search.sql`**

```sql
-- TEST 1: "edge" should find KPI tables
-- Expected: kpi_markettrade, kpi dataset, kpi_brokertrade
SELECT source_type, table_name, ROUND(distance, 4) as dist
FROM VECTOR_SEARCH(
  TABLE metadata.schema_embeddings, 'embedding',
  (SELECT text_embedding as embedding FROM ML.GENERATE_EMBEDDING(
    MODEL metadata.embedding_model,
    (SELECT 'what was the edge on our trade?' as content),
    STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output))),
  top_k => 5, distance_type => 'COSINE');

-- TEST 2: "vol" / "IV" should find theodata
-- Expected: theodata as top result
SELECT source_type, table_name, ROUND(distance, 4) as dist
FROM VECTOR_SEARCH(
  TABLE metadata.schema_embeddings, 'embedding',
  (SELECT text_embedding as embedding FROM ML.GENERATE_EMBEDDING(
    MODEL metadata.embedding_model,
    (SELECT 'how did implied vol change over the last month?' as content),
    STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output))),
  top_k => 5, distance_type => 'COSINE');

-- TEST 3: "broker BGC vs MGN" should find kpi_brokertrade
-- Expected: kpi_brokertrade as top result
SELECT source_type, table_name, ROUND(distance, 4) as dist
FROM VECTOR_SEARCH(
  TABLE metadata.schema_embeddings, 'embedding',
  (SELECT text_embedding as embedding FROM ML.GENERATE_EMBEDDING(
    MODEL metadata.embedding_model,
    (SELECT 'how have broker trades by BGC performed compared to MGN?' as content),
    STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output))),
  top_k => 5, distance_type => 'COSINE');

-- TEST 4: "quoting levels at 11:15" should find quoter_quotertrade (NOT kpi)
-- Expected: quoter_quotertrade, not kpi_quotertrade
SELECT source_type, table_name, ROUND(distance, 4) as dist
FROM VECTOR_SEARCH(
  TABLE metadata.schema_embeddings, 'embedding',
  (SELECT text_embedding as embedding FROM ML.GENERATE_EMBEDDING(
    MODEL metadata.embedding_model,
    (SELECT 'what levels were we quoting in strike 98 at 11:15?' as content),
    STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output))),
  top_k => 5, distance_type => 'COSINE');

-- TEST 5: Example query retrieval - should find similar past queries
SELECT question, tables_used, ROUND(distance, 4) as dist
FROM VECTOR_SEARCH(
  TABLE metadata.query_memory, 'embedding',
  (SELECT text_embedding as embedding FROM ML.GENERATE_EMBEDDING(
    MODEL metadata.embedding_model,
    (SELECT 'show me PnL breakdown by delta bucket yesterday' as content),
    STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output))),
  top_k => 5, distance_type => 'COSINE');
```

Run all 5 tests. If routing is wrong, adjust the description text in schema_embeddings and re-embed. Iterate until all 5 pass.

**Deliverable:** All embeddings generated, vector indexes built, 5/5 routing tests passing.

---

## Phase C: Agent Tools

**Dependencies:** Phase B Wave 3 complete (working vector search).

Each tool is a Python function decorated with `@tool` or using ADK's `FunctionTool`.

### C.1 — vector_search_tables

**File: `agent/nl2sql/tools/vector_search.py`**

```python
from google.adk.tools import FunctionTool
from google.cloud import bigquery

bq = bigquery.Client()

def vector_search_tables(question: str) -> str:
    """Find the most relevant tables for a natural language question.

    Args:
        question: The trader's natural language question.

    Returns:
        Top 5 matching tables with descriptions and relevance scores.
    """
    query = """
    SELECT source_type, dataset_name, table_name, description,
           ROUND(distance, 4) as relevance_score
    FROM VECTOR_SEARCH(
      TABLE metadata.schema_embeddings, 'embedding',
      (SELECT text_embedding as embedding FROM ML.GENERATE_EMBEDDING(
        MODEL metadata.embedding_model,
        (SELECT @question as content),
        STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output))),
      top_k => 5, distance_type => 'COSINE')
    ORDER BY distance
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("question", "STRING", question)]
    )
    results = bq.query(query, job_config=job_config).to_dataframe()
    return results.to_markdown(index=False)


def fetch_few_shot_examples(question: str) -> str:
    """Find similar past validated queries to use as few-shot examples.

    Args:
        question: The trader's natural language question.

    Returns:
        Top 5 similar past questions with their validated SQL queries.
    """
    query = """
    SELECT question, sql_query, tables_used, complexity,
           ROUND(distance, 4) as similarity_score
    FROM VECTOR_SEARCH(
      TABLE metadata.query_memory, 'embedding',
      (SELECT text_embedding as embedding FROM ML.GENERATE_EMBEDDING(
        MODEL metadata.embedding_model,
        (SELECT @question as content),
        STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output))),
      top_k => 5, distance_type => 'COSINE')
    ORDER BY distance
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("question", "STRING", question)]
    )
    results = bq.query(query, job_config=job_config).to_dataframe()
    return results.to_markdown(index=False)


vector_search_tables_tool = FunctionTool(vector_search_tables)
fetch_few_shot_examples_tool = FunctionTool(fetch_few_shot_examples)
```

### C.2 — metadata_loader

**File: `agent/nl2sql/tools/metadata_loader.py`**

```python
import yaml
from pathlib import Path
from google.adk.tools import FunctionTool

CATALOG_DIR = Path(__file__).parent.parent.parent.parent / "catalog"

def load_yaml_metadata(table_name: str) -> str:
    """Load the YAML metadata catalog for a specific table.

    Args:
        table_name: The table to load metadata for (e.g., 'theodata', 'kpi_markettrade').

    Returns:
        Full YAML metadata including column descriptions, synonyms, business rules.
    """
    # Map table names to YAML file paths
    table_to_path = {
        "theodata": "volbox/theodata.yaml",
        "kpi_markettrade": "kpi/markettrade.yaml",
        "kpi_quotertrade": "kpi/quotertrade.yaml",
        "kpi_brokertrade": "kpi/brokertrade.yaml",
        "kpi_clicktrade": "kpi/clicktrade.yaml",
        "kpi_otoswing": "kpi/otoswing.yaml",
        "quoter_quotertrade": "quoter/quotertrade.yaml",
    }

    path = table_to_path.get(table_name)
    if not path:
        return f"No metadata found for table '{table_name}'"

    full_path = CATALOG_DIR / path
    if not full_path.exists():
        return f"YAML file not found: {full_path}"

    with open(full_path) as f:
        content = yaml.safe_load(f)

    # Also load KPI dataset metadata if it's a KPI table
    if table_name.startswith("kpi_"):
        dataset_path = CATALOG_DIR / "kpi" / "_dataset.yaml"
        if dataset_path.exists():
            with open(dataset_path) as f:
                dataset_meta = yaml.safe_load(f)
            content["kpi_dataset_context"] = dataset_meta

    return yaml.dump(content, default_flow_style=False)


load_yaml_metadata_tool = FunctionTool(load_yaml_metadata)
```

### C.3 — SQL validator and executor

**File: `agent/nl2sql/tools/sql_validator.py`**

```python
from google.adk.tools import FunctionTool
from google.cloud import bigquery

bq = bigquery.Client()

def dry_run_sql(sql_query: str) -> str:
    """Validate a SQL query using BigQuery dry run. Checks syntax and permissions
    without executing. Returns estimated bytes processed or error details.

    Args:
        sql_query: The BigQuery SQL query to validate.

    Returns:
        'VALID: estimated X MB processed' or 'ERROR: <error details>'
    """
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    try:
        query_job = bq.query(sql_query, job_config=job_config)
        mb = query_job.total_bytes_processed / (1024 * 1024)
        return f"VALID: estimated {mb:.1f} MB processed"
    except Exception as e:
        return f"ERROR: {str(e)}"


def execute_sql(sql_query: str) -> str:
    """Execute a validated SQL query against BigQuery and return results.
    Read-only. Maximum 1000 rows returned.

    Args:
        sql_query: The BigQuery SQL query to execute (must be SELECT).

    Returns:
        Query results as a markdown table.
    """
    if not sql_query.strip().upper().startswith("SELECT"):
        return "ERROR: Only SELECT queries are allowed."

    try:
        results = bq.query(sql_query).to_dataframe()
        if len(results) > 1000:
            results = results.head(1000)
            return results.to_markdown(index=False) + "\n\n(Truncated to 1000 rows)"
        return results.to_markdown(index=False)
    except Exception as e:
        return f"ERROR: {str(e)}"


dry_run_sql_tool = FunctionTool(dry_run_sql)
execute_sql_tool = FunctionTool(execute_sql)
```

### C.4 — Learning loop

**File: `agent/nl2sql/tools/learning_loop.py`**

```python
from google.adk.tools import FunctionTool
from google.cloud import bigquery

bq = bigquery.Client()

def save_validated_query(question: str, sql_query: str, tables_used: str) -> str:
    """Save a validated question→SQL pair to the query memory for future retrieval.
    Call this when the trader confirms the query result was correct.

    Args:
        question: The original natural language question.
        sql_query: The SQL that produced correct results.
        tables_used: Comma-separated list of tables used.

    Returns:
        Confirmation message.
    """
    tables_array = [t.strip() for t in tables_used.split(",")]

    query = """
    INSERT INTO metadata.query_memory (question, sql_query, tables_used, validated_by)
    VALUES (@question, @sql_query, @tables_used, 'trader')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("question", "STRING", question),
            bigquery.ScalarQueryParameter("sql_query", "STRING", sql_query),
            bigquery.ArrayQueryParameter("tables_used", "STRING", tables_array),
        ]
    )
    bq.query(query, job_config=job_config).result()

    # Generate embedding for the new question
    embed_query = """
    UPDATE metadata.query_memory
    SET embedding = (
      SELECT text_embedding FROM ML.GENERATE_EMBEDDING(
        MODEL metadata.embedding_model,
        (SELECT question as content),
        STRUCT('RETRIEVAL_DOCUMENT' AS task_type, TRUE AS flatten_json_output)))
    WHERE embedding IS NULL
    """
    bq.query(embed_query).result()

    return f"✓ Saved validated query. Tables: {tables_array}. This will improve future similar questions."


save_validated_query_tool = FunctionTool(save_validated_query)
```

### C.5 — Wire tools into nl2sql_agent

**Update `agent/nl2sql/agent.py`:**

```python
from google.adk.agents import LlmAgent
from .prompts import NL2SQL_SYSTEM_PROMPT
from .tools.vector_search import vector_search_tables_tool, fetch_few_shot_examples_tool
from .tools.metadata_loader import load_yaml_metadata_tool
from .tools.sql_validator import dry_run_sql_tool, execute_sql_tool
from .tools.learning_loop import save_validated_query_tool

nl2sql_agent = LlmAgent(
    name="nl2sql_agent",
    model="gemini-2.5-flash",
    description=(
        "Answers questions about Mako trading data by querying BigQuery. "
        "Handles theo/vol/delta analysis, KPI/PnL queries, quoter activity, "
        "broker performance, edge/slippage analysis across all trading desks. "
        "Routes to the correct table based on question context."
    ),
    instruction=NL2SQL_SYSTEM_PROMPT,
    tools=[
        vector_search_tables_tool,
        fetch_few_shot_examples_tool,
        load_yaml_metadata_tool,
        dry_run_sql_tool,
        execute_sql_tool,
        save_validated_query_tool,
    ],
)
```

**Deliverable:** All 6 tools implemented and wired into nl2sql_agent.

---

## Phase D: System Instructions & Routing

**Dependencies:** Phase C complete.

### D.1 — Refine NL2SQL system prompt

Update `agent/nl2sql/prompts.py` with the full system prompt. This should include:

- Explicit tool ordering (always search → metadata → examples → generate → validate → execute)
- All routing rules from the `_routing.yaml` and `_dataset.yaml`
- BigQuery SQL dialect rules
- Partition usage enforcement
- LIMIT enforcement
- How to handle ambiguous questions (ask for clarification)
- How to handle failed dry runs (retry with error context, up to 3 times)

### D.2 — Refine root agent prompt

Ensure the root agent knows when to delegate vs answer directly. Test with:
- "Hello" → root agent answers
- "What was the edge?" → delegates to nl2sql_agent
- "Can you explain what delta means?" → root agent answers (domain knowledge, not a data query)
- "Show me PnL by delta bucket" → delegates to nl2sql_agent

### D.3 — End-to-end testing

Run through all request types from the original list:

```
Request #1 tests:
  □ "How did the theo of the 98 call change over the last month?"
  □ "What was delta and vol of strike 100 puts at close yesterday?"
  □ "Compare theo for strikes 96, 98, 100 calls"

Request #2 tests:
  □ "How many trades did the quoter do daily this month?"
  □ "What levels were we quoting in strike 98 between 11:15 and 11:16?"

Request #5 tests:
  □ "What was the edge on our trade in strike 98 at price 12.50?"
  □ "What was instant PnL yesterday by delta bucket?"
  □ "How have broker trades by BGC performed vs MGN?"
  □ "What was total PnL across all trade types yesterday?"

Routing tests:
  □ "broker performance" → kpi_brokertrade (NOT markettrade)
  □ "quoting levels at 11:15" → quoter_quotertrade (NOT kpi_quotertrade)
  □ "quoter edge" → kpi_quotertrade (NOT quoter_quotertrade)
  □ "all trades PnL" → UNION ALL across all kpi tables
```

For each failure, diagnose:
1. Wrong table? → Improve schema_embeddings description text, re-embed
2. Wrong SQL? → Add a more specific example to query_memory, re-embed
3. Wrong columns? → Fix YAML metadata, update prompts
4. Routing confusion? → Strengthen disambiguation descriptions

**Deliverable:** Agent passes all routing tests and handles core request patterns.

---

## Phase E: Evaluation & Hardening

**Dependencies:** Phase D complete.

### E.1 — Build gold-standard eval set

**File: `eval/gold_queries.yaml`**

50 questions with expected:
- Correct table(s)
- Required columns in output
- Required WHERE filters
- Expected result characteristics (non-empty, reasonable values)

### E.2 — Automated eval runner

**File: `eval/run_eval.py`**

Runs all gold queries through the agent, measures:
- SQL syntax validity %
- Correct table routing %
- Result accuracy %
- Latency percentiles

### E.3 — Add LoopAgent retry pattern

If dry-run failures > 10%, wrap SQL generation in a retry loop:

```python
from google.adk.agents import LoopAgent

sql_retry_agent = LoopAgent(
    name="sql_retry",
    sub_agents=[sql_generator_agent],
    max_iterations=3,
)
```

### E.4 — Iterate on prompts

Use eval results to identify failure patterns. Adjust system prompt, add examples, improve descriptions.

**Deliverable:** Eval framework running, accuracy metrics tracked, retry logic in place.

---

## Phase F: Learning Loop & Production

**Dependencies:** Phase E passing at acceptable accuracy.

### F.1 — Activate learning loop

The `save_validated_query` tool is already built. Add a UX flow where the agent asks "Was this result correct?" after returning data.

### F.2 — Semantic caching

Before running the full pipeline, check if a very similar question was already answered:

```python
def check_semantic_cache(question: str, threshold: float = 0.05) -> str | None:
    """Check if a very similar question exists in query_memory."""
    results = vector_search(question, table="query_memory", top_k=1)
    if results and results[0].distance < threshold:
        return results[0].sql_query
    return None
```

### F.3 — Session state for follow-ups

Store last query results in ADK session state so the trader can ask follow-ups:

```python
# In execute_sql tool:
tool_context.state["last_query"] = sql_query
tool_context.state["last_results_summary"] = results.describe().to_markdown()
tool_context.state["last_tables_used"] = tables_used
```

### F.4 — Deploy

Options:
- **Cloud Run** — containerised ADK agent
- **Vertex AI Agent Engine** — managed deployment
- **Slack integration** — highest trader adoption potential

---

## Execution Summary

| Phase | What | Key Output |
|-------|------|-----------|
| **A** | Repo + dev data + agent skeleton | Delegation works, schemas extracted |
| **B.1** | YAML metadata catalog | 8 YAML files with descriptions, synonyms, rules |
| **B.2** | Example queries | 30+ validated Q→SQL pairs |
| **B.3** | Embeddings in BigQuery | 3 embedding tables, vector indexes, routing tests pass |
| **C** | Agent tools | 6 tools: vector search, metadata, validate, execute, learn |
| **D** | System instructions + testing | Agent handles all request types, routing correct |
| **E** | Eval framework + hardening | Accuracy measured, retry logic, prompt iteration |
| **F** | Learning loop + deploy | Self-improving system, production deployment |
