# NL2SQL Agent — Environment Setup Guide

How to set up the NL2SQL agent from scratch in a new GCP project.

---

## Part 1: GCP Infrastructure (one-time)

### 1.1 Enable APIs

```bash
PROJECT=your-project-id

gcloud services enable bigquery.googleapis.com \
  aiplatform.googleapis.com \
  bigqueryconnection.googleapis.com \
  --project=$PROJECT
```

### 1.2 Create Vertex AI Connection

The agent uses a BigQuery remote connection to call the Vertex AI text-embedding-005 model.

```bash
bq mk --connection \
  --connection_type=CLOUD_RESOURCE \
  --project_id=$PROJECT \
  --location=europe-west2 \
  vertex-ai-connection
```

Get the connection's service account:

```bash
bq show --connection --format=json $PROJECT.europe-west2.vertex-ai-connection \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['cloudResource']['serviceAccountId'])"
```

Grant it the Vertex AI User role so it can call embedding endpoints:

```bash
SA=<service-account-from-above>

gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:$SA" \
  --role="roles/aiplatform.user"
```

> **IAM propagation delay**: Wait ~60 seconds after granting the role before
> testing the model. IAM changes are eventually consistent.

### 1.3 Create the Embedding Model

Create a dataset to host the model reference, then create the model:

```bash
bq mk --location=europe-west2 $PROJECT:nl2sql
```

```sql
CREATE MODEL IF NOT EXISTS `PROJECT.nl2sql.text_embedding_model`
REMOTE WITH CONNECTION `PROJECT.europe-west2.vertex-ai-connection`
OPTIONS (ENDPOINT = 'text-embedding-005');
```

Replace `PROJECT` with your actual project ID.

### 1.4 Same-Project vs Cross-Project

In **dev** (`melodic-stone-437916-t3`), the connection, model, and data all live in the same project:

```
VERTEX_AI_CONNECTION=melodic-stone-437916-t3.europe-west2.vertex-ai-connection
EMBEDDING_MODEL_REF=melodic-stone-437916-t3.nl2sql.text_embedding_model
```

In **prod**, the embedding model lives in a separate AI project (`cloud-ai-d-base-a2df`), while the data lives in `cloud-data-n-base-d4b3`:

```
VERTEX_AI_CONNECTION=cloud-ai-d-base-a2df.europe-west2.vertex-ai-connection
EMBEDDING_MODEL_REF=cloud-ai-d-base-a2df.nl2sql.text_embedding_model
```

For cross-project setups, grant the data project's BigQuery service account `roles/bigquery.connectionUser` on the AI project, and the AI connection's service account `roles/aiplatform.user` on the AI project.

---

## Part 2: BigQuery Datasets & Data Tables

### 2.1 Create Datasets

The agent uses three datasets:

| Dataset | Purpose |
|---------|---------|
| `nl2sql_omx_kpi` | Gold layer — KPI trade metrics (5 tables) |
| `nl2sql_omx_data` | Silver layer — raw market/trade data (8 tables) |
| `nl2sql_metadata` | Embeddings, column metadata, query memory |

```bash
bq mk --location=europe-west2 $PROJECT:nl2sql_omx_kpi
bq mk --location=europe-west2 $PROJECT:nl2sql_omx_data
bq mk --location=europe-west2 $PROJECT:nl2sql_metadata
```

### 2.2 Populate Data Tables

Copy thin data slices from a source project. The patterns below are from `raw_sql_init.sql` — replace `SOURCE_PROJECT` and `TARGET_PROJECT` with your actual project IDs, and adjust the `WHERE trade_date = ...` filter to a date with data.

#### KPI tables (partition by `trade_date` only)

```sql
-- 5 tables: brokertrade, clicktrade, markettrade, otoswing, quotertrade
CREATE OR REPLACE TABLE `TARGET_PROJECT.nl2sql_omx_kpi.markettrade`
PARTITION BY trade_date
AS
SELECT * FROM `SOURCE_PROJECT.omx_kpi.markettrade`
WHERE trade_date = '2026-02-17';
```

Repeat for `brokertrade`, `clicktrade`, `otoswing`, `quotertrade`.

#### Data tables (partition + cluster)

```sql
-- Tables with clustering: brokertrade, clicktrade, markettrade, quotertrade, theodata
CREATE OR REPLACE TABLE `TARGET_PROJECT.nl2sql_omx_data.theodata`
PARTITION BY trade_date
CLUSTER BY portfolio, symbol, term, instrument_hash
AS
SELECT * FROM `SOURCE_PROJECT.omx_data.theodata`
WHERE trade_date = '2026-02-17';

-- Tables with 3-column clustering: marketdata, marketdepth
CREATE OR REPLACE TABLE `TARGET_PROJECT.nl2sql_omx_data.marketdata`
PARTITION BY trade_date
CLUSTER BY symbol, term, instrument_hash
AS
SELECT * FROM `SOURCE_PROJECT.omx_data.marketdata`
WHERE trade_date = '2026-02-17';

-- Tables without clustering: swingdata
CREATE OR REPLACE TABLE `TARGET_PROJECT.nl2sql_omx_data.swingdata`
PARTITION BY trade_date
AS
SELECT * FROM `SOURCE_PROJECT.omx_data.swingdata`
WHERE trade_date = '2026-02-17';
```

Repeat for the remaining data tables following the same pattern.

### 2.3 Verify

```sql
-- Row counts for all KPI tables
SELECT 'brokertrade' AS tbl, COUNT(*) AS rows FROM `PROJECT.nl2sql_omx_kpi.brokertrade`
UNION ALL SELECT 'clicktrade', COUNT(*) FROM `PROJECT.nl2sql_omx_kpi.clicktrade`
UNION ALL SELECT 'markettrade', COUNT(*) FROM `PROJECT.nl2sql_omx_kpi.markettrade`
UNION ALL SELECT 'otoswing', COUNT(*) FROM `PROJECT.nl2sql_omx_kpi.otoswing`
UNION ALL SELECT 'quotertrade', COUNT(*) FROM `PROJECT.nl2sql_omx_kpi.quotertrade`;

-- Row counts for data tables
SELECT 'theodata' AS tbl, COUNT(*) AS rows FROM `PROJECT.nl2sql_omx_data.theodata`
UNION ALL SELECT 'marketdata', COUNT(*) FROM `PROJECT.nl2sql_omx_data.marketdata`
UNION ALL SELECT 'marketdepth', COUNT(*) FROM `PROJECT.nl2sql_omx_data.marketdepth`
UNION ALL SELECT 'swingdata', COUNT(*) FROM `PROJECT.nl2sql_omx_data.swingdata`
UNION ALL SELECT 'markettrade', COUNT(*) FROM `PROJECT.nl2sql_omx_data.markettrade`
UNION ALL SELECT 'quotertrade', COUNT(*) FROM `PROJECT.nl2sql_omx_data.quotertrade`
UNION ALL SELECT 'clicktrade', COUNT(*) FROM `PROJECT.nl2sql_omx_data.clicktrade`
UNION ALL SELECT 'brokertrade', COUNT(*) FROM `PROJECT.nl2sql_omx_data.brokertrade`;
```

All tables should have >0 rows. If a KPI table is empty (e.g. `brokertrade` may have no trades for some dates), try a different `trade_date`.

---

## Part 3: `.env` Configuration

All config is loaded by `nl2sql_agent/config.py` via pydantic-settings. Copy the template and fill in your values:

```bash
cp nl2sql_agent/.env.example nl2sql_agent/.env
```

### Field-by-Field Reference

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `LITELLM_API_KEY` | str | *(required)* | API key for the LiteLLM proxy |
| `LITELLM_API_BASE` | str | *(required)* | LiteLLM proxy URL (e.g. `http://localhost:4000`) |
| `LITELLM_MODEL` | str | `gemini-3-flash-preview` | Default LLM model for simple queries |
| `LITELLM_MODEL_COMPLEX` | str | `gemini-3-pro-preview` | LLM model for complex queries |
| `GCP_PROJECT` | str | `cloud-data-n-base-d4b3` | GCP project containing the data |
| `BQ_LOCATION` | str | `europe-west2` | BigQuery dataset location |
| `KPI_DATASET` | str | `nl2sql_omx_kpi` | KPI gold-layer dataset name |
| `DATA_DATASET` | str | `nl2sql_omx_data` | Data silver-layer dataset name |
| `METADATA_DATASET` | str | `nl2sql_metadata` | Metadata/embeddings dataset name |
| `VERTEX_AI_CONNECTION` | str | `cloud-ai-d-base-a2df.europe-west2.vertex-ai-connection` | Fully-qualified Vertex AI connection |
| `EMBEDDING_MODEL_REF` | str | `cloud-ai-d-base-a2df.nl2sql.text_embedding_model` | Fully-qualified BQ ML model reference |
| `EMBEDDING_MODEL` | str | `text-embedding-005` | Underlying Vertex AI model name |
| `BQ_QUERY_TIMEOUT_SECONDS` | float | `30.0` | BigQuery query execution timeout |
| `BQ_MAX_RESULT_ROWS` | int | `1000` | Maximum rows returned by `execute_sql` |
| `VECTOR_SEARCH_TOP_K` | int | `5` | Number of results for vector search |
| `ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS` | bool | — | Set `true` to suppress ADK/LiteLLM warnings |

### Example: Dev (same-project, local LiteLLM)

```env
LITELLM_API_KEY=<your-litellm-master-key>
LITELLM_API_BASE=http://localhost:4000
LITELLM_MODEL=claude-haiku
LITELLM_MODEL_COMPLEX=claude-sonnet

GCP_PROJECT=melodic-stone-437916-t3
BQ_LOCATION=europe-west2
KPI_DATASET=nl2sql_omx_kpi
DATA_DATASET=nl2sql_omx_data
METADATA_DATASET=nl2sql_metadata

VERTEX_AI_CONNECTION=melodic-stone-437916-t3.europe-west2.vertex-ai-connection
EMBEDDING_MODEL_REF=melodic-stone-437916-t3.nl2sql.text_embedding_model
EMBEDDING_MODEL=text-embedding-005

ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS=true
```

### Example: Prod (cross-project, hosted LiteLLM)

```env
LITELLM_API_KEY=<prod-litellm-key>
LITELLM_API_BASE=https://litellm.production.mako-cloud.com/

LITELLM_MODEL=gemini-3-flash-preview
LITELLM_MODEL_COMPLEX=gemini-3-pro-preview

GCP_PROJECT=cloud-data-n-base-d4b3
BQ_LOCATION=europe-west2
KPI_DATASET=nl2sql_omx_kpi
DATA_DATASET=nl2sql_omx_data
METADATA_DATASET=nl2sql_metadata

VERTEX_AI_CONNECTION=cloud-ai-d-base-a2df.europe-west2.vertex-ai-connection
EMBEDDING_MODEL_REF=cloud-ai-d-base-a2df.nl2sql.text_embedding_model
EMBEDDING_MODEL=text-embedding-005

ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS=true
```

> **Docker note**: When running in Docker, use `http://host.docker.internal:4000` instead of `http://localhost:4000` for `LITELLM_API_BASE`.

---

## Part 4: Embedding Pipeline

The embedding pipeline creates and populates three BigQuery tables in the `nl2sql_metadata` dataset:

| Table | Contents | Populated By |
|-------|----------|-------------|
| `schema_embeddings` | Table/dataset descriptions for routing | `run_embeddings.py --step populate-schema` |
| `column_embeddings` | Column-level descriptions from YAML catalog | `populate_embeddings.py` |
| `query_memory` | Validated Q→SQL pairs for few-shot retrieval | `populate_embeddings.py` |

### 4.1 Prerequisites

- GCP authentication: `gcloud auth application-default login`
- `.env` configured (Part 3)
- Python dependencies installed: `pip install -e .`

### 4.2 Full Pipeline (recommended for first setup)

Run in order from the repo root:

```bash
# Step 1: Create metadata dataset + tables, verify model, populate schema descriptions
python scripts/run_embeddings.py --step all

# Step 2: Populate column_embeddings and query_memory from YAML catalog + examples
python scripts/populate_embeddings.py

# Step 3: Generate embeddings for newly-populated rows
python scripts/run_embeddings.py --step generate-embeddings

# Step 4: Verify vector search works
python scripts/run_embeddings.py --step test-search
```

### 4.3 What Each Step Does

`run_embeddings.py` has 7 steps (run individually with `--step <name>` or all at once with `--step all`):

| Step | What It Does |
|------|-------------|
| `create-dataset` | `CREATE SCHEMA IF NOT EXISTS` for the metadata dataset |
| `verify-model` | Checks the embedding model exists and is accessible |
| `create-tables` | `CREATE OR REPLACE TABLE` for all 3 embedding tables |
| `populate-schema` | `MERGE` schema_embeddings with table/dataset/routing descriptions |
| `generate-embeddings` | `UPDATE ... SET embedding = ML.GENERATE_EMBEDDING(...)` for rows with empty embeddings |
| `create-indexes` | `CREATE VECTOR INDEX IF NOT EXISTS` (TREE_AH, COSINE) on all 3 tables |
| `test-search` | Runs 5 test vector searches to verify routing quality |

`populate_embeddings.py` reads from YAML catalog files and example YAML files:
- Inserts column descriptions (with synonyms) into `column_embeddings`
- Inserts validated Q→SQL pairs into `query_memory`
- Both use `MERGE` — idempotent on re-run

### 4.4 Re-Running Safely

All steps are idempotent:
- `CREATE SCHEMA IF NOT EXISTS` / `CREATE OR REPLACE TABLE` — safe to re-run
- `MERGE` — updates existing rows, inserts new ones
- `generate-embeddings` — only processes rows where `ARRAY_LENGTH(embedding) = 0`
- `CREATE VECTOR INDEX IF NOT EXISTS` — skips if index exists

To refresh after updating YAML catalog or examples:

```bash
python scripts/populate_embeddings.py              # re-merge catalog data
python scripts/run_embeddings.py --step generate-embeddings  # embed new/updated rows
python scripts/run_embeddings.py --step test-search          # verify
```

---

## Part 5: Running the Agent

### 5.1 Local — Web UI (default)

```bash
scripts/start_local.sh
```

Opens the ADK web UI on http://localhost:8000. See `scripts/start_local.sh` for prerequisite checks.

### 5.2 Local — Terminal Mode

```bash
scripts/start_local.sh -t
```

Runs the agent in interactive terminal mode (no web UI).

### 5.3 Docker

```bash
docker compose up --build
```

Exposes the ADK web UI on http://localhost:8000. Requires:
- `~/.config/gcloud` for ADC credentials (mounted read-only)
- `nl2sql_agent/.env` for environment variables
- LiteLLM proxy reachable (use `host.docker.internal:4000` for local proxy)

### 5.4 LiteLLM Proxy (dev only)

The local dev setup requires a running LiteLLM proxy. Start it in a separate terminal:

```bash
scripts/start_litellm.sh
```

This reads API keys from `pass` and starts the proxy on http://localhost:4000.

### 5.5 Tests

```bash
pytest
```

Tests run without BigQuery or LiteLLM — they use fake clients and `Settings(_env_file=None)`.

---

## Part 6: Troubleshooting

### "Model not found" or "Not found: Model"

The `EMBEDDING_MODEL_REF` in `.env` doesn't point to a valid model.

```bash
# Verify the model exists
bq ls --models $PROJECT:nl2sql
```

Check that:
- The `nl2sql` dataset exists in the correct project
- The model was created with the correct connection
- For cross-project: the model is in the AI project, not the data project

### "Permission denied" on ML.GENERATE_EMBEDDING

The Vertex AI connection's service account doesn't have `roles/aiplatform.user`.

```bash
# Re-grant and wait 60s
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:$SA" \
  --role="roles/aiplatform.user"
```

IAM changes take up to 60 seconds to propagate.

### VECTOR_SEARCH returns empty results

Embeddings haven't been generated yet. Run:

```bash
python scripts/run_embeddings.py --step generate-embeddings
```

Then verify:

```sql
SELECT COUNT(*) AS with_embedding
FROM `PROJECT.nl2sql_metadata.schema_embeddings`
WHERE ARRAY_LENGTH(embedding) > 0;
```


### "Total rows is smaller than min allowed 5000" (Vector Index)

BigQuery requires a minimum of 5,000 rows to create a `TREE_AH` vector index. For metadata tables (like `schema_embeddings`), you will likely have fewer rows.

- **Status**: This is NOT a blocker.
- **Behavior**: `VECTOR_SEARCH` will automatically fall back to a flat scan if no index exists. For small tables, this is extremely fast.
- **Action**: You can safely ignore errors from `python scripts/run_embeddings.py --step create-indexes` if your metadata catalog is small. Once you exceed 5,000 rows, re-run the command to enable the index.

### "Connection refused" on LiteLLM

The LiteLLM proxy isn't running or isn't reachable.

```bash
# Check if it's running
curl -s http://localhost:4000/health

# Start it
scripts/start_litellm.sh
```

In Docker, make sure `.env` uses `http://host.docker.internal:4000`, not `localhost`.

### Cross-Project IAM Issues

When the embedding model is in a different project (prod setup):

1. The data project's BigQuery service agent needs `roles/bigquery.connectionUser` on the AI project
2. The AI connection's service account needs `roles/aiplatform.user` on the AI project
3. The data project's service account needs `roles/bigquery.dataViewer` on the AI project's `nl2sql` dataset

```bash
# Find your data project's BQ service agent
DATA_SA="bq-$(gcloud projects describe $DATA_PROJECT --format='value(projectNumber)')@bigquery-encryption.iam.gserviceaccount.com"

# Grant cross-project access
gcloud projects add-iam-policy-binding $AI_PROJECT \
  --member="serviceAccount:$DATA_SA" \
  --role="roles/bigquery.connectionUser"
```

### "No module named 'nl2sql_agent'"

Install the package in editable mode:

```bash
pip install -e .
```

### ADK Can't Find the Agent

ADK expects to be run from the **parent** of the agent package directory, and requires `nl2sql_agent/__init__.py` to export `from . import agent`.

```bash
# Correct: run from repo root
adk web nl2sql_agent

# Wrong: run from inside the package
cd nl2sql_agent && adk web .
```
