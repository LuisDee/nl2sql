# Getting Started

How to go from a fresh clone to a working NL2SQL agent — assuming your GCP project already has market data in BigQuery.

> **First time setting up the GCP infrastructure?** See [ENVIRONMENT_SETUP.md](ENVIRONMENT_SETUP.md) Parts 1-2 for Vertex AI connection, embedding model creation, and data table population.

---

## 1. Prerequisites

- **Python 3.11+** and [uv](https://docs.astral.sh/uv/) (or pip)
- **gcloud CLI** installed and authenticated:
  ```bash
  gcloud auth application-default login
  ```
- **GCP project** with data tables already populated in BigQuery (KPI + data datasets)
- **Vertex AI connection + embedding model** already created ([details](ENVIRONMENT_SETUP.md#12-create-vertex-ai-connection))

---

## 2. Clone & Install

```bash
git clone <repo-url> && cd nl2sql-agent
uv sync --dev
```

Verify the install:

```bash
pytest
```

All 733+ tests should pass (no BigQuery or LiteLLM needed — tests use fake clients).

---

## 3. Configure `.env`

```bash
cp nl2sql_agent/.env.example nl2sql_agent/.env
```

Edit `nl2sql_agent/.env` with your values. The key fields:

### Field Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `LITELLM_API_KEY` | *(required)* | API key for the LiteLLM proxy |
| `LITELLM_API_BASE` | *(required)* | LiteLLM proxy URL (e.g. `http://localhost:4000`) |
| `LITELLM_MODEL` | `openai/gemini-3-flash-preview` | LLM model for simple queries. Must include `openai/` prefix. |
| `LITELLM_MODEL_COMPLEX` | `openai/gemini-3-pro-preview` | LLM model for complex queries. Must include `openai/` prefix. |
| `GCP_PROJECT` | `cloud-data-n-base-d4b3` | GCP project containing the data |
| `BQ_LOCATION` | `europe-west2` | BigQuery dataset location |
| `DATASET_PREFIX` | `nl2sql_` | Prefix for all dataset names. Set to `""` for prod. |
| `DEFAULT_EXCHANGE` | `omx` | Exchange code used in dataset name computation |
| `VERTEX_AI_CONNECTION` | *(prod default)* | Fully-qualified Vertex AI connection |
| `EMBEDDING_MODEL_REF` | *(prod default)* | Fully-qualified BQ ML model reference |
| `EMBEDDING_MODEL` | `text-embedding-005` | Underlying Vertex AI model name |

### How Datasets Are Computed

You do **not** need to set `KPI_DATASET`, `DATA_DATASET`, or `METADATA_DATASET` explicitly. They are auto-computed from `DATASET_PREFIX` and `DEFAULT_EXCHANGE`:

| Setting | Formula | Example (prefix=`nl2sql_`, exchange=`omx`) |
|---------|---------|---------------------------------------------|
| `KPI_DATASET` | `{prefix}{exchange}_kpi` | `nl2sql_omx_kpi` |
| `DATA_DATASET` | `{prefix}{exchange}_data` | `nl2sql_omx_data` |
| `METADATA_DATASET` | `{prefix}metadata` | `nl2sql_metadata` |

You can still override any dataset explicitly — explicit values take precedence over the computed ones.

### Example: Dev (same-project, local LiteLLM)

```env
LITELLM_API_KEY=<your-litellm-master-key>
LITELLM_API_BASE=http://localhost:4000
LITELLM_MODEL=openai/claude-haiku
LITELLM_MODEL_COMPLEX=openai/claude-sonnet

GCP_PROJECT=melodic-stone-437916-t3
BQ_LOCATION=europe-west2
DATASET_PREFIX=nl2sql_
DEFAULT_EXCHANGE=omx

VERTEX_AI_CONNECTION=melodic-stone-437916-t3.europe-west2.vertex-ai-connection
EMBEDDING_MODEL_REF=melodic-stone-437916-t3.nl2sql.text_embedding_model
EMBEDDING_MODEL=text-embedding-005

ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS=true
```

Computed datasets: `nl2sql_omx_kpi`, `nl2sql_omx_data`, `nl2sql_metadata`

### Example: Prod (cross-project, hosted LiteLLM)

```env
LITELLM_API_KEY=<prod-litellm-key>
LITELLM_API_BASE=https://litellm.production.mako-cloud.com/
LITELLM_MODEL=openai/gemini-3-flash-preview
LITELLM_MODEL_COMPLEX=openai/gemini-3-pro-preview

GCP_PROJECT=cloud-data-n-base-d4b3
BQ_LOCATION=europe-west2
DATASET_PREFIX=
DEFAULT_EXCHANGE=omx

VERTEX_AI_CONNECTION=cloud-ai-d-base-a2df.europe-west2.vertex-ai-connection
EMBEDDING_MODEL_REF=cloud-ai-d-base-a2df.nl2sql.text_embedding_model
EMBEDDING_MODEL=text-embedding-005

ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS=true
```

Computed datasets: `omx_kpi`, `omx_data`, `metadata`

> **Docker note**: Use `http://host.docker.internal:4000` instead of `http://localhost:4000` for `LITELLM_API_BASE` when running in Docker.

---

## 4. Seed Metadata & Run Embeddings

The agent needs three BigQuery tables in the metadata dataset before it can route questions:

| Table | Contents | Rows |
|-------|----------|------|
| `schema_embeddings` | Table/dataset descriptions for routing | ~17 |
| `column_embeddings` | Column descriptions + synonyms from YAML catalog | ~4,600 |
| `query_memory` | Validated question-to-SQL pairs for few-shot retrieval | ~53 |

### Run the full pipeline

Execute from the repo root, in order:

```bash
# Step 1: Create metadata dataset, tables, populate schema descriptions,
#         generate embeddings, create vector indexes, test search
python scripts/run_embeddings.py --step all

# Step 2: Populate column_embeddings and query_memory from YAML catalog + examples
python scripts/populate_embeddings.py

# Step 3: Generate embeddings for the newly-populated rows
python scripts/run_embeddings.py --step generate-embeddings

# Step 4: Verify vector search works
python scripts/run_embeddings.py --step test-search
```

### Why this ordering?

1. `--step all` creates the BQ infrastructure (dataset, tables), populates schema-level descriptions, and generates their embeddings. But it doesn't know about YAML catalog data.
2. `populate_embeddings.py` reads column definitions from `catalog/*.yaml` and example queries from `examples/*.yaml`, then MERGEs them into `column_embeddings` and `query_memory`. These rows have empty embedding arrays.
3. `--step generate-embeddings` calls `ML.GENERATE_EMBEDDING` on every row where `ARRAY_LENGTH(embedding) = 0` — i.e., the rows just inserted.
4. `--step test-search` runs 5 sample questions through `VECTOR_SEARCH` to verify routing quality.

### Verify it worked

```sql
-- Check row counts
SELECT 'schema_embeddings' AS tbl, COUNT(*) AS total,
  COUNTIF(ARRAY_LENGTH(embedding) > 0) AS with_embedding
FROM `PROJECT.METADATA_DATASET.schema_embeddings`
UNION ALL
SELECT 'column_embeddings', COUNT(*), COUNTIF(ARRAY_LENGTH(embedding) > 0)
FROM `PROJECT.METADATA_DATASET.column_embeddings`
UNION ALL
SELECT 'query_memory', COUNT(*), COUNTIF(ARRAY_LENGTH(embedding) > 0)
FROM `PROJECT.METADATA_DATASET.query_memory`;
```

Replace `PROJECT` and `METADATA_DATASET` with your actual values. All rows should have embeddings.

---

## 5. Start the Agent

### Option A: Web UI (recommended for first run)

Requires a running LiteLLM proxy. Start it first in a separate terminal:

```bash
scripts/start_litellm.sh
```

Then launch the agent:

```bash
scripts/start_local.sh
```

Opens the ADK web UI at http://localhost:8001.

### Option B: Terminal mode

```bash
scripts/start_local.sh -t
```

Interactive terminal mode — no web UI, just a chat prompt.

### Option C: Docker

```bash
docker compose up --build
```

Exposes the ADK web UI at http://localhost:8001. Requires:
- `~/.config/gcloud` for ADC credentials (mounted read-only)
- `nl2sql_agent/.env` configured
- LiteLLM proxy reachable (use `host.docker.internal:4000` for local proxy)

### Smoke test

Ask any of these:

> "What was the average edge on market trades today?"
> "How many quoter trades did we do yesterday?"
> "Show me the top 10 symbols by instant PnL today"

The agent should route to the correct table, generate SQL, validate it via dry run, execute it, and return results.

---

## 6. Add to Gemini CLI or Claude Code (MCP)

The agent can be used as an MCP tool from Gemini CLI, Claude Code, or Claude Desktop.

### Gemini CLI

The repo ships a `.gemini/settings.json` that configures the MCP server. Copy it to your home directory or symlink it:

```bash
# If you don't have ~/.gemini/settings.json yet:
cp .gemini/settings.json ~/.gemini/settings.json

# If you already have one, merge the mcpServers block:
```

```json
{
  "mcpServers": {
    "mako-trading": {
      "command": ".venv/bin/python",
      "args": ["-m", "nl2sql_agent.mcp_server"],
      "timeout": 120000,
      "trust": true
    }
  }
}
```

> **Note**: The `command` uses `.venv/bin/python` — adjust the path if your virtual environment is elsewhere. The `cwd` defaults to the directory where Gemini CLI is launched, so run it from the repo root.

### Claude Code

Create `.mcp.json` at the repo root:

```json
{
  "mcpServers": {
    "mako-trading": {
      "command": "python",
      "args": ["-m", "nl2sql_agent.mcp_server"],
      "env": { "LITELLM_API_KEY": "${LITELLM_API_KEY}" }
    }
  }
}
```

### Claude Desktop

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "mako-trading": {
      "command": "/path/to/nl2sql-agent/.venv/bin/python",
      "args": ["-m", "nl2sql_agent.mcp_server"],
      "cwd": "/path/to/nl2sql-agent"
    }
  }
}
```

### How it works

The MCP server (`nl2sql_agent/mcp_server.py`) exposes a single tool — `ask_trading_data` — that wraps the full ADK agent pipeline. When you ask a trading question in Gemini CLI or Claude, it:

1. Creates a fresh ADK session
2. Runs the question through the agent (cache check, vector search, SQL generation, validation, execution)
3. Emits progress notifications ("Searching for relevant tables...", "Validating SQL...", etc.)
4. Returns the final answer

### Prerequisites for MCP

- LiteLLM proxy running
- GCP auth via ADC (`gcloud auth application-default login`)
- Python environment with `nl2sql-agent` installed (`uv sync` or `pip install -e .`)

---

## 7. Verification Checklist

Run through these gates to confirm everything is working:

1. `pytest` — all 733+ unit tests pass
2. `.env` exists at `nl2sql_agent/.env` with correct `GCP_PROJECT`
3. `gcloud auth application-default print-access-token` succeeds
4. `schema_embeddings` has ~17 rows with embeddings
5. `column_embeddings` has ~4,600 rows with embeddings
6. `query_memory` has ~53 rows with embeddings
7. `python scripts/run_embeddings.py --step test-search` returns sensible routing
8. LiteLLM proxy responds: `curl -s http://localhost:4000/health`
9. `scripts/start_local.sh` opens web UI — ask a test question and get an answer

---

## 8. Refreshing After Catalog Changes

When you update YAML catalog files (column descriptions, synonyms, business rules) or add example queries in `examples/*.yaml`:

```bash
# 1. Re-merge YAML data into BigQuery (uses MERGE — idempotent)
python scripts/populate_embeddings.py

# 2. Generate embeddings for new/updated rows only
python scripts/run_embeddings.py --step generate-embeddings

# 3. (Optional) Verify search quality
python scripts/run_embeddings.py --step test-search
```

If you changed schema-level descriptions (table routing text in `run_embeddings.py`):

```bash
python scripts/run_embeddings.py --step populate-schema
python scripts/run_embeddings.py --step generate-embeddings
```

All operations are idempotent — safe to re-run at any time.

---

## 9. Troubleshooting Quick Reference

| Problem | Likely cause | Fix |
|---------|-------------|-----|
| `LLM Provider NOT provided` | Model name missing `openai/` prefix | Add `openai/` to `LITELLM_MODEL` and `LITELLM_MODEL_COMPLEX` |
| `Not found: Model` | Embedding model doesn't exist | Create it — see [ENVIRONMENT_SETUP.md Part 1.3](ENVIRONMENT_SETUP.md#13-create-the-embedding-model) |
| `Permission denied` on ML.GENERATE_EMBEDDING | Missing IAM role | Grant `roles/aiplatform.user` to the connection's service account |
| VECTOR_SEARCH returns empty | Embeddings not generated | Run `python scripts/run_embeddings.py --step generate-embeddings` |
| `Connection refused` on LiteLLM | Proxy not running | Start it: `scripts/start_litellm.sh` |
| `No module named 'nl2sql_agent'` | Package not installed | Run `uv sync --dev` or `pip install -e .` |
| ADK can't find agent | Wrong working directory | Run ADK commands from repo root |
| Docker can't reach LiteLLM | Wrong hostname | Use `http://host.docker.internal:4000` in `.env` |
| "Total rows smaller than 5000" on vector index | Normal for small catalogs | Ignore — VECTOR_SEARCH falls back to flat scan automatically |

For detailed troubleshooting, see [ENVIRONMENT_SETUP.md Part 6](ENVIRONMENT_SETUP.md#part-6-troubleshooting).
