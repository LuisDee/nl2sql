# NL2SQL Agent — Project Overview

## What We're Building

A **natural-language-to-SQL tooling agent** for an options market-making firm (Mako Group). Traders ask questions in plain English — "what was the edge on our quoter trades yesterday?" — and the agent figures out which BigQuery table to query, writes correct SQL, validates it, executes it, and returns the answer.

The agent runs as a **Google ADK sub-agent** exposed as a tool service. It will be consumed by CLI agents (tested with Claude Code, production will be Gemini CLI) so that any conversational AI can ask data questions and get accurate answers back.

## Architecture

```
CLI Agent (Claude Code / Gemini CLI)
  │  asks: "what was PnL by delta bucket yesterday?"
  │
  └─► NL2SQL Agent (Google ADK, LiteLLM proxy → Gemini)
        │
        ├─ 1. Vector Search    → find right table(s) via BigQuery VECTOR_SEARCH
        ├─ 2. Load Metadata    → YAML catalog with column descriptions, synonyms, business rules
        ├─ 3. Few-Shot Examples → retrieve similar validated Q→SQL pairs from query_memory
        ├─ 4. Generate SQL     → LLM builds BigQuery SQL with full context
        ├─ 5. Dry Run          → BigQuery validates syntax + permissions
        ├─ 6. Execute          → BigQuery runs the query, returns results
        └─ 7. Learning Loop    → save successful queries for future retrieval
```

## Key Components

### Two-Layer Metadata System

**Layer 1 — YAML Catalog** (static, in-repo):
- One YAML file per table with column descriptions, trader synonyms, business rules
- Dataset-level routing rules (e.g. "broker" → kpi_brokertrade, "quoting levels" → data.quotertrade)
- Disambiguation for confusing overlaps (kpi.quotertrade vs data.quotertrade)

**Layer 2 — BigQuery Vector Embeddings** (dynamic, in BQ):
- `schema_embeddings`: table/dataset descriptions embedded as 768-dim vectors (text-embedding-005)
- `column_embeddings`: critical column descriptions embedded for column-level routing
- `query_memory`: 30+ validated Q→SQL pairs embedded for few-shot retrieval
- All searched via `VECTOR_SEARCH` with COSINE distance at query time

### Protocol-Based Dependency Injection
Every external dependency (BigQuery, embeddings) gets a Python Protocol interface. Business logic depends on abstractions, never concrete clients. This makes testing and swapping implementations trivial.

## Dev vs Production Environment

**IMPORTANT**: We develop and test against a **dev GCP project** (`melodic-stone-437916-t3`) with sample data. Production is `cloud-data-n-base-d4b3`. The table schemas and structure are identical, but dev has thin data slices.

The agent is designed so that swapping environments requires only changing the `.env` config — no code changes.

| | Dev (current) | Production |
|---|---|---|
| **GCP Project** | `melodic-stone-437916-t3` | `cloud-data-n-base-d4b3` |
| **Vertex AI Connection** | `melodic-stone-437916-t3.europe-west2.vertex-ai-connection` | `cloud-ai-d-base-a2df.europe-west2.vertex-ai-connection` |
| **Embedding Model** | `melodic-stone-437916-t3.nl2sql.text_embedding_model` | `cloud-ai-d-base-a2df.nl2sql.text_embedding_model` |
| **LiteLLM** | `http://localhost:4000` (local proxy → Claude) | `https://litellm.production.mako-cloud.com/` (→ Gemini) |
| **LLM Models** | `openai/claude-haiku` / `openai/claude-sonnet` | `openai/gemini-3-flash-preview` / `openai/gemini-3-pro-preview` |

### Current Dev `.env` (`nl2sql_agent/.env`)

```env
LITELLM_API_KEY=<from pass: api/litellm-master>
LITELLM_API_BASE=http://localhost:4000
LITELLM_MODEL=openai/claude-haiku
LITELLM_MODEL_COMPLEX=openai/claude-sonnet
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

## Tables In Scope (13 total)

### KPI Dataset (`nl2sql_omx_kpi`) — Gold Layer, 5 tables
Performance metrics per trade. One table per trade origin, all sharing KPI columns (edge_bps, instant_pnl, delta_bucket, slippage).

| Table | What It Contains |
|-------|-----------------|
| `markettrade` | Exchange/market trades — **default** when trade type unspecified |
| `quotertrade` | Auto-quoter fill KPIs (NOT raw activity) |
| `brokertrade` | Broker trades — has `account` field for BGC/MGN comparison |
| `clicktrade` | Manual click trades |
| `otoswing` | OTO swing trades |
NOTE SOME OF THESE ARE SUFFIXED BETS _ THEY ONLY HAVE SYMBOL BETS TO REDUCE SIZE


### Data Dataset (`nl2sql_omx_data`) — Silver Layer, 8 tables
Raw and enriched market/activity data.

| Table | What It Contains |
|-------|-----------------|
| `theodata` | Theo pricing snapshots (theo, delta, vol, vega per strike/timestamp) |
| `quotertrade` | **Raw** quoter activity (timestamps, levels, sizes) — different from KPI quotertrade |
| + 6 others | Various market data tables |

### Critical Routing Challenge
`kpi.quotertrade` (performance metrics: edge, PnL) vs `data.quotertrade` (raw activity: timestamps, levels) are **different tables** that sound similar. The agent must disambiguate based on question intent.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Agent Framework | Google ADK (Agent Development Kit) |
| LLM Access | LiteLLM proxy → Gemini models |
| Database | BigQuery (read-only queries) |
| Embeddings | text-embedding-005 via BigQuery ML.GENERATE_EMBEDDING |
| Vector Search | BigQuery VECTOR_SEARCH (brute-force, <200 rows) |
| Config | pydantic-settings from `.env` |
| Logging | structlog (JSON) |
| Container | Docker + docker-compose |
| Testing | pytest |

## Production Config (reference only — NOT active)

Production `.env-prod` exists at `nl2sql_agent/.env-prod` for reference. Uses `cloud-data-n-base-d4b3` project with Gemini models via Mako's hosted LiteLLM proxy. Do not use until production deployment.

## Implementation Tracks

| Track | What | Status |
|-------|------|--------|
| **01 — Foundation** | Repo, Docker, ADK skeleton, config, dev dataset, schema extraction | Plan complete |
| **02 — Context Layer** | YAML catalog, 32 Q→SQL examples, 3 embedding tables, vector search validation | Plan complete |
| **03 — Agent Tools** | Wire vector_search, metadata_loader, sql_validator, sql_executor, learning_loop into agent | Planned |
| **04 — System Prompts** | Refine routing instructions, end-to-end testing | Planned |
| **05 — Eval & Hardening** | Gold-standard eval set, accuracy metrics, retry logic | Planned |

## Key Conventions (Non-Negotiable)

1. **ADK**: `root_agent` variable name mandatory. `__init__.py` must contain `from . import agent`.
2. **LiteLLM**: Import `LiteLlm` (camelCase), never `LiteLLM`. Model string goes through proxy.
3. **Config**: All config via `from nl2sql_agent.config import settings`. Never `os.getenv()`.
4. **Protocols**: All BigQuery/embedding interactions through Protocol interfaces.
5. **SQL Safety**: Read-only. Always filter on partition column (`trade_date`). Always LIMIT unless explicit.
6. **YAML**: Always `yaml.safe_load()`, never `yaml.load()`.
7. **Embeddings**: `RETRIEVAL_DOCUMENT` task_type for stored content, `RETRIEVAL_QUERY` for search queries. COSINE distance. No vector indexes needed (<200 rows).
8. **Idempotent SQL**: All embedding scripts use `CREATE OR REPLACE` or `MERGE`. Never duplicate on re-run.
9. **LiteLLM Model Naming**: All model strings MUST include a provider prefix (e.g. `openai/`) when going through the LiteLLM proxy. Without it, litellm cannot determine the API protocol and raises `BadRequestError`.
