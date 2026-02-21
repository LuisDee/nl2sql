# NL2SQL Agent

Specialized NL2SQL agent for Mako Group's trading desk. Converts natural language questions into BigQuery SQL queries across 10 exchanges.

## Architecture

```
User Question
    |
    v
root_agent (mako_assistant)  -- delegates trading questions
    |
    v
nl2sql_agent (8 tools, temperature=0.1)
    |
    +-> check_semantic_cache      -- BQ cosine similarity (~0.10 threshold)
    +-> resolve_exchange          -- alias/symbol lookup → dataset routing
    +-> vector_search_columns     -- BQ VECTOR_SEARCH on column embeddings
    +-> fetch_few_shot_examples   -- cached from vector search
    +-> load_yaml_metadata        -- YAML catalog (table/column descriptions)
    +-> dry_run_sql               -- BQ dry run validation
    +-> execute_sql               -- read-only BQ execution (max 1000 rows)
    +-> save_validated_query      -- learning loop (BQ INSERT + embedding)
```

**Two-layer metadata:** YAML catalog (version-controlled descriptions) + BQ vector embeddings (semantic search). See [`catalog/README.md`](catalog/README.md).

## Quick Start

**Prerequisites:** Python 3.11+, [uv](https://docs.astral.sh/uv/), `gcloud` CLI with ADC configured.

```bash
# Install dependencies
uv sync --dev

# Configure environment
cp nl2sql_agent/.env.example nl2sql_agent/.env
# Edit .env with your GCP project, LiteLLM endpoint, etc.

# Run tests
make test

# Start ADK web UI (requires LiteLLM proxy running)
make serve
```

## Development

```bash
make help          # Show all available targets
make lint          # Ruff lint + format (autofix)
make type-check    # Mypy type checking
make test          # Unit tests
make test-cov      # Tests with coverage report
make pre-commit    # Run all pre-commit hooks
make ci            # Full CI locally via act
```

## Project Structure

| Directory | Purpose |
|-----------|---------|
| `nl2sql_agent/` | Main package: agent, config, tools, callbacks, prompts |
| `nl2sql_agent/tools/` | ADK tool functions (vector search, SQL execution, caching) |
| `catalog/` | YAML metadata catalog (table/column descriptions, routing) |
| `examples/` | Validated Q->SQL pairs for few-shot retrieval |
| `scripts/` | Embedding pipeline and local dev startup |
| `setup/` | BQ dataset creation SQL and schema extraction |
| `eval/` | Evaluation framework (gold queries, offline/online runner) |
| `tests/` | Unit tests (429+) |
| `tests/integration/` | Integration tests (requires live BQ + LiteLLM) |
| `data/` | Symbol-to-exchange mapping CSV |
| `docs/` | Architecture reports and planning documents |

## Configuration

All config is managed via `nl2sql_agent/.env`. See [`.env.example`](nl2sql_agent/.env.example) for all available settings with defaults and descriptions.

Key variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `GCP_PROJECT` | Yes | BigQuery project ID |
| `LITELLM_API_KEY` | Yes | LiteLLM proxy API key |
| `LITELLM_API_BASE` | Yes | LiteLLM proxy URL |
| `KPI_DATASET` | No | KPI dataset name (default: `nl2sql_omx_kpi`) |
| `DATA_DATASET` | No | Data dataset name (default: `nl2sql_omx_data`) |

## Deployment

**Docker Compose:**
```bash
docker compose up
```

**MCP Server** (for Gemini CLI / Claude Code):
```bash
python -m nl2sql_agent.mcp_server
```
