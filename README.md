# NL2SQL Agent

Specialized Natural Language to SQL (NL2SQL) agent for Mako Group's trading desk.
Answers questions about trading data by querying BigQuery.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone ...
    cd nl2sql-agent
    ```

2.  **Configure Environment:**
    Copy `.env.example` to `.env` and fill in the required values.
    ```bash
    cp nl2sql_agent/.env.example nl2sql_agent/.env
    ```

3.  **Build Docker Image:**
    ```bash
    docker compose build
    ```

## Running Tests

Run the test suite inside the Docker container:

```bash
docker compose run --rm agent pytest tests/ -v
```

## Running the Agent

Start the agent in interactive terminal mode:

```bash
docker compose run --rm agent
```

## Project Structure

- `nl2sql_agent/`: Main Python package.
    - `agent.py`: ADK agent definitions (`root_agent`, `nl2sql_agent`).
    - `config.py`: Configuration loading (pydantic-settings, reads `.env`).
    - `catalog_loader.py`: YAML catalog loader and validator.
    - `protocols.py`: Interfaces for external dependencies (`BigQueryProtocol`, `EmbeddingProtocol`).
    - `clients.py`: Concrete implementations of protocols.
    - `logging_config.py`: Structured JSON logging via structlog.
- `catalog/`: YAML metadata catalog -- table schemas, column descriptions, routing rules. See [`catalog/README.md`](catalog/README.md).
- `examples/`: Validated Q->SQL example pairs for few-shot retrieval.
- `scripts/`: Embedding pipeline tooling (`run_embeddings.py`, `populate_embeddings.py`).
- `setup/`: SQL scripts and schema extraction tools.
- `schemas/`: Extracted JSON schemas (not in Git).
- `tests/`: Unit tests.

## Metadata System

The agent uses a **two-layer metadata** architecture:

1. **YAML Catalog** (`catalog/`): Version-controlled table/column descriptions, synonyms, and routing rules.
2. **BigQuery Embeddings** (`nl2sql_metadata` dataset): Vector embeddings for semantic search -- routes natural language questions to the right tables and retrieves relevant few-shot examples.

See [`catalog/README.md`](catalog/README.md) for full documentation on how to add tables, update embeddings, and manage the metadata pipeline.
