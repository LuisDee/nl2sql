# Track 01: Foundation — Implementation Plan

## Objective

Set up the complete project skeleton: Docker container, Python package, ADK agent delegation (root → sub-agent), pydantic-settings configuration, BigQuery dev dataset with thin data slices, and schema extraction. At the end of this track, `adk run` inside the container shows working delegation: a greeting goes to the root agent, a data question triggers `transfer_to_agent(agent_name='nl2sql_agent')`.

---

## CRITICAL ADK CONVENTIONS

These are non-negotiable. ADK's CLI (`adk run`, `adk web`) discovers agents by convention. If you break these rules, nothing works.

1. **The agent package MUST be a directory** containing `__init__.py` and `agent.py`.
2. **`agent.py` MUST define a module-level variable called `root_agent`**. This is the ONLY name ADK looks for. Do NOT name it `main_agent`, `app`, `my_agent`, or anything else.
3. **`__init__.py` MUST contain `from . import agent`**. This is how ADK discovers the agent module.
4. **`adk run` and `adk web` are run from the PARENT directory** of the agent package. If the package is `nl2sql_agent/`, you run `adk run` from the directory that CONTAINS `nl2sql_agent/`.
5. **Agent names must be valid Python identifiers**. No hyphens, no spaces. Use underscores.
6. **Agent name cannot be `"user"`** — this is reserved by ADK for end-user input.
7. **An agent instance can ONLY be added as a sub-agent once**. Do NOT add the same instance to multiple parent agents.

### ADK Directory Structure (MANDATORY)

```
project_root/              ← run `adk run nl2sql_agent/` from HERE
├── nl2sql_agent/          ← this is the agent package
│   ├── __init__.py        ← MUST contain: from . import agent
│   ├── agent.py           ← MUST define: root_agent = ...
│   └── .env               ← environment variables
├── pyproject.toml
├── Dockerfile
└── ...
```

**DO NOT** nest the agent package deeper (e.g., `src/agent/nl2sql_agent/`). ADK expects it at the top level relative to where you run `adk run`.

---

## CRITICAL LiteLLM CONVENTIONS

We use LiteLLM as a proxy to access LLMs. This means we do NOT use Google's native Gemini integration. We use the `LiteLlm` wrapper class.

1. **Import path**: `from google.adk.models.lite_llm import LiteLlm`
2. **Model string format**: The model string is passed directly to litellm. For Gemini models via a LiteLLM proxy, use the format the proxy expects (e.g., `"gemini-3-flash-preview"`). Check your proxy's model list.
3. **API key**: Set via `LITELLM_API_KEY` environment variable. Do NOT hardcode.
4. **API base**: Set via `LITELLM_API_BASE` environment variable pointing to your LiteLLM proxy URL.
5. **Suppress Gemini warnings**: Set `ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS=true` in `.env` to avoid noisy warnings about using Gemini via LiteLLM.

### LiteLlm Usage Pattern

```python
from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm

agent = LlmAgent(
    name="my_agent",
    model=LiteLlm(model="gemini-3-flash-preview"),  # model string your proxy expects
    instruction="...",
    description="...",
)
```

**DO NOT** use `model="gemini-3-flash-preview"` (bare string). That uses ADK's native Gemini integration which requires a Google API key, not LiteLLM.

---

## CRITICAL DESIGN PATTERN: Protocol-Based Dependency Injection

Every external dependency (BigQuery, Vertex AI embeddings, any future API) gets a `Protocol` interface. Business logic and agent tools depend on the protocol, NEVER on the concrete client class.

```
protocols.py          → defines BigQueryProtocol, EmbeddingProtocol
clients.py            → defines LiveBigQueryClient, LiveEmbeddingClient (real services)
tests/fakes.py        → defines FakeBigQueryClient, FakeEmbeddingClient (test doubles)
```

**Why this matters**:
- **Unit tests run instantly** — no BigQuery calls, no Vertex AI calls, no network at all
- **Integration tests run against real services** — same business logic, different client
- **Swapping backends** — if you move from BigQuery to Postgres, you change `clients.py`, not your tools

**The rule**: If a function calls BigQuery, it must accept a `BigQueryProtocol` parameter. It must NEVER do `from google.cloud import bigquery` and create its own client.

```python
# CORRECT — testable, injectable
def execute_and_format(sql: str, bq: BigQueryProtocol) -> str:
    df = bq.execute_query(sql)
    return df.to_markdown()

# WRONG — untestable, tightly coupled
def execute_and_format(sql: str) -> str:
    from google.cloud import bigquery
    client = bigquery.Client()
    df = client.query(sql).to_dataframe()
    return df.to_markdown()
```

This pattern is ESTABLISHED in Track 01 and ENFORCED in all subsequent tracks.

**DO NOT** use `from google.adk.models.lite_llm import LiteLLM` (capital letters). The class name is `LiteLlm` (camelCase).

---

## File-by-File Specification

### 1. `pyproject.toml`

**Path**: `project_root/pyproject.toml`

```toml
[project]
name = "nl2sql-agent"
version = "0.1.0"
description = "NL2SQL sub-agent for Mako Group trading data"
requires-python = ">=3.11"
dependencies = [
    "google-adk>=1.0.0",
    "google-cloud-bigquery>=3.25.0",
    "google-cloud-aiplatform>=1.60.0",
    "litellm>=1.40.0",
    "pydantic>=2.0.0",
    "pydantic-settings>=2.0.0",
    "pyyaml>=6.0",
    "python-dotenv>=1.0.0",
    "structlog>=24.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
]

[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.backends._legacy:_Backend"
```

**DO NOT** add `google-genai` or `google-generativeai` to dependencies. LiteLLM handles model access.

**DO NOT** use `poetry` or `pdm`. Use `pip install -e .` for editable install.

---

### 2. `Dockerfile`

**Path**: `project_root/Dockerfile`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (Docker layer caching)
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

# Copy application code
COPY . .

# Install the package in editable mode (now with all source files)
RUN pip install --no-cache-dir -e ".[dev]"

# Default command: run the agent in terminal mode
# adk run expects to be in the PARENT of the agent package directory
CMD ["adk", "run", "nl2sql_agent"]
```

**DO NOT** use `python:3.12` — some ADK dependencies may have compatibility issues. Use `3.11-slim`.

**DO NOT** set `ENTRYPOINT`. Use `CMD` so it can be overridden for testing.

---

### 3. `docker-compose.yml`

**Path**: `project_root/docker-compose.yml`

```yaml
services:
  agent:
    build: .
    volumes:
      - .:/app                    # Mount source code for live development
    env_file:
      - nl2sql_agent/.env         # Load environment variables
    stdin_open: true              # Required for interactive adk run
    tty: true                     # Required for interactive adk run
    ports:
      - "8000:8000"               # For adk web (when used)
    command: adk run nl2sql_agent  # Default: terminal chat mode
```

**IMPORTANT**: Both `stdin_open: true` and `tty: true` are REQUIRED for `adk run` to work interactively in Docker. Without them, the container exits immediately because there's no stdin.

To run interactively:
```bash
docker compose run --rm agent
```

To run `adk web` instead:
```bash
docker compose run --rm -p 8000:8000 agent adk web --host 0.0.0.0 --port 8000 nl2sql_agent
```

**DO NOT** use `docker compose up` for interactive terminal mode. Use `docker compose run --rm agent`.

---

### 4. `.env` file

**Path**: `nl2sql_agent/.env`

```env
# LiteLLM Configuration
LITELLM_API_KEY=sk-bRMOi4qeZzcqZt14IlnhEw
LITELLM_API_BASE=https://litellm.production.mako-cloud.com/
LITELLM_MODEL=gemini-3-flash-preview
LITELLM_MODEL_COMPLEX=gemini-3-pro-preview

# Google Cloud / BigQuery
GCP_PROJECT=cloud-data-n-base-d4b3
BQ_LOCATION=europe-west2
KPI_DATASET=nl2sql_omx_kpi
DATA_DATASET=nl2sql_omx_data
METADATA_DATASET=nl2sql_metadata

# Vertex AI (for embeddings — connection and model both in cloud-ai project)
VERTEX_AI_CONNECTION=cloud-ai-d-base-a2df.europe-west2.vertex-ai-connection
EMBEDDING_MODEL_REF=cloud-ai-d-base-a2df.nl2sql.text_embedding_model
EMBEDDING_MODEL=text-embedding-005

# ADK
ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS=true
```

**IMPORTANT**: The `.env` file goes INSIDE the agent package directory (`nl2sql_agent/.env`), not at the project root. This is where ADK looks for it.

**DO NOT** commit `.env` to Git. Add it to `.gitignore`.

**DO NOT** put quotes around values in the `.env` file.

**NOTE on datasets**: There are TWO data datasets plus one for metadata:
- `nl2sql_omx_kpi` — Gold layer. KPI-enriched trade data (edge_bps, instant_pnl, slippage, delta_bucket). 5 tables.
- `nl2sql_omx_data` — Silver layer. Raw trade/market data (exact timestamps, prices, sizes, theo values). 8 tables.
- `nl2sql_metadata` — Agent infrastructure. Embeddings, query memory, schema descriptions. Created in Track 02.

---

### 5. `.env.example`

**Path**: `nl2sql_agent/.env.example`

Same as `.env` but with placeholder values:

```env
# LiteLLM Configuration
LITELLM_API_KEY=your-litellm-api-key
LITELLM_API_BASE=https://litellm.production.mako-cloud.com/
LITELLM_MODEL=gemini-3-flash-preview
LITELLM_MODEL_COMPLEX=gemini-3-pro-preview

# Google Cloud / BigQuery
GCP_PROJECT=cloud-data-n-base-d4b3
BQ_LOCATION=europe-west2
KPI_DATASET=nl2sql_omx_kpi
DATA_DATASET=nl2sql_omx_data
METADATA_DATASET=nl2sql_metadata

# Vertex AI (for embeddings — connection and model both in cloud-ai project)
VERTEX_AI_CONNECTION=cloud-ai-d-base-a2df.europe-west2.vertex-ai-connection
EMBEDDING_MODEL_REF=cloud-ai-d-base-a2df.nl2sql.text_embedding_model
EMBEDDING_MODEL=text-embedding-005

# ADK
ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS=true
```

This file IS committed to Git. It documents what environment variables are required.

---

### 6. `.gitignore`

**Path**: `project_root/.gitignore`

```gitignore
# Environment
.env
*.env
!.env.example

# Python
__pycache__/
*.py[cod]
*.egg-info/
dist/
build/
.eggs/

# Virtual environment
.venv/
venv/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Test
.pytest_cache/
htmlcov/
.coverage

# Extracted schemas (regeneratable)
schemas/*.json
```

---

### 7. Configuration Module

**Path**: `nl2sql_agent/config.py`

```python
"""Application configuration loaded from environment variables via pydantic-settings.

Usage:
    from nl2sql_agent.config import settings
    print(settings.gcp_project)
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All application settings. Loaded from environment variables and .env file.

    Environment variables are case-insensitive. For example, GCP_PROJECT or
    gcp_project will both work.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore extra env vars not defined here
    )

    # --- LiteLLM ---
    litellm_api_key: str = Field(description="LiteLLM proxy API key")
    litellm_api_base: str = Field(description="LiteLLM proxy base URL")
    litellm_model: str = Field(
        default="gemini-3-flash-preview",
        description="Default LLM model string for LiteLLM proxy",
    )
    litellm_model_complex: str = Field(
        default="gemini-3-pro-preview",
        description="Complex query LLM model string for LiteLLM proxy",
    )

    # --- Google Cloud / BigQuery ---
    gcp_project: str = Field(
        default="cloud-data-n-base-d4b3",
        description="Google Cloud project ID",
    )
    bq_location: str = Field(
        default="europe-west2",
        description="BigQuery dataset location (London)",
    )
    kpi_dataset: str = Field(
        default="nl2sql_omx_kpi",
        description="BigQuery dataset for KPI gold-layer tables",
    )
    data_dataset: str = Field(
        default="nl2sql_omx_data",
        description="BigQuery dataset for raw silver-layer data tables",
    )
    metadata_dataset: str = Field(
        default="nl2sql_metadata",
        description="BigQuery dataset for embeddings and query memory",
    )

    # --- Vertex AI (embeddings only) ---
    vertex_ai_connection: str = Field(
        default="cloud-ai-d-base-a2df.europe-west2.vertex-ai-connection",
        description="BigQuery Vertex AI connection for embedding model (cross-project)",
    )
    embedding_model_ref: str = Field(
        default="cloud-ai-d-base-a2df.nl2sql.text_embedding_model",
        description="Fully qualified BigQuery ML model reference for text embeddings",
    )
    embedding_model: str = Field(
        default="text-embedding-005",
        description="Vertex AI text embedding model name (underlying endpoint)",
    )


# Singleton instance — import this everywhere
settings = Settings()
```

**IMPORTANT DETAILS**:
- `extra="ignore"` prevents crashes when `.env` has vars not defined in the model (like `ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS`).
- The `settings` singleton is created at module import time. This is the standard pydantic-settings pattern.
- Every field has a `description` — this serves as documentation.
- Fields with `default=` are optional in `.env`. Fields without `default=` are REQUIRED and will raise `ValidationError` if missing.

**DO NOT** use `os.getenv()` anywhere in the codebase. Always use `settings.field_name`.

**DO NOT** use `@lru_cache` on a settings factory function. The singleton pattern above is simpler and equivalent.

**DO NOT** put `SecretStr` on `litellm_api_key`. LiteLLM needs the raw string value; `SecretStr` would require `.get_secret_value()` everywhere and add complexity for no benefit in a Docker container.

---

### 8. Logging Module

**Path**: `nl2sql_agent/logging_config.py`

```python
"""Structured JSON logging configuration.

Usage:
    from nl2sql_agent.logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("something happened", table="theodata", rows=42)
"""

import logging
import structlog


def setup_logging() -> None:
    """Configure structlog for JSON output. Call once at startup."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a named logger instance.

    Args:
        name: Logger name, typically __name__ of the calling module.

    Returns:
        A structlog bound logger that outputs JSON.
    """
    return structlog.get_logger(name)
```

**DO NOT** use `logging.basicConfig()`. Use structlog for JSON output.

**DO NOT** use `structlog.dev.ConsoleRenderer()` in the processor chain. We want JSON, not pretty-printed dev output.

---

### 9. Protocols (Dependency Interfaces)

**Path**: `nl2sql_agent/protocols.py`

This is the most important architectural pattern in the project. Every external dependency gets a `Protocol` (Python's structural typing interface). Your business logic depends on the protocol, never on the concrete client. This means:

- **Unit tests** use lightweight mock/fake implementations (no network calls)
- **Integration tests** use real clients against live endpoints
- **Swapping implementations** (e.g., BigQuery → PostgreSQL) only changes the concrete class, not the business logic

```python
"""Protocols (interfaces) for external dependencies.

All external services are accessed through these protocols. Business logic
and agent tools depend on these abstractions, NEVER on concrete clients.

Unit tests provide mock implementations.
Integration tests provide real implementations.

Usage:
    # In agent tools or business logic:
    def my_function(bq: BigQueryProtocol) -> str:
        results = bq.execute_query("SELECT 1")
        return results.to_markdown()

    # In production (agent.py):
    from nl2sql_agent.clients import LiveBigQueryClient
    client = LiveBigQueryClient(project="my-project")
    my_function(client)

    # In tests:
    from tests.fakes import FakeBigQueryClient
    client = FakeBigQueryClient(mock_results={"SELECT 1": ...})
    my_function(client)
"""

from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class BigQueryProtocol(Protocol):
    """Interface for BigQuery operations.

    Concrete implementations:
    - LiveBigQueryClient (nl2sql_agent/clients.py) — real BigQuery
    - FakeBigQueryClient (tests/fakes.py) — in-memory mock for unit tests
    """

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame.

        Args:
            sql: BigQuery SQL query string. Must be a SELECT statement.

        Returns:
            Query results as a pandas DataFrame.

        Raises:
            BigQueryError: If the query fails.
        """
        ...

    def dry_run_query(self, sql: str) -> dict:
        """Validate a SQL query without executing it.

        Args:
            sql: BigQuery SQL query string.

        Returns:
            Dict with keys:
                - "valid" (bool): Whether the query is syntactically valid.
                - "total_bytes_processed" (int): Estimated bytes if valid.
                - "error" (str | None): Error message if invalid.
        """
        ...

    def get_table_schema(self, dataset: str, table: str) -> list[dict]:
        """Get schema for a table.

        Args:
            dataset: BigQuery dataset name.
            table: Table name.

        Returns:
            List of dicts with keys: name, type, mode, description.
        """
        ...


@runtime_checkable
class EmbeddingProtocol(Protocol):
    """Interface for text embedding generation.

    Used in Track 02+ for vector search. Defined here so the interface
    is established from the start.

    Concrete implementations:
    - LiveEmbeddingClient (nl2sql_agent/clients.py) — Vertex AI via BigQuery
    - FakeEmbeddingClient (tests/fakes.py) — returns dummy vectors for testing
    """

    def embed_text(self, text: str) -> list[float]:
        """Generate an embedding vector for a text string.

        Args:
            text: Text to embed.

        Returns:
            Embedding vector as a list of floats.
        """
        ...

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for multiple texts.

        Args:
            texts: List of texts to embed.

        Returns:
            List of embedding vectors, one per input text.
        """
        ...
```

**CRITICAL RULES for protocols**:

1. `@runtime_checkable` — allows `isinstance()` checks at runtime, useful for validation.
2. Every method has a docstring specifying args, returns, and raises. This IS the contract.
3. The `...` (Ellipsis) in method bodies is correct — this is Protocol syntax, not incomplete code.
4. Protocols live in ONE file (`protocols.py`). Do NOT scatter them across modules.
5. Business logic functions accept `BigQueryProtocol` as a parameter, NOT `bigquery.Client`.

**DO NOT** use `abc.ABC` or `abc.abstractmethod`. Python `Protocol` is the modern approach — it uses structural (duck) typing, not nominal (inheritance) typing. A class satisfies a Protocol if it has the right methods, even without explicitly inheriting from it.

**DO NOT** put concrete implementations in `protocols.py`. Only abstract interfaces go here.

---

### 10. Concrete Clients

**Path**: `nl2sql_agent/clients.py`

```python
"""Concrete implementations of external service protocols.

These classes implement the protocols defined in protocols.py using
real external services (BigQuery, Vertex AI, etc.).

For unit testing, use the fakes in tests/fakes.py instead.
"""

import pandas as pd
from google.cloud import bigquery

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger

logger = get_logger(__name__)


class LiveBigQueryClient:
    """Real BigQuery client. Implements BigQueryProtocol.

    Usage:
        client = LiveBigQueryClient()
        df = client.execute_query("SELECT * FROM my_table LIMIT 10")
    """

    def __init__(self, project: str | None = None, location: str | None = None):
        self._project = project or settings.gcp_project
        self._location = location or settings.bq_location
        self._client = bigquery.Client(
            project=self._project,
            location=self._location,
        )
        logger.info(
            "bigquery_client_initialised",
            project=self._project,
            location=self._location,
        )

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame."""
        logger.info("bigquery_execute", sql_preview=sql[:200])
        results = self._client.query(sql).to_dataframe()
        logger.info("bigquery_results", rows=len(results))
        return results

    def dry_run_query(self, sql: str) -> dict:
        """Validate a SQL query via dry run."""
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        try:
            job = self._client.query(sql, job_config=job_config)
            return {
                "valid": True,
                "total_bytes_processed": job.total_bytes_processed,
                "error": None,
            }
        except Exception as e:
            return {
                "valid": False,
                "total_bytes_processed": 0,
                "error": str(e),
            }

    def get_table_schema(self, dataset: str, table: str) -> list[dict]:
        """Get schema for a table."""
        table_ref = f"{self._project}.{dataset}.{table}"
        bq_table = self._client.get_table(table_ref)
        return [
            {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description or "",
            }
            for field in bq_table.schema
        ]
```

**IMPORTANT**: This class does NOT inherit from `BigQueryProtocol`. It doesn't need to — Python Protocols use structural typing. As long as the class has the right method signatures, it satisfies the protocol. You CAN add `isinstance(client, BigQueryProtocol)` checks if you want runtime validation, thanks to `@runtime_checkable`.

**DO NOT** instantiate `LiveBigQueryClient` at module level. Create instances in the functions that need them, or use dependency injection.

---

### 11. Test Fakes

**Path**: `tests/fakes.py`

```python
"""Fake implementations of protocols for unit testing.

These replace real external services with in-memory implementations
that return predictable, controlled data.

Usage in tests:
    from tests.fakes import FakeBigQueryClient
    client = FakeBigQueryClient()
    client.add_result("SELECT 1", pd.DataFrame({"col": [1]}))
    result = client.execute_query("SELECT 1")
"""

import pandas as pd


class FakeBigQueryClient:
    """In-memory fake BigQuery client. Implements BigQueryProtocol.

    Register expected queries and their results before use.
    Raises KeyError if an unregistered query is executed.
    """

    def __init__(self):
        self._results: dict[str, pd.DataFrame] = {}
        self._schemas: dict[str, list[dict]] = {}
        self._dry_run_responses: dict[str, dict] = {}
        self.executed_queries: list[str] = []  # Tracks what was called (for assertions)

    def add_result(self, sql: str, result: pd.DataFrame) -> None:
        """Register an expected query and its result.

        Args:
            sql: The exact SQL string that will be passed to execute_query.
            result: The DataFrame to return when this SQL is executed.
        """
        self._results[sql] = result

    def add_schema(self, dataset: str, table: str, schema: list[dict]) -> None:
        """Register a table schema.

        Args:
            dataset: Dataset name.
            table: Table name.
            schema: List of column dicts with keys: name, type, mode, description.
        """
        self._schemas[f"{dataset}.{table}"] = schema

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Return pre-registered result for the query."""
        self.executed_queries.append(sql)
        if sql not in self._results:
            raise KeyError(
                f"FakeBigQueryClient: No result registered for query: {sql[:100]}..."
            )
        return self._results[sql]

    def dry_run_query(self, sql: str) -> dict:
        """Return pre-registered dry run response, or default valid."""
        if sql in self._dry_run_responses:
            return self._dry_run_responses[sql]
        return {"valid": True, "total_bytes_processed": 1024, "error": None}

    def get_table_schema(self, dataset: str, table: str) -> list[dict]:
        """Return pre-registered schema."""
        key = f"{dataset}.{table}"
        if key not in self._schemas:
            raise KeyError(f"FakeBigQueryClient: No schema registered for {key}")
        return self._schemas[key]


class FakeEmbeddingClient:
    """Fake embedding client that returns deterministic dummy vectors.

    Returns a fixed-length vector of 0.1 for every input.
    Useful for testing vector search pipeline without Vertex AI calls.
    """

    def __init__(self, dimension: int = 768):
        self._dimension = dimension

    def embed_text(self, text: str) -> list[float]:
        """Return a dummy embedding vector."""
        return [0.1] * self._dimension

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Return dummy embeddings for a batch."""
        return [[0.1] * self._dimension for _ in texts]
```

**IMPORTANT**: `FakeBigQueryClient.executed_queries` is a list that records every query passed to `execute_query`. This lets tests assert not just the result, but WHAT was queried:

```python
def test_something():
    client = FakeBigQueryClient()
    client.add_result("SELECT 1", pd.DataFrame({"x": [1]}))

    my_function(client)  # calls client.execute_query("SELECT 1") internally

    assert len(client.executed_queries) == 1
    assert "SELECT 1" in client.executed_queries[0]
```

---

### 12. Agent Package `__init__.py`

**Path**: `nl2sql_agent/__init__.py`

```python
from . import agent  # noqa: F401 — required by ADK for agent discovery
```

This file MUST contain exactly this line. ADK's CLI discovers agents by importing the package and looking for `agent.root_agent`.

**DO NOT** add other imports here. Keep it minimal.

**DO NOT** remove this file or leave it empty. ADK will not find your agent.

---

### 13. Agent Definition

**Path**: `nl2sql_agent/agent.py`

```python
"""ADK agent definitions: root agent and NL2SQL sub-agent.

The root_agent variable is REQUIRED by ADK convention.
ADK discovers it automatically when running `adk run nl2sql_agent`.
"""

import os

from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import setup_logging, get_logger

# --- Initialise logging ---
setup_logging()
logger = get_logger(__name__)

# --- Configure LiteLLM environment ---
# LiteLLM reads these environment variables directly.
# We set them here from our pydantic settings to ensure they're available.
os.environ["LITELLM_API_KEY"] = settings.litellm_api_key
os.environ["LITELLM_API_BASE"] = settings.litellm_api_base

# --- Model instances ---
default_model = LiteLlm(model=settings.litellm_model)

# --- NL2SQL Sub-Agent ---
nl2sql_agent = LlmAgent(
    name="nl2sql_agent",
    model=default_model,
    description=(
        "Answers questions about Mako trading data by querying BigQuery. "
        "Handles theo/vol/delta analysis, KPI/PnL queries, quoter activity, "
        "broker performance, edge/slippage analysis across all trading desks. "
        "Routes to the correct table based on question context."
    ),
    instruction=(
        "You are a SQL expert for Mako Group, an options market-making firm. "
        "Your job is to answer natural language questions about trading data. "
        "For now, you have no tools — just acknowledge the question and explain "
        "that you will be able to query BigQuery once tools are connected. "
        "Mention which tables might be relevant based on the question. "
        "KPI tables (gold layer) are in nl2sql_omx_kpi dataset. "
        "Raw data tables (silver layer) are in nl2sql_omx_data dataset."
    ),
    # tools=[] — no tools in Track 01. Added in Track 03.
)

# --- Root Agent ---
# IMPORTANT: This variable MUST be named `root_agent`. ADK looks for this exact name.
root_agent = LlmAgent(
    name="mako_assistant",
    model=default_model,
    description="Mako Group trading assistant.",
    instruction=(
        "You are a helpful assistant for Mako Group traders. "
        "For any questions about trading data, performance, KPIs, "
        "theo/vol analysis, quoter activity, edge, slippage, PnL, "
        "or anything that requires querying a database, delegate to nl2sql_agent. "
        "For general questions, greetings, or clarifications, answer directly. "
        "If the trader's question is ambiguous, ask a clarifying question."
    ),
    sub_agents=[nl2sql_agent],
)

logger.info(
    "agents_initialised",
    root_agent=root_agent.name,
    sub_agents=[a.name for a in root_agent.sub_agents],
    model=settings.litellm_model,
)
```

**CRITICAL DETAILS**:

1. The variable MUST be called `root_agent`. NOT `agent`, NOT `app`, NOT `mako_assistant`.
2. `sub_agents=[nl2sql_agent]` — this is how delegation works. The root agent's LLM reads the `description` of each sub-agent and decides when to delegate via `transfer_to_agent`.
3. `os.environ` for LiteLLM keys: LiteLLM reads `LITELLM_API_KEY` and `LITELLM_API_BASE` from environment variables directly. We bridge our pydantic settings into `os.environ` so LiteLLM can find them even when running outside Docker.
4. The nl2sql_agent has a placeholder instruction. It has NO tools yet. Tools are added in Track 03.

**DO NOT** name the variable anything other than `root_agent`.

**DO NOT** create the LiteLlm instance inside the LlmAgent constructor. Create it as a separate variable so it can be reused and tested.

**DO NOT** import tools that don't exist yet. Track 01 has zero tools.

---

### 14. Dev Dataset SQL Scripts

**The datasets and tables already exist.** The SQL below was already run. These scripts are kept in the repo for documentation only — DO NOT run them again.

#### 14a. Dataset Creation (ALREADY DONE)

**Path**: `setup/01_create_datasets.sql`

```sql
-- ALREADY EXECUTED — DO NOT RUN AGAIN
-- Kept for documentation only.
--
-- bq mk --location=europe-west2 cloud-data-n-base-d4b3:nl2sql_omx_kpi
-- bq mk --location=europe-west2 cloud-data-n-base-d4b3:nl2sql_omx_data
```

#### 14b. Table Inventory (ALREADY POPULATED)

**Path**: `setup/02_table_inventory.sql`

```sql
-- ALREADY EXECUTED — DO NOT RUN AGAIN
-- Kept for documentation: shows what tables exist and where they came from.
--
-- ============================================================
-- nl2sql_omx_kpi — GOLD LAYER (KPI-enriched trade data)
-- Partition: trade_date
-- Data: 2026-02-17 only
-- ============================================================
-- cloud-data-n-base-d4b3.nl2sql_omx_kpi.brokertrade   (EMPTY — no data for this date)
-- cloud-data-n-base-d4b3.nl2sql_omx_kpi.clicktrade
-- cloud-data-n-base-d4b3.nl2sql_omx_kpi.markettrade
-- cloud-data-n-base-d4b3.nl2sql_omx_kpi.otoswing
-- cloud-data-n-base-d4b3.nl2sql_omx_kpi.quotertrade
--
-- ============================================================
-- nl2sql_omx_data — SILVER LAYER (raw trade/market data)
-- Partition: trade_date
-- Cluster: varies (see below)
-- Data: 2026-02-17 only
-- ============================================================
-- cloud-data-n-base-d4b3.nl2sql_omx_data.brokertrade    CLUSTER BY portfolio, symbol, term, instrument_hash
-- cloud-data-n-base-d4b3.nl2sql_omx_data.clicktrade     CLUSTER BY portfolio, symbol, term, instrument_hash
-- cloud-data-n-base-d4b3.nl2sql_omx_data.markettrade    CLUSTER BY portfolio, symbol, term, instrument_hash
-- cloud-data-n-base-d4b3.nl2sql_omx_data.swingdata      (no clustering)
-- cloud-data-n-base-d4b3.nl2sql_omx_data.quotertrade    CLUSTER BY portfolio, symbol, term, instrument_hash
-- cloud-data-n-base-d4b3.nl2sql_omx_data.theodata       CLUSTER BY portfolio, symbol, term, instrument_hash
-- cloud-data-n-base-d4b3.nl2sql_omx_data.marketdata     CLUSTER BY symbol, term, instrument_hash
-- cloud-data-n-base-d4b3.nl2sql_omx_data.marketdepth    CLUSTER BY symbol, term, instrument_hash
```

#### 14c. Verify Data

**Path**: `setup/03_verify_data.sql`

This script SHOULD be run to verify row counts and inspect schemas. It is read-only.

```sql
-- ============================================================
-- Row counts — KPI tables
-- ============================================================
SELECT 'kpi.brokertrade' AS table_name, COUNT(*) AS row_count
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.brokertrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'kpi.clicktrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.clicktrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'kpi.markettrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.markettrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'kpi.otoswing', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.otoswing`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'kpi.quotertrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.quotertrade`
WHERE trade_date = '2026-02-17'
ORDER BY table_name;

-- ============================================================
-- Row counts — DATA tables
-- ============================================================
SELECT 'data.brokertrade' AS table_name, COUNT(*) AS row_count
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.brokertrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.clicktrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.clicktrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.markettrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.markettrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.swingdata', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.swingdata`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.quotertrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.quotertrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.theodata', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.theodata`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.marketdata', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.marketdata`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.marketdepth', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.marketdepth`
WHERE trade_date = '2026-02-17'
ORDER BY table_name;

-- ============================================================
-- Schema inspection — KPI markettrade (check column names)
-- ============================================================
SELECT column_name, data_type, is_nullable
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'markettrade'
ORDER BY ordinal_position;

-- Schema inspection — KPI brokertrade (must have account field)
SELECT column_name, data_type, is_nullable
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'brokertrade'
ORDER BY ordinal_position;

-- Schema inspection — DATA theodata
SELECT column_name, data_type, is_nullable
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'theodata'
ORDER BY ordinal_position;

-- Schema inspection — DATA quotertrade (raw activity)
SELECT column_name, data_type, is_nullable
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'quotertrade'
ORDER BY ordinal_position;

-- Sample distinct values for routing-critical columns
SELECT DISTINCT delta_bucket FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.markettrade` WHERE trade_date = '2026-02-17';
SELECT DISTINCT account FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.brokertrade` WHERE trade_date = '2026-02-17';
```

**Action after running:** Record the actual column names from INFORMATION_SCHEMA. Column names may differ from our assumptions. Everything downstream depends on getting real column names right.

---

### 15. Schema Extraction Script

**Path**: `setup/extract_schemas.py`

```python
"""Extract BigQuery schemas from nl2sql_omx_kpi and nl2sql_omx_data tables.

Usage:
    python setup/extract_schemas.py

Output:
    schemas/kpi/brokertrade.json
    schemas/kpi/clicktrade.json
    schemas/kpi/markettrade.json
    schemas/kpi/otoswing.json
    schemas/kpi/quotertrade.json
    schemas/data/brokertrade.json
    schemas/data/clicktrade.json
    schemas/data/markettrade.json
    schemas/data/swingdata.json
    schemas/data/quotertrade.json
    schemas/data/theodata.json
    schemas/data/marketdata.json
    schemas/data/marketdepth.json

Each JSON file contains an array of objects with: name, type, mode, description.
"""

import json
from pathlib import Path

from google.cloud import bigquery

# --- Configuration ---
PROJECT = "cloud-data-n-base-d4b3"

DATASETS = {
    "nl2sql_omx_kpi": [
        "brokertrade",
        "clicktrade",
        "markettrade",
        "otoswing",
        "quotertrade",
    ],
    "nl2sql_omx_data": [
        "brokertrade",
        "clicktrade",
        "markettrade",
        "swingdata",
        "quotertrade",
        "theodata",
        "marketdata",
        "marketdepth",
    ],
}

OUTPUT_DIR = Path("schemas")


def extract_schema(client: bigquery.Client, dataset: str, table_name: str) -> list[dict]:
    """Extract schema from a BigQuery table.

    Args:
        client: BigQuery client instance.
        dataset: BigQuery dataset name.
        table_name: Name of the table.

    Returns:
        List of column dicts with keys: name, type, mode, description.
    """
    table_ref = f"{PROJECT}.{dataset}.{table_name}"
    table = client.get_table(table_ref)

    schema = [
        {
            "name": field.name,
            "type": field.field_type,
            "mode": field.mode,
            "description": field.description or "",
        }
        for field in table.schema
    ]

    return schema


def main() -> None:
    client = bigquery.Client(project=PROJECT, location="europe-west2")

    for dataset, tables in DATASETS.items():
        # Create output subdirectory: schemas/kpi/ or schemas/data/
        short_name = "kpi" if "kpi" in dataset else "data"
        output_subdir = OUTPUT_DIR / short_name
        output_subdir.mkdir(parents=True, exist_ok=True)

        for table_name in tables:
            schema = extract_schema(client, dataset, table_name)

            output_path = output_subdir / f"{table_name}.json"
            with open(output_path, "w") as f:
                json.dump(schema, f, indent=2)

            print(f"✓ {dataset}.{table_name}: {len(schema)} columns → {output_path}")


if __name__ == "__main__":
    main()
```

**IMPORTANT**: This script is run manually by the developer, NOT by the agent. It produces JSON files that are used in Track 02 to generate YAML metadata.

**DO NOT** use `bq` CLI for schema extraction. The Python client gives structured output.

**DO NOT** store extracted schemas in Git (they're in `.gitignore`). They are regeneratable artifacts.

---

## Complete Directory Tree

After Track 01 is complete, the repo MUST look exactly like this:

```
nl2sql-agent/                       ← project root (Git repo root)
├── .gitignore
├── pyproject.toml
├── Dockerfile
├── docker-compose.yml
├── README.md
│
├── nl2sql_agent/                   ← ADK agent package
│   ├── __init__.py                 ← contains: from . import agent
│   ├── agent.py                    ← defines: root_agent (LlmAgent)
│   ├── config.py                   ← pydantic Settings class
│   ├── logging_config.py           ← structlog JSON setup
│   ├── protocols.py                ← Protocol interfaces for all external deps
│   ├── clients.py                  ← Concrete implementations (LiveBigQueryClient, etc.)
│   ├── .env                        ← actual secrets (NOT in Git)
│   └── .env.example                ← template (IN Git)
│
├── setup/                          ← one-time infrastructure scripts
│   ├── 01_create_datasets.sql      ← ALREADY RUN — documentation only
│   ├── 02_table_inventory.sql      ← ALREADY RUN — documentation only
│   ├── 03_verify_data.sql          ← Run this to check row counts
│   └── extract_schemas.py
│
├── schemas/                        ← extracted schemas (NOT in Git)
│   ├── kpi/
│   │   ├── brokertrade.json
│   │   ├── clicktrade.json
│   │   ├── markettrade.json
│   │   ├── otoswing.json
│   │   └── quotertrade.json
│   └── data/
│       ├── brokertrade.json
│       ├── clicktrade.json
│       ├── markettrade.json
│       ├── swingdata.json
│       ├── quotertrade.json
│       ├── theodata.json
│       ├── marketdata.json
│       └── marketdepth.json
│
├── catalog/                        ← YAML metadata (Track 02, empty for now)
│   ├── kpi/                        ← Gold layer table metadata
│   │   └── .gitkeep
│   ├── data/                       ← Silver layer table metadata
│   │   └── .gitkeep
│   └── .gitkeep
│
├── examples/                       ← Validated Q→SQL pairs (Track 02, empty for now)
│   └── .gitkeep
│
├── embeddings/                     ← Embedding SQL scripts (Track 02, empty for now)
│   └── .gitkeep
│
├── eval/                           ← Eval framework (Track 05, empty for now)
│   └── .gitkeep
│
└── tests/                          ← Test suite
    ├── __init__.py
    ├── conftest.py
    ├── fakes.py                    ← Fake implementations for unit tests
    ├── test_config.py
    ├── test_agent_init.py
    └── test_protocols.py           ← Verify fakes satisfy protocols
```

**DO NOT** create directories that are not listed above.

**DO NOT** create empty `__init__.py` files in `setup/`, `schemas/`, `catalog/`, `examples/`, `embeddings/`, or `eval/`. These are NOT Python packages.

**DO** create `.gitkeep` in empty directories so Git tracks them.

---

## Test Specifications

### Test Configuration

**Path**: `tests/conftest.py`

```python
"""Shared test fixtures for the nl2sql-agent test suite."""

import os

import pytest


@pytest.fixture(autouse=True)
def set_test_env(monkeypatch):
    """Set required environment variables for all tests.

    This ensures tests don't depend on the real .env file.
    Every env var that pydantic Settings requires MUST be set here.
    """
    monkeypatch.setenv("LITELLM_API_KEY", "test-key-not-real")
    monkeypatch.setenv("LITELLM_API_BASE", "http://localhost:4000")
    monkeypatch.setenv("LITELLM_MODEL", "gemini-3-flash-preview")
    monkeypatch.setenv("LITELLM_MODEL_COMPLEX", "gemini-3-pro-preview")
    monkeypatch.setenv("GCP_PROJECT", "cloud-data-n-base-d4b3")
    monkeypatch.setenv("BQ_LOCATION", "europe-west2")
    monkeypatch.setenv("KPI_DATASET", "nl2sql_omx_kpi")
    monkeypatch.setenv("DATA_DATASET", "nl2sql_omx_data")
    monkeypatch.setenv("METADATA_DATASET", "nl2sql_metadata")
    monkeypatch.setenv("ADK_SUPPRESS_GEMINI_LITELLM_WARNINGS", "true")
```

**IMPORTANT**: The `autouse=True` fixture runs for EVERY test automatically. This ensures no test accidentally reads the real `.env` file.

---

### Test: Configuration Loading

**Path**: `tests/test_config.py`

```python
"""Tests for configuration loading via pydantic-settings."""

from nl2sql_agent.config import Settings


class TestSettings:
    """Test that Settings loads correctly from environment variables."""

    def test_settings_loads_from_env(self):
        """Settings should load all required fields from environment."""
        s = Settings()
        assert s.litellm_api_key == "test-key-not-real"
        assert s.litellm_api_base == "http://localhost:4000"
        assert s.gcp_project == "cloud-data-n-base-d4b3"
        assert s.bq_location == "europe-west2"
        assert s.kpi_dataset == "nl2sql_omx_kpi"

    def test_settings_defaults(self):
        """Settings should have correct default values."""
        s = Settings()
        assert s.litellm_model == "gemini-3-flash-preview"
        assert s.litellm_model_complex == "gemini-3-pro-preview"
        assert s.bq_location == "europe-west2"
        assert s.kpi_dataset == "nl2sql_omx_kpi"
        assert s.data_dataset == "nl2sql_omx_data"
        assert s.metadata_dataset == "nl2sql_metadata"
        assert s.embedding_model == "text-embedding-005"

    def test_settings_missing_required_field_raises(self, monkeypatch):
        """Settings should raise ValidationError if required field is missing."""
        monkeypatch.delenv("LITELLM_API_KEY", raising=False)
        from pydantic import ValidationError
        import pytest

        with pytest.raises(ValidationError):
            Settings()

    def test_settings_ignores_extra_env_vars(self, monkeypatch):
        """Settings should not crash on unknown env vars."""
        monkeypatch.setenv("SOME_RANDOM_THING", "foobar")
        s = Settings()  # Should not raise
        assert s.gcp_project == "cloud-data-n-base-d4b3"
```

---

### Test: Agent Initialisation

**Path**: `tests/test_agent_init.py`

```python
"""Tests for ADK agent initialisation and delegation structure."""

from google.adk.agents import LlmAgent


class TestAgentStructure:
    """Test that agents are correctly configured."""

    def test_root_agent_exists_and_is_llm_agent(self):
        """The root_agent must exist and be an LlmAgent instance."""
        from nl2sql_agent.agent import root_agent

        assert isinstance(root_agent, LlmAgent)
        assert root_agent.name == "mako_assistant"

    def test_root_agent_has_sub_agents(self):
        """Root agent must have exactly one sub-agent: nl2sql_agent."""
        from nl2sql_agent.agent import root_agent

        assert len(root_agent.sub_agents) == 1
        assert root_agent.sub_agents[0].name == "nl2sql_agent"

    def test_nl2sql_agent_is_llm_agent(self):
        """The NL2SQL sub-agent must be an LlmAgent instance."""
        from nl2sql_agent.agent import nl2sql_agent

        assert isinstance(nl2sql_agent, LlmAgent)
        assert nl2sql_agent.name == "nl2sql_agent"

    def test_nl2sql_agent_has_description(self):
        """NL2SQL agent description must mention key domains."""
        from nl2sql_agent.agent import nl2sql_agent

        desc = nl2sql_agent.description.lower()
        assert "trading data" in desc
        assert "bigquery" in desc

    def test_nl2sql_agent_has_no_tools_in_track_01(self):
        """In Track 01, nl2sql_agent must have zero tools."""
        from nl2sql_agent.agent import nl2sql_agent

        assert nl2sql_agent.tools is None or len(nl2sql_agent.tools) == 0

    def test_root_agent_instruction_mentions_delegation(self):
        """Root agent instruction must tell it to delegate data questions."""
        from nl2sql_agent.agent import root_agent

        instruction = root_agent.instruction.lower()
        assert "delegate" in instruction or "nl2sql_agent" in instruction

    def test_agent_names_are_valid_python_identifiers(self):
        """All agent names must be valid Python identifiers (ADK requirement)."""
        from nl2sql_agent.agent import root_agent, nl2sql_agent

        assert root_agent.name.isidentifier()
        assert nl2sql_agent.name.isidentifier()

    def test_agent_names_are_not_reserved(self):
        """No agent can be named 'user' — reserved by ADK."""
        from nl2sql_agent.agent import root_agent, nl2sql_agent

        assert root_agent.name != "user"
        assert nl2sql_agent.name != "user"
```

---

### Test: Protocols and Fakes

**Path**: `tests/test_protocols.py`

```python
"""Tests that verify fake implementations satisfy their protocols,
and that the protocol pattern works correctly."""

import pandas as pd

from nl2sql_agent.protocols import BigQueryProtocol, EmbeddingProtocol
from tests.fakes import FakeBigQueryClient, FakeEmbeddingClient


class TestFakeBigQueryClient:
    """Verify FakeBigQueryClient satisfies BigQueryProtocol."""

    def test_satisfies_protocol(self):
        """FakeBigQueryClient must be a valid BigQueryProtocol implementation."""
        client = FakeBigQueryClient()
        assert isinstance(client, BigQueryProtocol)

    def test_execute_query_returns_registered_result(self):
        """execute_query should return the DataFrame registered via add_result."""
        client = FakeBigQueryClient()
        expected = pd.DataFrame({"col": [1, 2, 3]})
        client.add_result("SELECT col FROM t", expected)

        result = client.execute_query("SELECT col FROM t")
        pd.testing.assert_frame_equal(result, expected)

    def test_execute_query_tracks_calls(self):
        """executed_queries list should track all queries passed to execute_query."""
        client = FakeBigQueryClient()
        client.add_result("Q1", pd.DataFrame())
        client.add_result("Q2", pd.DataFrame())

        client.execute_query("Q1")
        client.execute_query("Q2")

        assert client.executed_queries == ["Q1", "Q2"]

    def test_execute_query_raises_on_unregistered(self):
        """execute_query should raise KeyError for queries not registered."""
        import pytest

        client = FakeBigQueryClient()
        with pytest.raises(KeyError, match="No result registered"):
            client.execute_query("SELECT * FROM unknown")

    def test_dry_run_returns_valid_by_default(self):
        """dry_run_query should return valid=True by default."""
        client = FakeBigQueryClient()
        result = client.dry_run_query("SELECT 1")
        assert result["valid"] is True
        assert result["error"] is None

    def test_get_table_schema_returns_registered_schema(self):
        """get_table_schema should return the schema registered via add_schema."""
        client = FakeBigQueryClient()
        schema = [{"name": "id", "type": "INT64", "mode": "REQUIRED", "description": ""}]
        client.add_schema("nl2sql_omx_data", "theodata", schema)

        result = client.get_table_schema("nl2sql_omx_data", "theodata")
        assert result == schema


class TestFakeEmbeddingClient:
    """Verify FakeEmbeddingClient satisfies EmbeddingProtocol."""

    def test_satisfies_protocol(self):
        """FakeEmbeddingClient must be a valid EmbeddingProtocol implementation."""
        client = FakeEmbeddingClient()
        assert isinstance(client, EmbeddingProtocol)

    def test_embed_text_returns_correct_dimension(self):
        """embed_text should return a vector of the configured dimension."""
        client = FakeEmbeddingClient(dimension=768)
        result = client.embed_text("hello")
        assert len(result) == 768
        assert all(isinstance(x, float) for x in result)

    def test_embed_batch_returns_one_vector_per_input(self):
        """embed_batch should return one vector per input text."""
        client = FakeEmbeddingClient(dimension=256)
        result = client.embed_batch(["a", "b", "c"])
        assert len(result) == 3
        assert all(len(v) == 256 for v in result)
```

---

### Running Tests

```bash
# Inside Docker container:
pytest tests/ -v

# Expected output:
# tests/test_config.py::TestSettings::test_settings_loads_from_env PASSED
# tests/test_config.py::TestSettings::test_settings_defaults PASSED
# tests/test_config.py::TestSettings::test_settings_missing_required_field_raises PASSED
# tests/test_config.py::TestSettings::test_settings_ignores_extra_env_vars PASSED
# tests/test_agent_init.py::TestAgentStructure::test_root_agent_exists_and_is_llm_agent PASSED
# tests/test_agent_init.py::TestAgentStructure::test_root_agent_has_sub_agents PASSED
# tests/test_agent_init.py::TestAgentStructure::test_nl2sql_agent_is_llm_agent PASSED
# tests/test_agent_init.py::TestAgentStructure::test_nl2sql_agent_has_description PASSED
# tests/test_agent_init.py::TestAgentStructure::test_nl2sql_agent_has_no_tools_in_track_01 PASSED
# tests/test_agent_init.py::TestAgentStructure::test_root_agent_instruction_mentions_delegation PASSED
# tests/test_agent_init.py::TestAgentStructure::test_agent_names_are_valid_python_identifiers PASSED
# tests/test_agent_init.py::TestAgentStructure::test_agent_names_are_not_reserved PASSED
# tests/test_protocols.py::TestFakeBigQueryClient::test_satisfies_protocol PASSED
# tests/test_protocols.py::TestFakeBigQueryClient::test_execute_query_returns_registered_result PASSED
# tests/test_protocols.py::TestFakeBigQueryClient::test_execute_query_tracks_calls PASSED
# tests/test_protocols.py::TestFakeBigQueryClient::test_execute_query_raises_on_unregistered PASSED
# tests/test_protocols.py::TestFakeBigQueryClient::test_dry_run_returns_valid_by_default PASSED
# tests/test_protocols.py::TestFakeBigQueryClient::test_get_table_schema_returns_registered_schema PASSED
# tests/test_protocols.py::TestFakeEmbeddingClient::test_satisfies_protocol PASSED
# tests/test_protocols.py::TestFakeEmbeddingClient::test_embed_text_returns_correct_dimension PASSED
# tests/test_protocols.py::TestFakeEmbeddingClient::test_embed_batch_returns_one_vector_per_input PASSED
#
# 21 passed
```

ALL 21 tests must pass. If any fail, the track is not complete.

---

## Implementation Order

Execute these steps in EXACTLY this order. Do not skip steps. Do not reorder.

### Step 1: Create directory structure
Create all directories and empty files as shown in the directory tree above. Use `mkdir -p` for nested directories. Create `.gitkeep` files in empty directories.

### Step 2: Write `pyproject.toml`
Copy exactly as specified above.

### Step 3: Write `Dockerfile` and `docker-compose.yml`
Copy exactly as specified above.

### Step 4: Write `.gitignore`
Copy exactly as specified above.

### Step 5: Write `.env.example` and `.env`
Copy exactly as specified above. Replace placeholder values in `.env` with actual values.

### Step 6: Write `nl2sql_agent/config.py`
Copy exactly as specified above.

### Step 7: Write `nl2sql_agent/logging_config.py`
Copy exactly as specified above.

### Step 8: Write `nl2sql_agent/protocols.py`
Copy exactly as specified above. Defines `BigQueryProtocol` and `EmbeddingProtocol`.

### Step 9: Write `nl2sql_agent/clients.py`
Copy exactly as specified above. Defines `LiveBigQueryClient`.

### Step 10: Write `nl2sql_agent/__init__.py`
One line: `from . import agent`

### Step 11: Write `nl2sql_agent/agent.py`
Copy exactly as specified above. This creates root_agent and nl2sql_agent.

### Step 12: Write test files
Create `tests/__init__.py` (empty), `tests/fakes.py`, `tests/conftest.py`, `tests/test_config.py`, `tests/test_agent_init.py`, `tests/test_protocols.py` exactly as specified above.

### Step 13: Build Docker container and run tests
```bash
docker compose build
docker compose run --rm agent pytest tests/ -v
```
ALL 21 tests must pass.

### Step 14: Test delegation manually
```bash
docker compose run --rm agent
```
This starts `adk run nl2sql_agent`. Test these interactions:

| Input | Expected Behaviour |
|-------|-------------------|
| "Hello" | Root agent responds directly with a greeting |
| "What can you help me with?" | Root agent explains it can help with trading data |
| "What was the edge on strike 98?" | Root agent delegates to nl2sql_agent (you should see `transfer_to_agent` in output) |
| "Show me PnL by delta bucket" | Root agent delegates to nl2sql_agent |
| "What is the capital of France?" | Root agent answers directly (not a data question) |

### Step 15: Write SQL setup scripts
Create `setup/01_create_datasets.sql` (documentation only — already run), `setup/02_table_inventory.sql` (documentation only — already run), `setup/03_verify_data.sql` (run this to verify) exactly as specified above.

### Step 16: Write schema extraction script
Create `setup/extract_schemas.py` exactly as specified above.

### Step 17: Run verification and extract schemas
Run `03_verify_data.sql` in BigQuery console. Check all tables have rows (kpi.brokertrade may be 0, that's expected). Then run:
```bash
docker compose run --rm agent python setup/extract_schemas.py
```
Verify 13 JSON files appear in `schemas/kpi/` (5 files) and `schemas/data/` (8 files).

### Step 18: Write `README.md`
Create a brief README documenting:
- What this project is (NL2SQL agent for Mako trading data)
- How to set up (copy `.env.example` to `.env`, fill in values, `docker compose build`)
- How to run tests (`docker compose run --rm agent pytest tests/ -v`)
- How to run the agent (`docker compose run --rm agent`)

---

## Acceptance Criteria

Track 01 is DONE when ALL of the following are true:

- [ ] `docker compose build` succeeds with no errors
- [ ] `docker compose run --rm agent pytest tests/ -v` shows 21/21 tests passing
- [ ] `docker compose run --rm agent` starts `adk run` and allows interactive chat
- [ ] Greeting messages are handled by root agent (no delegation)
- [ ] Data questions trigger delegation to nl2sql_agent
- [ ] `nl2sql_agent/.env` exists with all required values
- [ ] `nl2sql_agent/.env.example` exists and is committed to Git
- [ ] `setup/*.sql` files exist with correct SQL
- [ ] `setup/extract_schemas.py` runs and produces 13 JSON files (5 in `schemas/kpi/`, 8 in `schemas/data/`)
- [ ] KPI tables in BigQuery have row_count > 0 (except kpi.brokertrade which may be empty)
- [ ] DATA tables in BigQuery have row_count > 0
- [ ] `pyproject.toml` has all required dependencies
- [ ] `.gitignore` excludes `.env`, `__pycache__`, `schemas/*.json`
- [ ] `nl2sql_agent/protocols.py` defines `BigQueryProtocol` and `EmbeddingProtocol`
- [ ] `nl2sql_agent/clients.py` defines `LiveBigQueryClient` implementing `BigQueryProtocol`
- [ ] `tests/fakes.py` defines `FakeBigQueryClient` and `FakeEmbeddingClient`
- [ ] `isinstance(FakeBigQueryClient(), BigQueryProtocol)` returns `True`
- [ ] `isinstance(FakeEmbeddingClient(), EmbeddingProtocol)` returns `True`

---

## Anti-Patterns (DO NOT DO THESE)

| Anti-Pattern | Why It's Wrong | Correct Approach |
|---|---|---|
| Name the agent variable `agent` or `app` | ADK only discovers `root_agent` | Use `root_agent = LlmAgent(...)` |
| Use `model="gemini-3-flash-preview"` (bare string) | This uses native Gemini, not LiteLLM | Use `model=LiteLlm(model="gemini-3-flash-preview")` |
| Put `.env` at project root | ADK looks for `.env` inside the agent package | Put `.env` inside `nl2sql_agent/` |
| Use `os.getenv()` for config | No validation, no type safety | Use `from nl2sql_agent.config import settings` |
| Add tools to nl2sql_agent in Track 01 | Tools are Track 03 scope | Leave `tools=[]` or omit |
| Import `from google.adk.models.lite_llm import LiteLLM` | Wrong case | Use `LiteLlm` (camelCase) |
| Create `src/` directory | ADK expects flat package at project root | Put `nl2sql_agent/` at project root |
| Use `docker compose up` for interactive mode | No stdin in detached mode | Use `docker compose run --rm agent` |
| Add `google-genai` to dependencies | Conflicts with LiteLLM approach | LiteLLM handles model access |
| Leave `__init__.py` empty | ADK won't discover the agent | Must contain `from . import agent` |
| Name an agent `"user"` | Reserved by ADK | Pick any other valid identifier |
| Import `bigquery.Client` directly in tool functions | Untestable, tightly coupled | Accept `BigQueryProtocol` parameter, inject `LiveBigQueryClient` or `FakeBigQueryClient` |
| Use `abc.ABC` / `abc.abstractmethod` for interfaces | Requires explicit inheritance | Use `typing.Protocol` — structural (duck) typing, no inheritance needed |
| Put fake/mock implementations in production code | Test code leaks into prod | Fakes live in `tests/fakes.py`, never imported in `nl2sql_agent/` |
