"""Application configuration loaded from environment variables via pydantic-settings.

Usage:
    from nl2sql_agent.config import settings
    print(settings.gcp_project)
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Resolve .env path relative to this file (nl2sql_agent/.env), not CWD.
# This allows scripts/ and tests/ to load settings from anywhere.
_ENV_FILE = Path(__file__).parent / ".env"


class Settings(BaseSettings):
    """All application settings. Loaded from environment variables and .env file.

    Environment variables are case-insensitive. For example, GCP_PROJECT or
    gcp_project will both work.
    """

    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore extra env vars not defined here
    )

    # --- LiteLLM ---
    litellm_api_key: str = Field(description="LiteLLM proxy API key")
    litellm_api_base: str = Field(description="LiteLLM proxy base URL")
    litellm_model: str = Field(
        default="openai/gemini-3-flash-preview",
        description="LLM model string. Must include provider prefix (e.g. openai/) for LiteLLM proxy.",
    )
    litellm_model_complex: str = Field(
        default="openai/gemini-3-pro-preview",
        description="Complex query LLM model string. Must include provider prefix (e.g. openai/) for LiteLLM proxy.",
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

    # --- Query Limits (Track 03) ---
    bq_query_timeout_seconds: float = Field(
        default=30.0,
        description="Timeout in seconds for BigQuery query execution",
    )
    bq_max_result_rows: int = Field(
        default=1000,
        description="Maximum rows returned by execute_sql tool",
    )
    vector_search_top_k: int = Field(
        default=5,
        description="Number of results for vector search queries",
    )

    # --- Semantic Cache (Track 05) ---
    semantic_cache_threshold: float = Field(
        default=0.05,
        description=(
            "Maximum COSINE distance for a semantic cache hit. "
            "0.05 ≈ 0.95 similarity — very tight, only near-exact paraphrases."
        ),
    )


# Singleton instance — import this everywhere
settings = Settings()