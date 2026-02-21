"""Shared TypedDict definitions for all agent tool return types.

These provide compile-time type safety and IDE autocompletion for tool
return values. All tools return dicts matching one of these shapes.
"""

from __future__ import annotations

from typing import TypedDict

# --- Shared ---


class ErrorResult(TypedDict):
    """Common error return for all tools."""

    status: str  # "error"
    error_message: str


# --- Semantic Cache ---


class CacheHitResult(TypedDict):
    """Returned when a near-exact match is found in query_memory."""

    cache_hit: bool  # True
    cached_sql: str
    cached_question: str
    cached_dataset: str
    tables_used: list[str]
    distance: float


class CacheMissResult(TypedDict):
    """Returned when no matching cache entry is found."""

    cache_hit: bool  # False
    reason: str


# --- Exchange Resolver ---


class ExchangeResolvedResult(TypedDict):
    """Returned when exchange is resolved (alias or symbol match)."""

    status: str  # "resolved" or "default"
    exchange: str
    kpi_dataset: str
    data_dataset: str


class ExchangeMultipleResult(TypedDict):
    """Returned when a symbol exists on multiple exchanges."""

    status: str  # "multiple"
    message: str
    matches: list[dict]


# --- Vector Search ---


class VectorSearchResult(TypedDict):
    """Returned by vector_search_tables."""

    status: str
    results: list[dict]


class ColumnSearchResult(TypedDict):
    """Returned by vector_search_columns."""

    status: str
    tables: list[dict]
    examples: list[dict]


class FewShotResult(TypedDict):
    """Returned by fetch_few_shot_examples."""

    status: str
    examples: list[dict]


# --- Metadata ---


class MetadataSuccessResult(TypedDict):
    """Returned by load_yaml_metadata on success."""

    status: str  # "success"
    table_name: str
    dataset_name: str
    metadata: str


# --- SQL Validation ---


class DryRunValidResult(TypedDict):
    """Returned when SQL passes dry-run validation."""

    status: str  # "valid"
    estimated_bytes: int
    estimated_mb: float


class DryRunInvalidResult(TypedDict):
    """Returned when SQL fails dry-run validation."""

    status: str  # "invalid"
    error_message: str


# --- SQL Execution ---


class ExecuteSuccessResult(TypedDict, total=False):
    """Returned on successful SQL execution.

    `warning` is optional (only present when rows are truncated).
    """

    status: str  # "success"
    row_count: int
    rows: list[dict]
    warning: str


# --- Learning Loop ---


class SaveQueryResult(TypedDict):
    """Returned by save_validated_query."""

    status: str  # "success", "partial_success", or "error"
    message: str
