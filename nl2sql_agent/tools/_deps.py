"""Shared dependency injection for all tool modules.

This module holds the BigQuery service instance that all tools share.
It is initialised once at agent startup via init_bq_service().

THREAD SAFETY: This module uses mutable module-level globals
(_bq_service, _vector_cache_*). It is NOT thread-safe.
Current usage (single ADK session per process) is safe.
If concurrent request handling is needed (e.g., async MCP server),
these globals must be replaced with thread-local storage or
a per-request context object.

Usage (inside tool modules):
    from nl2sql_agent.tools._deps import get_bq_service
"""

from typing import Any

from nl2sql_agent.logging_config import get_logger

logger = get_logger(__name__)

_bq_service: Any = None


def init_bq_service(service) -> None:
    """Set the shared BigQuery service for all tools.

    Args:
        service: An object implementing BigQueryProtocol.
    """
    global _bq_service
    _bq_service = service
    logger.info("tools_bq_service_initialised", service_type=type(service).__name__)


def get_bq_service() -> Any:
    """Get the shared BigQuery service.

    Raises:
        RuntimeError: If init_bq_service() has not been called yet.
    """
    if _bq_service is None:
        raise RuntimeError(
            "BigQuery service not initialised. Call init_bq_service() in agent.py before using tools."
        )
    return _bq_service


# --- Per-question vector search result cache ---
# Caches the combined vector search result so that vector_search_tables()
# and fetch_few_shot_examples() share a single embedding call.

_vector_cache_question: str | None = None
_vector_cache_result: dict | None = None


def cache_vector_result(question: str, result: dict) -> None:
    """Store combined vector search result for the given question."""
    global _vector_cache_question, _vector_cache_result
    _vector_cache_question = question
    _vector_cache_result = result
    logger.info("vector_cache_stored", question=question[:80])


def get_cached_vector_result(question: str) -> dict | None:
    """Return cached result if the question matches, else None."""
    if _vector_cache_question == question and _vector_cache_result is not None:
        logger.info("vector_cache_hit", question=question[:80])
        return _vector_cache_result
    return None


def clear_vector_cache() -> None:
    """Reset the vector cache (for test isolation and session boundaries)."""
    global _vector_cache_question, _vector_cache_result
    _vector_cache_question = None
    _vector_cache_result = None
