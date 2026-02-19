"""Shared dependency injection for all tool modules.

This module holds the BigQuery service instance that all tools share.
It is initialised once at agent startup via init_bq_service().

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
