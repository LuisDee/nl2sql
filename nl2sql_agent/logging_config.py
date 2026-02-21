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
