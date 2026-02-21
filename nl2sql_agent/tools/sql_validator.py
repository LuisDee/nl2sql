"""SQL validation tool using BigQuery dry run.

Validates SQL syntax, column references, table permissions, and estimates
query cost — all without executing the query.
"""

from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.tools._deps import get_bq_service
from nl2sql_agent.types import DryRunInvalidResult, DryRunValidResult

logger = get_logger(__name__)


def dry_run_sql(sql_query: str) -> DryRunValidResult | DryRunInvalidResult:
    """Validate a BigQuery SQL query without executing it.

    Use this tool AFTER generating SQL to check for syntax errors,
    invalid column names, missing table permissions, and to estimate
    the query cost in bytes processed. If the dry run fails, examine
    the error message and fix the SQL before trying execute_sql.

    Common errors and fixes:
    - "Unrecognized name: column_x" → check YAML metadata for correct column name
    - "Not found: Table" → check fully-qualified table name format
    - "Access Denied" → table may not exist or permissions missing

    Args:
        sql_query: The BigQuery SQL query to validate.

    Returns:
        Dict with 'status' ('valid' or 'invalid'), and either
        'estimated_bytes' and 'estimated_mb' if valid, or
        'error_message' if invalid.
    """
    bq = get_bq_service()

    logger.info("dry_run_start", sql_preview=sql_query[:200])

    result = bq.dry_run_query(sql_query)

    if result["valid"]:
        mb = result["total_bytes_processed"] / (1024 * 1024)
        logger.info("dry_run_valid", estimated_mb=round(mb, 2))
        return {
            "status": "valid",
            "estimated_bytes": result["total_bytes_processed"],
            "estimated_mb": round(mb, 2),
        }
    else:
        logger.warning("dry_run_invalid", error=result["error"])
        return {
            "status": "invalid",
            "error_message": result["error"],
        }
