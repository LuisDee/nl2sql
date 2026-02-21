"""SQL execution tool for BigQuery read-only queries.

Executes validated SQL and returns results. Enforces read-only (SELECT only)
and row limits to prevent runaway queries.
"""

import re

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.serialization import sanitize_rows
from nl2sql_agent.sql_guard import contains_dml
from nl2sql_agent.tools._deps import get_bq_service

logger = get_logger(__name__)


def execute_sql(sql_query: str) -> dict:
    """Execute a validated BigQuery SQL query and return the results.

    Use this tool ONLY after dry_run_sql confirms the query is valid.
    Only SELECT queries are allowed — any attempt to INSERT, UPDATE,
    DELETE, DROP, or otherwise modify data will be rejected.

    Results are limited to 1000 rows maximum. If the query returns more,
    the results are truncated and a warning is included.

    Args:
        sql_query: The BigQuery SQL query to execute (must be SELECT).

    Returns:
        Dict with 'status', 'row_count', and 'rows' (list of row dicts)
        if successful, or 'error_message' if execution failed.
    """
    # --- Read-only enforcement ---
    stripped = sql_query.strip()
    is_blocked, reason = contains_dml(stripped)
    if is_blocked:
        logger.warning("execute_sql_rejected", reason=reason)
        return {"status": "error", "error_message": reason}

    # --- Add LIMIT if not present at outer query level ---
    max_rows = settings.bq_max_result_rows
    has_outer_limit = bool(re.search(r'\bLIMIT\s+\d+\s*$', stripped, re.IGNORECASE))
    if not has_outer_limit:
        sql_query = f"{stripped}\nLIMIT {max_rows}"
        logger.info("execute_sql_limit_added", limit=max_rows)

    bq = get_bq_service()

    logger.info("execute_sql_start", sql_preview=sql_query[:200])

    try:
        df = bq.execute_query(sql_query)
        rows = sanitize_rows(df.to_dict(orient="records"))
        truncated = len(rows) >= max_rows

        logger.info(
            "execute_sql_complete",
            row_count=len(rows),
            truncated=truncated,
        )

        result = {
            "status": "success",
            "row_count": len(rows),
            "rows": rows,
        }
        if truncated:
            result["warning"] = (
                f"Results truncated to {max_rows} rows. "
                "Add more specific filters to see all data."
            )

        return result

    except Exception as e:
        logger.error("execute_sql_error", error=str(e))
        return {
            "status": "error",
            "error_message": str(e),
        }
