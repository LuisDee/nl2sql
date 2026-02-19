"""SQL execution tool for BigQuery read-only queries.

Executes validated SQL and returns results. Enforces read-only (SELECT only)
and row limits to prevent runaway queries.
"""

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.serialization import sanitize_rows
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
    first_keyword = stripped.split()[0].upper() if stripped else ""
    if first_keyword not in ("SELECT", "WITH"):
        logger.warning("execute_sql_rejected", first_keyword=first_keyword)
        return {
            "status": "error",
            "error_message": (
                f"Only SELECT queries are allowed. Got: {first_keyword}. "
                "This tool is read-only and cannot modify data."
            ),
        }

    # --- Add LIMIT if not present ---
    max_rows = settings.bq_max_result_rows
    upper = stripped.upper()
    if "LIMIT" not in upper:
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
