"""Concrete implementations of external service protocols.

These classes implement the protocols defined in protocols.py using
real external services (BigQuery, Vertex AI, etc.).

For unit testing, use the fakes in tests/fakes.py instead.
"""

from typing import Any

import pandas as pd
from google.cloud import bigquery

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.serialization import sanitize_rows

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
        job = self._client.query(sql)
        results = job.result(timeout=settings.bq_query_timeout_seconds).to_dataframe()
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

    def query_with_params(
        self, sql: str, params: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a parameterised SQL query and return results as list of dicts."""
        job_config = bigquery.QueryJobConfig()

        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(p["name"], p["type"], p["value"])
                for p in params
            ]

        logger.info(
            "bq_query_with_params",
            sql_preview=sql[:200],
            has_params=bool(params),
        )

        try:
            query_job = self._client.query(
                sql,
                job_config=job_config,
                timeout=settings.bq_query_timeout_seconds,
            )
            rows = sanitize_rows([
                dict(row)
                for row in query_job.result(
                    timeout=settings.bq_query_timeout_seconds
                )
            ])
            logger.info("bq_query_with_params_complete", row_count=len(rows))
            return rows
        except Exception as e:
            logger.error(
                "bq_query_with_params_error",
                error=str(e),
                sql_preview=sql[:200],
            )
            raise