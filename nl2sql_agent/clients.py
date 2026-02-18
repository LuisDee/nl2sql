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