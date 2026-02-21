"""Integration tests for BigQuery connectivity.

Verifies that the BigQuery client can connect, execute queries,
and access the expected datasets and tables.
"""


class TestBigQueryConnectivity:
    def test_bq_select_one(self, bq_client):
        """BigQuery client must execute a trivial query."""
        df = bq_client.execute_query("SELECT 1 AS n")
        assert len(df) == 1
        assert df.iloc[0]["n"] == 1

    def test_bq_dry_run_valid(self, bq_client, real_settings):
        """Dry run of valid SQL must return valid=True."""
        sql = f"""
        SELECT trade_date, symbol
        FROM `{real_settings.gcp_project}.{real_settings.kpi_dataset}.markettrade`
        WHERE trade_date = CURRENT_DATE()
        LIMIT 1
        """
        result = bq_client.dry_run_query(sql)
        assert result["valid"] is True
        assert result["total_bytes_processed"] >= 0

    def test_bq_dry_run_invalid_sql(self, bq_client):
        """Dry run of invalid SQL must return valid=False with error."""
        result = bq_client.dry_run_query(
            "SELECT nonexistent_col FROM nonexistent_table"
        )
        assert result["valid"] is False
        assert result["error"] is not None

    def test_kpi_markettrade_accessible(self, bq_client, real_settings):
        """Must be able to query kpi.markettrade with a date filter."""
        sql = f"""
        SELECT trade_date, symbol
        FROM `{real_settings.gcp_project}.{real_settings.kpi_dataset}.markettrade`
        WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        LIMIT 1
        """
        df = bq_client.execute_query(sql)
        # May be 0 rows if no recent data, but query must not error
        assert df is not None
