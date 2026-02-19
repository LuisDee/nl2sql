"""Tests for the SQL dry run validator tool."""

from nl2sql_agent.tools.sql_validator import dry_run_sql


class TestDryRunSql:
    def test_valid_query_returns_valid_status(self, mock_bq):
        result = dry_run_sql("SELECT * FROM my_table")

        assert result["status"] == "valid"
        assert "estimated_mb" in result
        assert result["estimated_mb"] > 0

    def test_invalid_query_returns_error(self, mock_bq):
        mock_bq._default_dry_run_response = {
            "valid": False,
            "total_bytes_processed": 0,
            "error": "Unrecognized name: fake_column",
        }

        result = dry_run_sql("SELECT fake_column FROM my_table")

        assert result["status"] == "invalid"
        assert "fake_column" in result["error_message"]

    def test_passes_sql_to_service(self, mock_bq):
        sql = "SELECT edge_bps FROM `test-project.nl2sql_omx_kpi.markettrade`"
        dry_run_sql(sql)

        assert mock_bq.last_query == sql
