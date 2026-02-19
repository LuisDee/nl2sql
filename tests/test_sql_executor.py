"""Tests for the SQL executor tool."""

from nl2sql_agent.tools.sql_executor import execute_sql


class TestExecuteSql:
    def test_select_query_succeeds(self, mock_bq):
        mock_bq._default_query_response = [
            {"edge_bps": 5.2, "symbol": "TEST"},
        ]

        result = execute_sql("SELECT edge_bps, symbol FROM my_table")

        assert result["status"] == "success"
        assert result["row_count"] == 1
        assert result["rows"][0]["edge_bps"] == 5.2

    def test_with_cte_query_succeeds(self, mock_bq):
        mock_bq._default_query_response = [{"total": 42}]

        result = execute_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")

        assert result["status"] == "success"

    def test_rejects_insert_query(self, mock_bq):
        result = execute_sql("INSERT INTO my_table VALUES (1, 2)")

        assert result["status"] == "error"
        assert "Only SELECT" in result["error_message"]

    def test_rejects_delete_query(self, mock_bq):
        result = execute_sql("DELETE FROM my_table WHERE id = 1")

        assert result["status"] == "error"

    def test_rejects_drop_query(self, mock_bq):
        result = execute_sql("DROP TABLE my_table")

        assert result["status"] == "error"

    def test_rejects_update_query(self, mock_bq):
        result = execute_sql("UPDATE my_table SET x = 1")

        assert result["status"] == "error"

    def test_adds_limit_when_missing(self, mock_bq):
        execute_sql("SELECT * FROM my_table")

        assert "LIMIT" in mock_bq.last_query

    def test_does_not_add_limit_when_present(self, mock_bq):
        execute_sql("SELECT * FROM my_table LIMIT 10")

        # Should NOT add a second LIMIT
        assert mock_bq.last_query.count("LIMIT") == 1

    def test_returns_truncation_warning(self, mock_bq):
        # Return exactly max_rows to trigger truncation warning
        mock_bq._default_query_response = [{"x": i} for i in range(1000)]

        result = execute_sql("SELECT x FROM my_table")

        assert "warning" in result
        assert "truncated" in result["warning"].lower()

    def test_returns_error_on_exception(self, mock_bq):
        original_method = mock_bq.execute_query

        def exploding_query(*args, **kwargs):
            raise RuntimeError("timeout exceeded")

        mock_bq.execute_query = exploding_query

        result = execute_sql("SELECT 1")

        assert result["status"] == "error"
        assert "timeout" in result["error_message"]

        mock_bq.execute_query = original_method
