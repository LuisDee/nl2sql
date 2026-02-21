"""Tests for LiveBigQueryClient."""

import inspect

from nl2sql_agent.clients import LiveBigQueryClient


class TestExecuteQueryTimeout:
    """execute_query must use a timeout to prevent indefinite hangs."""

    def test_execute_query_uses_timeout(self):
        """execute_query source must reference bq_query_timeout_seconds."""
        source = inspect.getsource(LiveBigQueryClient.execute_query)
        assert "timeout" in source, (
            "execute_query() has no timeout — agent can hang indefinitely "
            "on expensive queries. Use job.result(timeout=...)"
        )

    def test_execute_query_uses_two_step_pattern(self):
        """execute_query should use job = query() then job.result(timeout=...)."""
        source = inspect.getsource(LiveBigQueryClient.execute_query)
        # Must NOT use the one-liner .query(sql).to_dataframe() pattern
        assert ".query(sql).to_dataframe()" not in source, (
            "execute_query() uses chained .query(sql).to_dataframe() which "
            "does not support timeout. Use job = query(sql), job.result(timeout=...)"
        )
