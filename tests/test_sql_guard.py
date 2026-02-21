"""Tests for the shared SQL guard module."""

from nl2sql_agent.sql_guard import contains_dml


class TestContainsDml:
    """contains_dml must detect DML/DDL in the full SQL body, not just first keyword."""

    def test_select_allowed(self):
        is_blocked, _ = contains_dml("SELECT * FROM t")
        assert not is_blocked

    def test_with_select_allowed(self):
        is_blocked, _ = contains_dml("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert not is_blocked

    def test_insert_blocked(self):
        is_blocked, reason = contains_dml("INSERT INTO t VALUES (1, 2)")
        assert is_blocked
        assert "INSERT" in reason

    def test_with_insert_blocked(self):
        """WITH ... INSERT INTO must be caught (bypasses first-keyword check)."""
        is_blocked, reason = contains_dml(
            "WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte"
        )
        assert is_blocked
        assert "INSERT" in reason

    def test_semicolon_multi_statement_blocked(self):
        is_blocked, reason = contains_dml("SELECT 1; DROP TABLE t")
        assert is_blocked

    def test_delete_blocked(self):
        is_blocked, reason = contains_dml("DELETE FROM t WHERE id = 1")
        assert is_blocked
        assert "DELETE" in reason

    def test_drop_blocked(self):
        is_blocked, reason = contains_dml("DROP TABLE t")
        assert is_blocked

    def test_update_blocked(self):
        is_blocked, reason = contains_dml("UPDATE t SET x = 1")
        assert is_blocked

    def test_truncate_blocked(self):
        is_blocked, reason = contains_dml("TRUNCATE TABLE t")
        assert is_blocked

    def test_merge_blocked(self):
        is_blocked, reason = contains_dml("MERGE INTO t USING s ON t.id = s.id")
        assert is_blocked

    def test_create_blocked(self):
        is_blocked, reason = contains_dml("CREATE TABLE t (id INT)")
        assert is_blocked

    def test_alter_blocked(self):
        is_blocked, reason = contains_dml("ALTER TABLE t ADD COLUMN x INT")
        assert is_blocked

    def test_returns_tuple(self):
        result = contains_dml("SELECT 1")
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_empty_string(self):
        is_blocked, _ = contains_dml("")
        assert not is_blocked

    def test_dml_in_string_literal_not_blocked(self):
        """DML keywords inside string literals should not trigger blocking."""
        is_blocked, _ = contains_dml("SELECT 'INSERT INTO' AS label FROM t")
        assert not is_blocked
