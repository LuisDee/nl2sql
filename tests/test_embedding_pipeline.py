"""Tests that embedding SQL templates handle NULL embeddings correctly.

The ARRAY_LENGTH(NULL) bug: BigQuery returns NULL for ARRAY_LENGTH(NULL),
so WHERE ARRAY_LENGTH(t.embedding) = 0 never matches newly inserted rows
(whose embedding column defaults to NULL). The fix is:
WHERE t.embedding IS NULL OR ARRAY_LENGTH(t.embedding) = 0
"""


class TestEmbeddingNullPredicate:
    """All embedding SQL templates must handle NULL embeddings."""

    def test_learning_loop_embed_sql_handles_null(self):
        """learning_loop._EMBED_NEW_ROWS_SQL must match NULL embedding rows."""
        from nl2sql_agent.tools.learning_loop import _EMBED_NEW_ROWS_SQL

        assert "IS NULL" in _EMBED_NEW_ROWS_SQL, (
            "Missing IS NULL check — ARRAY_LENGTH(NULL) returns NULL, "
            "not 0, so newly inserted rows are never embedded"
        )
        assert "ARRAY_LENGTH" in _EMBED_NEW_ROWS_SQL

    def test_run_embeddings_schema_handles_null(self):
        """run_embeddings schema_embeddings UPDATE must handle NULL."""
        from scripts.run_embeddings import generate_embeddings
        import inspect

        source = inspect.getsource(generate_embeddings)
        # The function builds SQL strings with WHERE clauses
        # Count occurrences of the fixed predicate
        assert source.count("IS NULL") >= 3, (
            "All 3 embedding UPDATE statements in generate_embeddings() "
            "must handle NULL embeddings"
        )

    def test_run_embeddings_all_updates_have_null_check(self):
        """Every WHERE ARRAY_LENGTH clause must be preceded by IS NULL."""
        from scripts.run_embeddings import generate_embeddings
        import inspect

        source = inspect.getsource(generate_embeddings)
        # Every ARRAY_LENGTH check should have a corresponding IS NULL
        array_length_count = source.count("ARRAY_LENGTH(t.embedding)")
        is_null_count = source.count("t.embedding IS NULL")
        assert array_length_count == is_null_count, (
            f"Found {array_length_count} ARRAY_LENGTH checks but "
            f"{is_null_count} IS NULL checks — all must be paired"
        )
