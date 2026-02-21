"""Integration tests: validate all gold SQL passes BigQuery dry-run.

These tests require real BigQuery credentials and are marked @pytest.mark.integration.
Run with: pytest -m integration tests/integration/test_eval_gold_set.py -v
"""

import pytest
from eval.run_eval import load_gold_queries

from nl2sql_agent.catalog_loader import resolve_example_sql

_GOLD_QUERIES = load_gold_queries()
_QUERY_IDS = [q["id"] for q in _GOLD_QUERIES]


@pytest.mark.parametrize(
    "query_index",
    range(len(_GOLD_QUERIES)),
    ids=_QUERY_IDS,
)
def test_gold_sql_dry_run(bq_client, real_settings, query_index):
    """Each gold SQL query must pass BigQuery dry-run validation."""
    q = _GOLD_QUERIES[query_index]
    resolved_sql = resolve_example_sql(q["gold_sql"], real_settings.gcp_project)

    result = bq_client.dry_run_query(resolved_sql)

    assert result["valid"], (
        f"Gold query {q['id']} failed dry-run:\n"
        f"  Question: {q['question']}\n"
        f"  Error: {result.get('error', 'unknown')}\n"
        f"  SQL: {resolved_sql[:200]}"
    )
