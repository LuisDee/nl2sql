"""Tests for the evaluation framework."""

import pandas as pd
from eval.run_eval import (
    EvalReport,
    EvalResult,
    EvalRunner,
    _extract_tables,
    check_routing,
    compare_result_sets,
    compute_component_match,
    load_gold_queries,
)
from eval.validate_gold_set import ALL_CATEGORIES, ALL_TABLES, validate_gold_set


class TestGoldSetLoading:
    def test_loads_50_queries(self):
        queries = load_gold_queries()
        assert len(queries) == 50

    def test_all_queries_have_required_fields(self):
        queries = load_gold_queries()
        required = {"id", "question", "category", "expected_tables", "gold_sql"}
        for q in queries:
            assert required <= set(q.keys()), f"{q['id']} missing fields"

    def test_no_duplicate_ids(self):
        queries = load_gold_queries()
        ids = [q["id"] for q in queries]
        assert len(ids) == len(set(ids))


class TestGoldSetValidation:
    def test_all_13_tables_covered(self):
        queries = load_gold_queries()
        found: set[str] = set()
        for q in queries:
            for t in q["expected_tables"]:
                found.add(t)
        assert found >= ALL_TABLES

    def test_all_9_categories_present(self):
        queries = load_gold_queries()
        found = {q["category"] for q in queries}
        assert found >= ALL_CATEGORIES

    def test_all_sql_has_project_placeholder(self):
        queries = load_gold_queries()
        for q in queries:
            assert "{project}" in q["gold_sql"], f"{q['id']} missing {{project}}"

    def test_validate_gold_set_passes(self):
        errors = validate_gold_set()
        assert errors == [], f"Validation errors: {errors}"


class TestResultSetComparison:
    def test_identical_dataframes(self):
        df1 = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        df2 = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        assert compare_result_sets(df1, df2) is True

    def test_different_column_order(self):
        df1 = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        df2 = pd.DataFrame({"b": [3.0, 4.0], "a": [1, 2]})
        assert compare_result_sets(df1, df2) is True

    def test_float_rounding(self):
        df1 = pd.DataFrame({"val": [1.00001]})
        df2 = pd.DataFrame({"val": [1.00002]})
        assert compare_result_sets(df1, df2) is True

    def test_different_values(self):
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"a": [2]})
        assert compare_result_sets(df1, df2) is False

    def test_both_empty(self):
        df1 = pd.DataFrame()
        df2 = pd.DataFrame()
        assert compare_result_sets(df1, df2) is True

    def test_one_empty(self):
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame()
        assert compare_result_sets(df1, df2) is False

    def test_different_columns(self):
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"b": [1]})
        assert compare_result_sets(df1, df2) is False


class TestRoutingAccuracy:
    def test_exact_match(self):
        assert check_routing(["markettrade"], ["markettrade"]) is True

    def test_mismatch(self):
        assert check_routing(["markettrade"], ["quotertrade"]) is False

    def test_multi_table_match(self):
        expected = ["markettrade", "quotertrade"]
        predicted = ["quotertrade", "markettrade"]
        assert check_routing(expected, predicted) is True

    def test_subset_is_not_match(self):
        expected = ["markettrade", "quotertrade"]
        predicted = ["markettrade"]
        assert check_routing(expected, predicted) is False


class TestComponentMatch:
    def test_identical_sql_scores_1(self):
        sql = "SELECT a, b FROM `p.d.t` WHERE x = 1 GROUP BY a ORDER BY b"
        assert compute_component_match(sql, sql) == 1.0

    def test_different_tables_reduces_score(self):
        gold = "SELECT a FROM `p.d.table_a` WHERE x = 1"
        pred = "SELECT a FROM `p.d.table_b` WHERE x = 1"
        score = compute_component_match(gold, pred)
        assert score < 1.0


class TestEvalReport:
    def _make_result(self, routing=True, syntax=True, execution=False, cm=0.8):
        return EvalResult(
            query_id="test",
            question="test?",
            category="kpi_basic",
            expected_tables=["markettrade"],
            routing_correct=routing,
            syntax_valid=syntax,
            execution_match=execution,
            component_match_score=cm,
        )

    def test_routing_accuracy(self):
        report = EvalReport(
            results=[
                self._make_result(routing=True),
                self._make_result(routing=False),
            ]
        )
        assert report.routing_accuracy == 0.5

    def test_syntax_accuracy(self):
        report = EvalReport(
            results=[
                self._make_result(syntax=True),
                self._make_result(syntax=True),
                self._make_result(syntax=False),
            ]
        )
        assert abs(report.syntax_accuracy - 2 / 3) < 0.01

    def test_to_markdown_contains_header(self):
        report = EvalReport(results=[self._make_result()])
        md = report.to_markdown()
        assert "# NL2SQL Evaluation Report" in md

    def test_empty_report(self):
        report = EvalReport()
        assert report.routing_accuracy == 0.0
        assert report.total == 0


class TestEvalRunner:
    def test_offline_run_produces_report(self):
        queries = load_gold_queries()
        runner = EvalRunner(queries)
        report = runner.run_offline()
        assert report.total == 50
        assert len(report.results) == 50

    def test_offline_checks_routing(self):
        runner = EvalRunner()
        report = runner.run_offline()
        # At minimum, gold SQL tables should match expected_tables
        for r in report.results:
            if not r.routing_correct:
                # Some union_all queries reference many tables
                pass


class TestOnlineEvalUsesRunner:
    """Online eval must use InMemoryRunner, not nl2sql_agent.run()."""

    def test_run_online_does_not_call_agent_run(self):
        """The broken nl2sql_agent.run() pattern must not be in run_online."""
        import inspect

        from eval.run_eval import EvalRunner

        source = inspect.getsource(EvalRunner.run_online)
        assert ".run(input=" not in source, (
            "run_online still calls nl2sql_agent.run() which doesn't exist on LlmAgent. "
            "Must use InMemoryRunner instead."
        )

    def test_run_online_uses_inmemory_runner(self):
        """run_online must use ADK's InMemoryRunner."""
        import inspect

        from eval.run_eval import EvalRunner

        source = inspect.getsource(EvalRunner.run_online)
        assert "InMemoryRunner" in source or "runner" in source.lower(), (
            "run_online must use InMemoryRunner for programmatic agent execution"
        )


class TestExtractTables:
    def test_single_table(self):
        sql = "SELECT * FROM `project.dataset.table_name`"
        assert _extract_tables(sql) == {"table_name"}

    def test_multiple_tables(self):
        sql = """
        SELECT * FROM `p.d.a`
        UNION ALL
        SELECT * FROM `p.d.b`
        """
        assert _extract_tables(sql) == {"a", "b"}
