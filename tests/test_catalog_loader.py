"""Unit tests for catalog_loader module (no BQ required)."""

from nl2sql_agent.catalog_loader import (
    validate_table_yaml,
    validate_examples_yaml,
    resolve_fqn,
    resolve_example_sql,
)


class TestResolveFqn:
    """Test the FQN resolution helper."""

    def test_resolve_fqn_dev(self):
        table_data = {"fqn": "{project}.nl2sql_omx_kpi.markettrade"}
        result = resolve_fqn(table_data, "melodic-stone-437916-t3")
        assert result == "melodic-stone-437916-t3.nl2sql_omx_kpi.markettrade"

    def test_resolve_fqn_prod(self):
        table_data = {"fqn": "{project}.nl2sql_omx_kpi.markettrade"}
        result = resolve_fqn(table_data, "cloud-data-n-base-d4b3")
        assert result == "cloud-data-n-base-d4b3.nl2sql_omx_kpi.markettrade"


class TestResolveExampleSql:
    """Test the example SQL resolution helper."""

    def test_resolve_single_table(self):
        sql = "SELECT * FROM `{project}.nl2sql_omx_kpi.markettrade` WHERE trade_date = '2026-02-17'"
        result = resolve_example_sql(sql, "melodic-stone-437916-t3")
        assert "melodic-stone-437916-t3.nl2sql_omx_kpi.markettrade" in result
        assert "{project}" not in result

    def test_resolve_multiple_tables(self):
        sql = """SELECT * FROM `{project}.nl2sql_omx_kpi.markettrade`
        UNION ALL SELECT * FROM `{project}.nl2sql_omx_kpi.quotertrade`"""
        result = resolve_example_sql(sql, "melodic-stone-437916-t3")
        assert result.count("melodic-stone-437916-t3") == 2
        assert "{project}" not in result


class TestValidateTableYaml:
    """Test the table YAML validator itself."""

    def test_valid_minimal_table(self):
        """A minimal valid table YAML should produce no errors."""
        data = {
            "table": {
                "name": "markettrade",
                "dataset": "nl2sql_omx_kpi",
                "fqn": "{project}.nl2sql_omx_kpi.markettrade",
                "layer": "kpi",
                "description": "KPI metrics for market trades",
                "partition_field": "trade_date",
                "columns": [
                    {"name": "trade_date", "type": "DATE", "description": "Trade date"}
                ],
            }
        }
        errors = validate_table_yaml(data)
        assert errors == []

    def test_missing_table_key(self):
        """Missing 'table' top-level key should produce an error."""
        errors = validate_table_yaml({"not_table": {}})
        assert len(errors) == 1
        assert "Missing top-level 'table' key" in errors[0]

    def test_invalid_layer(self):
        """Invalid layer value should produce an error."""
        data = {
            "table": {
                "name": "test", "dataset": "nl2sql_omx_kpi",
                "fqn": "{project}.nl2sql_omx_kpi.test",
                "layer": "gold",
                "description": "x", "partition_field": "trade_date",
                "columns": [{"name": "a", "type": "STRING", "description": "x"}],
            }
        }
        errors = validate_table_yaml(data)
        assert any("Invalid layer" in e for e in errors)

    def test_invalid_dataset(self):
        """Invalid dataset value should produce an error."""
        data = {
            "table": {
                "name": "test", "dataset": "dev_agent_test",
                "fqn": "{project}.dev_agent_test.test",
                "layer": "kpi", "description": "x",
                "partition_field": "trade_date",
                "columns": [{"name": "a", "type": "STRING", "description": "x"}],
            }
        }
        errors = validate_table_yaml(data)
        assert any("Invalid dataset" in e for e in errors)

    def test_fqn_without_project_placeholder(self):
        """fqn without {project} placeholder should produce an error."""
        data = {
            "table": {
                "name": "test", "dataset": "nl2sql_omx_kpi",
                "fqn": "hardcoded-project.nl2sql_omx_kpi.test",
                "layer": "kpi", "description": "x",
                "partition_field": "trade_date",
                "columns": [{"name": "a", "type": "STRING", "description": "x"}],
            }
        }
        errors = validate_table_yaml(data)
        assert any("{project}" in e for e in errors)


class TestValidateExamplesYaml:
    """Test the examples YAML validator."""

    def test_valid_example(self):
        data = {
            "examples": [{
                "question": "What was the PnL?",
                "sql": "SELECT * FROM `{project}.nl2sql_omx_kpi.markettrade` WHERE trade_date = '2026-02-17'",
                "tables_used": ["markettrade"],
                "dataset": "nl2sql_omx_kpi",
                "complexity": "simple",
            }]
        }
        errors = validate_examples_yaml(data)
        assert errors == []

    def test_missing_project_placeholder_in_sql(self):
        data = {
            "examples": [{
                "question": "What?",
                "sql": "SELECT * FROM markettrade WHERE trade_date = '2026-02-17'",
                "tables_used": ["markettrade"],
                "dataset": "nl2sql_omx_kpi",
                "complexity": "simple",
            }]
        }
        errors = validate_examples_yaml(data)
        assert any("{project}" in e for e in errors)
