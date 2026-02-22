"""Validate that example SQL uses correct column names from the catalog.

Catches poisoned few-shot examples that reference nonexistent columns,
which causes the LLM to generate broken SQL.
"""

import re
from pathlib import Path

import pytest
import yaml

CATALOG_DIR = Path(__file__).parent.parent / "catalog"
EXAMPLES_DIR = Path(__file__).parent.parent / "examples"


def _load_catalog_columns(layer: str, table_name: str) -> set[str]:
    """Load column names from catalog/{layer}/{table_name}.yaml."""
    yaml_path = CATALOG_DIR / layer / f"{table_name}.yaml"
    if not yaml_path.exists():
        return set()
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    # Columns are nested under table.columns
    table = data.get("table", {})
    columns = table.get("columns", [])
    return {c["name"] for c in columns if isinstance(c, dict) and "name" in c}


def _extract_sql_identifiers(sql: str) -> set[str]:
    """Extract potential column identifiers from SQL.

    Returns bare identifiers that appear as column references,
    excluding SQL keywords, table aliases, and function names.
    """
    sql_keywords = {
        "SELECT",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "AS",
        "ON",
        "IN",
        "IS",
        "NULL",
        "BETWEEN",
        "LIKE",
        "ORDER",
        "BY",
        "GROUP",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "UNION",
        "ALL",
        "DISTINCT",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "OUTER",
        "CROSS",
        "FULL",
        "WITH",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "ASC",
        "DESC",
        "INSERT",
        "INTO",
        "VALUES",
        "UPDATE",
        "SET",
        "DELETE",
        "CREATE",
        "TABLE",
        "IF",
        "EXISTS",
        "TRUE",
        "FALSE",
        "CURRENT_DATE",
        "TIMESTAMP",
        "ROUND",
        "SUM",
        "AVG",
        "COUNT",
        "MIN",
        "MAX",
        "ABS",
        "SPLIT",
    }
    # Match identifiers: word characters after dot (table.column) or standalone
    # We specifically look for table_alias.column_name patterns
    dot_refs = re.findall(r"\b\w+\.(\w+)\b", sql)
    return {col for col in dot_refs if col.upper() not in sql_keywords}


def _get_table_layer(table_name: str, dataset_ref: str) -> str:
    """Determine catalog layer from dataset reference."""
    if "kpi" in dataset_ref:
        return "kpi"
    elif "data" in dataset_ref:
        return "data"
    return "kpi"  # default


class TestExampleColumnNames:
    """Every column referenced in example SQL must exist in the catalog."""

    def test_kpi_examples_no_bare_edge_on_non_markettrade(self):
        """Non-markettrade KPI tables must use instant_edge, not edge."""
        with open(EXAMPLES_DIR / "kpi_examples.yaml") as f:
            data = yaml.safe_load(f)

        errors = []
        for ex in data["examples"]:
            tables = ex.get("tables_used", [])
            sql = ex["sql"]
            # Skip multi-table queries and markettrade-only queries
            if len(tables) != 1 or tables[0] == "markettrade":
                continue
            table = tables[0]
            catalog_cols = _load_catalog_columns("kpi", table)
            if not catalog_cols:
                continue
            # Check for bare `edge` usage (not instant_edge, edge_model_type)
            # Matches: AVG(edge), SUM(edge), edge AS, edge > etc.
            if re.search(r"(?<!\w)edge(?!_)\b", sql) and "edge" not in catalog_cols:
                errors.append(
                    f"Q: {ex['question'][:60]} | table={table} "
                    f"uses 'edge' but catalog only has 'instant_edge'"
                )

        assert not errors, "Wrong column names in kpi_examples.yaml:\n" + "\n".join(
            errors
        )

    def test_kpi_examples_correct_slippage_column_names(self):
        """Slippage columns must use {metric}_{interval}_per_unit pattern."""
        with open(EXAMPLES_DIR / "kpi_examples.yaml") as f:
            data = yaml.safe_load(f)

        errors = []
        for ex in data["examples"]:
            sql = ex["sql"]
            # Check for wrong pattern: vol_slippage_per_unit_10m
            if "vol_slippage_per_unit_10m" in sql:
                errors.append(
                    f"Q: {ex['question'][:60]} | "
                    "uses 'vol_slippage_per_unit_10m' "
                    "but correct name is 'vol_slippage_10m_per_unit'"
                )
            # Check for wrong pattern: delta_slippage_fired_at_1h
            if "delta_slippage_fired_at_1h" in sql:
                errors.append(
                    f"Q: {ex['question'][:60]} | "
                    "uses 'delta_slippage_fired_at_1h' "
                    "but correct name is 'delta_slippage_1h_fired_at'"
                )

        assert not errors, "Wrong slippage column names:\n" + "\n".join(errors)

    def test_data_examples_correct_marketdepth_columns(self):
        """marketdepth examples must use bid_volume_0 not bid_size_0."""
        with open(EXAMPLES_DIR / "data_examples.yaml") as f:
            data = yaml.safe_load(f)

        errors = []
        for ex in data["examples"]:
            sql = ex["sql"]
            tables = ex.get("tables_used", [])
            if "marketdepth" not in tables:
                continue

            for wrong, correct in [
                ("bid_size_0", "bid_volume_0"),
                ("ask_size_0", "ask_volume_0"),
                ("bid_size_1", "bid_volume_1"),
                ("ask_size_1", "ask_volume_1"),
            ]:
                if wrong in sql:
                    errors.append(
                        f"Q: {ex['question'][:60]} | "
                        f"uses '{wrong}' but correct name is '{correct}'"
                    )

        assert not errors, "Wrong marketdepth columns:\n" + "\n".join(errors)

    def test_data_examples_no_putcall_column(self):
        """marketdepth examples must use option_type_name not putcall."""
        with open(EXAMPLES_DIR / "data_examples.yaml") as f:
            data = yaml.safe_load(f)

        errors = []
        for ex in data["examples"]:
            sql = ex["sql"]
            tables = ex.get("tables_used", [])
            if "marketdepth" not in tables:
                continue
            # Match putcall as a column name (not inside a string or comment)
            if re.search(r"\bputcall\b", sql):
                errors.append(
                    f"Q: {ex['question'][:60]} | "
                    "uses 'putcall' but correct name is 'option_type_name'"
                )

        assert not errors, "Wrong putcall column:\n" + "\n".join(errors)

    def test_no_union_all_markettrade_with_mako_tables(self):
        """UNION ALL must not combine markettrade with Mako-specific tables.

        markettrade contains ALL trades (Mako + counterparties).
        quotertrade/brokertrade/clicktrade/otoswing are Mako-only subsets.
        Combining them double-counts Mako's PnL.
        """
        mako_tables = {"quotertrade", "brokertrade", "clicktrade", "otoswing"}

        for yaml_file in ["kpi_examples.yaml", "routing_examples.yaml"]:
            path = EXAMPLES_DIR / yaml_file
            if not path.exists():
                continue
            with open(path) as f:
                data = yaml.safe_load(f)

            for ex in data.get("examples", []):
                tables = set(ex.get("tables_used", []))
                if "markettrade" in tables and tables & mako_tables:
                    pytest.fail(
                        f"Double-counting risk in {yaml_file}: "
                        f"Q: {ex['question'][:60]} | "
                        f"tables_used combines markettrade with {tables & mako_tables}. "
                        "markettrade already contains Mako trades."
                    )

    def test_all_example_columns_exist_in_catalog(self):
        """Every dot-referenced column in example SQL must exist in the union of referenced tables."""
        errors = []
        for yaml_file in [
            "kpi_examples.yaml",
            "data_examples.yaml",
            "routing_examples.yaml",
        ]:
            path = EXAMPLES_DIR / yaml_file
            if not path.exists():
                continue
            with open(path) as f:
                data = yaml.safe_load(f)

            for ex in data.get("examples", []):
                tables_used = ex.get("tables_used", [])
                dataset_ref = ex.get("dataset", "")
                if not tables_used:
                    continue
                layer = _get_table_layer(tables_used[0], dataset_ref)
                sql_cols = _extract_sql_identifiers(ex["sql"])

                # Collect columns from ALL tables used in this query
                all_catalog_cols: set[str] = set()
                for table in tables_used:
                    all_catalog_cols |= _load_catalog_columns(layer, table)

                if not all_catalog_cols:
                    continue

                # Case-insensitive comparison (BQ columns are case-insensitive)
                # Also normalize underscores for CamelCase vs snake_case mismatch
                # (e.g. DataTimestamp vs data_timestamp — both refer to the same BQ column)
                catalog_lower = {c.lower() for c in all_catalog_cols}
                catalog_normalized = {
                    c.lower().replace("_", "") for c in all_catalog_cols
                }

                # Filter out false positives: pure numbers, short tokens
                invalid = {
                    c
                    for c in sql_cols
                    if c.lower() not in catalog_lower
                    and c.lower().replace("_", "") not in catalog_normalized
                    and not c.isdigit()
                    and len(c) > 1
                }
                if invalid:
                    errors.append(
                        f"{yaml_file}: Q='{ex['question'][:50]}' "
                        f"tables={tables_used} invalid_cols={sorted(invalid)}"
                    )

        assert not errors, (
            "Example SQL references columns not in catalog:\n" + "\n".join(errors)
        )
