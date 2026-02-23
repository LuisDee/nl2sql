"""Tests for scripts/check_coverage.py — enrichment coverage gate."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from check_coverage import (
    _is_placeholder_description,
    check_all_tables,
    check_table_coverage,
)


@pytest.fixture
def catalog_dir(tmp_path):
    """Create a temporary catalog directory structure."""
    kpi_dir = tmp_path / "catalog" / "kpi"
    kpi_dir.mkdir(parents=True)
    data_dir = tmp_path / "catalog" / "data"
    data_dir.mkdir(parents=True)
    return tmp_path / "catalog"


def _write_table_yaml(catalog_dir: Path, layer: str, table: str, columns: list[dict]):
    """Helper to write a minimal table YAML."""
    data = {
        "table": {
            "name": table,
            "dataset": "{kpi_dataset}" if layer == "kpi" else "{data_dataset}",
            "fqn": f"{{project}}.{{{'kpi_dataset' if layer == 'kpi' else 'data_dataset'}}}.{table}",
            "layer": layer,
            "description": f"Test table {table}",
            "partition_field": "trade_date",
            "columns": columns,
        }
    }
    path = catalog_dir / layer / f"{table}.yaml"
    path.write_text(yaml.dump(data, default_flow_style=False))
    return path


class TestIsPlaceholderDescription:
    def test_none_is_placeholder(self):
        assert _is_placeholder_description(None)

    def test_empty_is_placeholder(self):
        assert _is_placeholder_description("")
        assert _is_placeholder_description("  ")

    def test_todo_is_placeholder(self):
        assert _is_placeholder_description("TODO")
        assert _is_placeholder_description("tbd")

    def test_real_description_is_not_placeholder(self):
        assert not _is_placeholder_description("The trading portfolio identifier")


class TestCheckTableCoverage:
    def test_full_coverage_passes(self, catalog_dir):
        columns = [
            {
                "name": "trade_date",
                "type": "DATE",
                "description": "Date of the trade",
                "category": "time",
                "source": "proto::trade_date",
            },
            {
                "name": "symbol",
                "type": "STRING",
                "description": "Underlying instrument",
                "category": "dimension",
                "filterable": True,
                "source": "proto::symbol",
            },
            {
                "name": "instant_pnl",
                "type": "FLOAT",
                "description": "Instant PnL metric",
                "category": "measure",
                "typical_aggregation": "SUM",
                "formula": "tv * qty * buy_sell_multiplier",
                "source": "kpi::instant_pnl",
            },
        ]
        _write_table_yaml(catalog_dir, "kpi", "test_table", columns)

        with patch("check_coverage.CATALOG_DIR", catalog_dir):
            result = check_table_coverage("kpi", "test_table")

        assert result["passed"] is True
        assert result["coverage"]["category"] == 100.0
        assert result["coverage"]["source"] == 100.0
        assert result["coverage"]["description"] == 100.0

    def test_missing_category_fails(self, catalog_dir):
        columns = [
            {
                "name": "col1",
                "type": "STRING",
                "description": "Some column",
                "source": "proto::col1",
                # No category!
            },
            {
                "name": "col2",
                "type": "STRING",
                "description": "Another column",
                "category": "dimension",
                "source": "proto::col2",
            },
        ]
        _write_table_yaml(catalog_dir, "data", "test_table", columns)

        with patch("check_coverage.CATALOG_DIR", catalog_dir):
            result = check_table_coverage(
                "data",
                "test_table",
                {
                    "min_category": 95,
                    "min_source": 90,
                    "min_formula": 85,
                    "min_description": 100,
                },
            )

        assert result["passed"] is False
        assert result["coverage"]["category"] == 50.0
        assert any("category" in g for g in result["gaps"])

    def test_kpi_measure_without_formula_counted(self, catalog_dir):
        columns = [
            {
                "name": "instant_pnl",
                "type": "FLOAT",
                "description": "PnL metric",
                "category": "measure",
                "source": "kpi",
                # No formula!
            },
        ]
        _write_table_yaml(catalog_dir, "kpi", "test_table", columns)

        with patch("check_coverage.CATALOG_DIR", catalog_dir):
            result = check_table_coverage(
                "kpi",
                "test_table",
                {
                    "min_category": 0,
                    "min_source": 0,
                    "min_formula": 95,
                    "min_description": 0,
                },
            )

        assert result["passed"] is False
        assert result["coverage"]["formula"] == 0.0
        assert any("formula" in g for g in result["gaps"])

    def test_data_measure_formula_not_required(self, catalog_dir):
        columns = [
            {
                "name": "price",
                "type": "FLOAT",
                "description": "Trade price",
                "category": "measure",
                "source": "proto::price",
                # No formula — fine for data layer
            },
        ]
        _write_table_yaml(catalog_dir, "data", "test_table", columns)

        with patch("check_coverage.CATALOG_DIR", catalog_dir):
            result = check_table_coverage(
                "data",
                "test_table",
                {
                    "min_category": 0,
                    "min_source": 0,
                    "min_formula": 95,
                    "min_description": 0,
                },
            )

        # formula should be N/A for data layer measures
        assert result["coverage"]["formula"] == "N/A"
        # Should not have a formula gap
        assert not any("formula" in g for g in result["gaps"])

    def test_missing_yaml_returns_error(self, catalog_dir):
        with patch("check_coverage.CATALOG_DIR", catalog_dir):
            result = check_table_coverage("data", "nonexistent")

        assert result["passed"] is False
        assert "YAML not found" in result.get("error", "")

    def test_empty_columns_fails(self, catalog_dir):
        _write_table_yaml(catalog_dir, "data", "empty_table", [])

        with patch("check_coverage.CATALOG_DIR", catalog_dir):
            result = check_table_coverage("data", "empty_table")

        assert result["passed"] is False


class TestCheckAllTables:
    def test_json_output_structure(self, catalog_dir):
        columns = [
            {
                "name": "col1",
                "type": "STRING",
                "description": "A column",
                "category": "dimension",
                "source": "proto::col1",
            },
        ]
        _write_table_yaml(catalog_dir, "data", "markettrade", columns)

        with (
            patch("check_coverage.CATALOG_DIR", catalog_dir),
            patch("check_coverage.ALL_TABLES", {"data": ["markettrade"]}),
            patch(
                "check_coverage.filter_tables", return_value={"data": ["markettrade"]}
            ),
        ):
            results = check_all_tables(table_filter="markettrade")

        assert "data/markettrade" in results
        report = results["data/markettrade"]
        assert "passed" in report
        assert "coverage" in report
        assert "gaps" in report
        assert "total_columns" in report

    def test_single_table_filter(self, catalog_dir):
        columns = [
            {
                "name": "col1",
                "type": "STRING",
                "description": "A column",
                "category": "dimension",
                "source": "proto::col1",
            },
        ]
        _write_table_yaml(catalog_dir, "kpi", "markettrade", columns)
        _write_table_yaml(catalog_dir, "kpi", "quotertrade", columns)

        with (
            patch("check_coverage.CATALOG_DIR", catalog_dir),
            patch(
                "check_coverage.filter_tables",
                return_value={"kpi": ["markettrade"]},
            ),
        ):
            results = check_all_tables(layer_filter="kpi", table_filter="markettrade")

        assert len(results) == 1
        assert "kpi/markettrade" in results
