"""Tests for scripts/table_registry.py — centralized table registry."""

import sys
from pathlib import Path

import pytest

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from table_registry import ALL_TABLES, KPI_TABLES, all_table_pairs, filter_tables

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATALOG_DIR = PROJECT_ROOT / "catalog"


class TestAllTables:
    def test_has_both_layers(self):
        assert "kpi" in ALL_TABLES
        assert "data" in ALL_TABLES

    def test_kpi_tables_not_empty(self):
        assert len(ALL_TABLES["kpi"]) > 0

    def test_data_tables_not_empty(self):
        assert len(ALL_TABLES["data"]) > 0

    def test_no_duplicate_tables_per_layer(self):
        for layer, tables in ALL_TABLES.items():
            assert len(tables) == len(set(tables)), f"Duplicates in {layer}"


class TestKpiTables:
    def test_kpi_tables_matches_all_tables_kpi(self):
        assert ALL_TABLES["kpi"] == KPI_TABLES

    def test_kpi_tables_is_same_object(self):
        assert KPI_TABLES is ALL_TABLES["kpi"]


class TestAllTablePairs:
    def test_count_matches_sum(self):
        expected = sum(len(tables) for tables in ALL_TABLES.values())
        assert len(all_table_pairs()) == expected

    def test_returns_tuples(self):
        pairs = all_table_pairs()
        for pair in pairs:
            assert isinstance(pair, tuple)
            assert len(pair) == 2

    def test_all_layers_valid(self):
        pairs = all_table_pairs()
        for layer, _ in pairs:
            assert layer in ALL_TABLES


class TestFilterTables:
    def test_no_filter_returns_all(self):
        result = filter_tables()
        assert result == ALL_TABLES

    def test_filter_by_layer(self):
        result = filter_tables(layer="kpi")
        assert list(result.keys()) == ["kpi"]
        assert result["kpi"] == ALL_TABLES["kpi"]

    def test_filter_by_table(self):
        result = filter_tables(table="markettrade")
        # markettrade exists in both layers
        assert "kpi" in result
        assert "data" in result
        assert result["kpi"] == ["markettrade"]
        assert result["data"] == ["markettrade"]

    def test_filter_by_layer_and_table(self):
        result = filter_tables(layer="kpi", table="markettrade")
        assert result == {"kpi": ["markettrade"]}

    def test_unknown_layer_raises(self):
        with pytest.raises(ValueError, match="Unknown layer"):
            filter_tables(layer="nonexistent")

    def test_unknown_table_raises(self):
        with pytest.raises(ValueError, match="not found"):
            filter_tables(table="nonexistent_table_xyz")

    def test_returns_copy(self):
        result = filter_tables()
        result["kpi"] = []
        assert len(ALL_TABLES["kpi"]) > 0


class TestEachTableHasYaml:
    @pytest.mark.parametrize("layer,table", all_table_pairs())
    def test_yaml_exists(self, layer, table):
        path = CATALOG_DIR / layer / f"{table}.yaml"
        assert path.exists(), f"Missing YAML: {path}"
