"""Tests for scripts/generate_skeleton.py — skeleton YAML generator."""

import sys
from pathlib import Path

import pytest
import yaml

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from generate_skeleton import generate_skeleton_yaml, write_skeleton

SAMPLE_COLUMNS = [
    {"name": "trade_date", "type": "DATE"},
    {"name": "symbol", "type": "STRING"},
    {"name": "price", "type": "FLOAT"},
    {"name": "quantity", "type": "INTEGER"},
]


class TestGenerateSkeletonYaml:
    def test_generates_valid_yaml(self):
        content = generate_skeleton_yaml("kpi", "test_table", SAMPLE_COLUMNS)
        data = yaml.safe_load(content)

        table = data["table"]
        assert table["name"] == "test_table"
        assert table["layer"] == "kpi"
        assert table["partition_field"] == "trade_date"
        assert len(table["columns"]) == 4

    def test_uses_project_placeholder(self):
        content = generate_skeleton_yaml("data", "test_table", SAMPLE_COLUMNS)
        data = yaml.safe_load(content)
        assert "{project}" in data["table"]["fqn"]

    def test_uses_kpi_dataset_placeholder(self):
        content = generate_skeleton_yaml("kpi", "test_table", SAMPLE_COLUMNS)
        data = yaml.safe_load(content)
        assert "{kpi_dataset}" in data["table"]["dataset"]
        assert "{kpi_dataset}" in data["table"]["fqn"]

    def test_uses_data_dataset_placeholder(self):
        content = generate_skeleton_yaml("data", "test_table", SAMPLE_COLUMNS)
        data = yaml.safe_load(content)
        assert "{data_dataset}" in data["table"]["dataset"]
        assert "{data_dataset}" in data["table"]["fqn"]

    def test_all_columns_present(self):
        content = generate_skeleton_yaml("data", "test_table", SAMPLE_COLUMNS)
        data = yaml.safe_load(content)
        col_names = [c["name"] for c in data["table"]["columns"]]
        expected = [c["name"] for c in SAMPLE_COLUMNS]
        assert col_names == expected

    def test_column_types_preserved(self):
        content = generate_skeleton_yaml("data", "test_table", SAMPLE_COLUMNS)
        data = yaml.safe_load(content)
        for i, col in enumerate(data["table"]["columns"]):
            assert col["type"] == SAMPLE_COLUMNS[i]["type"]

    def test_descriptions_are_empty(self):
        content = generate_skeleton_yaml("data", "test_table", SAMPLE_COLUMNS)
        data = yaml.safe_load(content)
        assert data["table"]["description"] == ""
        for col in data["table"]["columns"]:
            assert col["description"] == ""

    def test_empty_columns_list(self):
        content = generate_skeleton_yaml("data", "test_table", [])
        data = yaml.safe_load(content)
        assert data["table"]["columns"] == []


class TestWriteSkeleton:
    def test_writes_file(self, tmp_path):
        catalog = tmp_path / "catalog" / "data"
        catalog.mkdir(parents=True)

        from unittest.mock import patch

        with patch("generate_skeleton.CATALOG_DIR", tmp_path / "catalog"):
            path = write_skeleton("data", "test_table", SAMPLE_COLUMNS)

        assert path.exists()
        data = yaml.safe_load(path.read_text())
        assert data["table"]["name"] == "test_table"

    def test_refuses_overwrite_without_force(self, tmp_path):
        catalog = tmp_path / "catalog" / "data"
        catalog.mkdir(parents=True)
        existing = catalog / "test_table.yaml"
        existing.write_text("existing content")

        from unittest.mock import patch

        with (
            patch("generate_skeleton.CATALOG_DIR", tmp_path / "catalog"),
            pytest.raises(FileExistsError, match="Use --force"),
        ):
            write_skeleton("data", "test_table", SAMPLE_COLUMNS)

    def test_force_overwrites(self, tmp_path):
        catalog = tmp_path / "catalog" / "data"
        catalog.mkdir(parents=True)
        existing = catalog / "test_table.yaml"
        existing.write_text("old content")

        from unittest.mock import patch

        with patch("generate_skeleton.CATALOG_DIR", tmp_path / "catalog"):
            path = write_skeleton("data", "test_table", SAMPLE_COLUMNS, force=True)

        content = path.read_text()
        assert "old content" not in content
        data = yaml.safe_load(content)
        assert data["table"]["name"] == "test_table"

    def test_creates_parent_dirs(self, tmp_path):
        from unittest.mock import patch

        with patch("generate_skeleton.CATALOG_DIR", tmp_path / "catalog"):
            path = write_skeleton("kpi", "test_table", SAMPLE_COLUMNS)

        assert path.exists()
        assert path.parent.name == "kpi"
