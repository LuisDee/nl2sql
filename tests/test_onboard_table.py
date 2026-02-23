"""Tests for scripts/onboard_table.py — orchestrator pipeline."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from onboard_table import (
    onboard_single_table,
    step_generate_skeleton,
    step_update_dataset_yaml,
)


@pytest.fixture
def catalog_dir(tmp_path):
    """Create a temp catalog with _dataset.yaml files."""
    for layer in ("kpi", "data"):
        d = tmp_path / "catalog" / layer
        d.mkdir(parents=True)
        dataset = {
            "dataset": {
                "name": f"{{{layer}_dataset}}",
                "layer": layer,
                "description": f"Test {layer} dataset",
                "tables": ["existing_table"],
            }
        }
        (d / "_dataset.yaml").write_text(yaml.dump(dataset, default_flow_style=False))

    return tmp_path / "catalog"


@pytest.fixture
def schema_dir(tmp_path):
    """Create a temp schema dir with a sample JSON."""
    d = tmp_path / "schemas" / "data"
    d.mkdir(parents=True)
    import json

    schema = [
        {"name": "trade_date", "type": "DATE", "mode": "NULLABLE", "description": ""},
        {"name": "symbol", "type": "STRING", "mode": "NULLABLE", "description": ""},
        {"name": "price", "type": "FLOAT", "mode": "NULLABLE", "description": ""},
    ]
    (d / "new_table.json").write_text(json.dumps(schema))
    return tmp_path / "schemas"


def _write_enriched_yaml(catalog_dir: Path, layer: str, table: str):
    """Write a fully-enriched table YAML."""
    data = {
        "table": {
            "name": table,
            "dataset": f"{{{layer}_dataset}}",
            "fqn": f"{{project}}.{{{layer}_dataset}}.{table}",
            "layer": layer,
            "description": f"Test table {table}",
            "partition_field": "trade_date",
            "columns": [
                {
                    "name": "trade_date",
                    "type": "DATE",
                    "description": "Date of trade",
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
                    "name": "price",
                    "type": "FLOAT",
                    "description": "Trade price",
                    "category": "measure",
                    "typical_aggregation": "AVG",
                    "source": "proto::price",
                },
            ],
        }
    }
    path = catalog_dir / layer / f"{table}.yaml"
    path.write_text(yaml.dump(data, default_flow_style=False))


class TestStepGenerateSkeleton:
    def test_skip_if_exists(self, catalog_dir):
        _write_enriched_yaml(catalog_dir, "data", "existing")
        with patch("onboard_table.CATALOG_DIR", catalog_dir):
            result = step_generate_skeleton("data", "existing")
        assert result is False

    def test_dry_run_no_write(self, catalog_dir, schema_dir):
        with (
            patch("onboard_table.CATALOG_DIR", catalog_dir),
            patch("generate_skeleton.CATALOG_DIR", catalog_dir),
            patch("generate_skeleton.SCHEMA_DIR", schema_dir),
        ):
            result = step_generate_skeleton("data", "new_table", dry_run=True)

        assert result is True
        assert not (catalog_dir / "data" / "new_table.yaml").exists()


class TestStepUpdateDatasetYaml:
    def test_adds_new_table(self, catalog_dir):
        with patch("onboard_table.CATALOG_DIR", catalog_dir):
            result = step_update_dataset_yaml("data", "new_table")

        assert result is True
        data = yaml.safe_load((catalog_dir / "data" / "_dataset.yaml").read_text())
        assert "new_table" in data["dataset"]["tables"]

    def test_skip_existing(self, catalog_dir):
        with patch("onboard_table.CATALOG_DIR", catalog_dir):
            result = step_update_dataset_yaml("data", "existing_table")

        assert result is False

    def test_dry_run_no_write(self, catalog_dir):
        with patch("onboard_table.CATALOG_DIR", catalog_dir):
            result = step_update_dataset_yaml("data", "new_table", dry_run=True)

        assert result is True
        data = yaml.safe_load((catalog_dir / "data" / "_dataset.yaml").read_text())
        assert "new_table" not in data["dataset"]["tables"]


class TestOnboardSingleTable:
    def test_enrich_only_skips_skeleton(self, catalog_dir):
        _write_enriched_yaml(catalog_dir, "data", "test_table")

        with (
            patch("onboard_table.CATALOG_DIR", catalog_dir),
            patch("check_coverage.CATALOG_DIR", catalog_dir),
            patch("enrich_categories.main", return_value={}),
            patch("enrich_aggregation.main", return_value={}),
            patch("enrich_source.main", return_value={}),
            patch("enrich_related.main", return_value={}),
        ):
            result = onboard_single_table(
                "data", "test_table", enrich_only=True, dry_run=True
            )

        assert "skeleton" not in result

    def test_dry_run_no_writes(self, catalog_dir, schema_dir):
        with (
            patch("onboard_table.CATALOG_DIR", catalog_dir),
            patch("generate_skeleton.CATALOG_DIR", catalog_dir),
            patch("generate_skeleton.SCHEMA_DIR", schema_dir),
            patch("check_coverage.CATALOG_DIR", catalog_dir),
            patch("enrich_categories.main", return_value={}),
            patch("enrich_aggregation.main", return_value={}),
            patch("enrich_source.main", return_value={}),
            patch("enrich_related.main", return_value={}),
        ):
            onboard_single_table("data", "new_table", dry_run=True)

        # Skeleton should not have been written
        assert not (catalog_dir / "data" / "new_table.yaml").exists()

    def test_full_pipeline_existing_table(self, catalog_dir):
        _write_enriched_yaml(catalog_dir, "data", "test_table")

        with (
            patch("onboard_table.CATALOG_DIR", catalog_dir),
            patch("check_coverage.CATALOG_DIR", catalog_dir),
            patch("enrich_categories.main", return_value={}),
            patch("enrich_aggregation.main", return_value={}),
            patch("enrich_source.main", return_value={}),
            patch("enrich_related.main", return_value={}),
        ):
            result = onboard_single_table("data", "test_table", enrich_only=True)

        assert result["coverage"]["passed"] is True

    def test_idempotent_rerun(self, catalog_dir):
        """Running twice produces consistent results."""
        _write_enriched_yaml(catalog_dir, "data", "test_table")

        patches = [
            patch("onboard_table.CATALOG_DIR", catalog_dir),
            patch("check_coverage.CATALOG_DIR", catalog_dir),
            patch("enrich_categories.main", return_value={}),
            patch("enrich_aggregation.main", return_value={}),
            patch("enrich_source.main", return_value={}),
            patch("enrich_related.main", return_value={}),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            r1 = onboard_single_table("data", "test_table", enrich_only=True)
            r2 = onboard_single_table("data", "test_table", enrich_only=True)

        assert r1["coverage"]["passed"] == r2["coverage"]["passed"]
        assert r1["coverage"]["coverage"] == r2["coverage"]["coverage"]
