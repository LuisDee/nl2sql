"""Validate YAML catalog columns against BQ schema ground truth JSONs.

The committed schemas/*.json files are extracted from BigQuery INFORMATION_SCHEMA
and serve as the source of truth for column existence. This test catches:
- Hallucinated columns: YAML defines a column that doesn't exist in BQ
- Missing columns: BQ has a column that YAML omits (warning only)
"""

import json
import warnings
from pathlib import Path

import pytest
import yaml

SCHEMA_DIR = Path(__file__).parent.parent / "schemas"
CATALOG_DIR = Path(__file__).parent.parent / "catalog"

# All table/layer pairs that have both a schema JSON and a catalog YAML
TABLE_PAIRS = [
    ("kpi", "brokertrade"),
    ("kpi", "clicktrade"),
    ("kpi", "markettrade"),
    ("kpi", "otoswing"),
    ("kpi", "quotertrade"),
    ("data", "clicktrade"),
    ("data", "markettrade"),
    ("data", "swingdata"),
    ("data", "quotertrade"),
    ("data", "theodata"),
    ("data", "marketdata"),
    ("data", "marketdepth"),
]


class TestSchemaDrift:
    """YAML catalog columns must match BigQuery INFORMATION_SCHEMA."""

    @pytest.mark.parametrize("layer,table", TABLE_PAIRS)
    def test_yaml_columns_match_bq_schema(self, layer, table):
        """YAML catalog columns must exist in the BQ schema (no hallucinations)."""
        schema_path = SCHEMA_DIR / layer / f"{table}.json"
        if not schema_path.exists():
            pytest.skip(f"No schema JSON: {schema_path}")

        yaml_path = CATALOG_DIR / layer / f"{table}.yaml"
        if not yaml_path.exists():
            pytest.skip(f"No catalog YAML: {yaml_path}")

        with open(schema_path) as f:
            bq_schema = json.load(f)
        bq_cols = {col["name"] for col in bq_schema}

        with open(yaml_path) as f:
            yaml_data = yaml.safe_load(f)
        yaml_cols = {
            col["name"]
            for col in yaml_data["table"]["columns"]
            if isinstance(col, dict) and "name" in col
        }

        hallucinated = yaml_cols - bq_cols
        missing = bq_cols - yaml_cols

        assert not hallucinated, (
            f"{layer}/{table}: YAML has columns not in BQ schema: {sorted(hallucinated)}"
        )

        if missing:
            warnings.warn(
                f"{layer}/{table}: BQ has {len(missing)} columns not in YAML "
                f"(intentional omissions or new columns): {sorted(missing)[:10]}...",
                stacklevel=2,
            )

    @pytest.mark.parametrize("layer,table", TABLE_PAIRS)
    def test_schema_json_exists(self, layer, table):
        """Every expected table must have a committed schema JSON."""
        schema_path = SCHEMA_DIR / layer / f"{table}.json"
        assert schema_path.exists(), f"Missing BQ schema ground truth: {schema_path}"

    @pytest.mark.parametrize("layer,table", TABLE_PAIRS)
    def test_schema_json_is_valid(self, layer, table):
        """Schema JSONs must be valid and non-empty."""
        schema_path = SCHEMA_DIR / layer / f"{table}.json"
        if not schema_path.exists():
            pytest.skip(f"No schema JSON: {schema_path}")

        with open(schema_path) as f:
            schema = json.load(f)

        assert isinstance(schema, list), "Schema must be a JSON array"
        assert len(schema) > 0, "Schema must have at least one column"
        for col in schema:
            assert "name" in col, f"Column missing 'name' key: {col}"
            assert "type" in col, f"Column missing 'type' key: {col}"
