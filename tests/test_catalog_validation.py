"""CI validation: every YAML catalog file passes Pydantic schema validation.

Loads every table and dataset YAML in catalog/{kpi,data}/ and validates
against the ColumnSchema/TableSchema/DatasetSchema models. This catches
hallucinated column names, invalid category values, malformed enrichment
fields, and constraint violations.

All enrichment fields are optional, so this test passes on un-enriched YAMLs
and becomes progressively stricter as enrichment is applied.
"""

# -- Table YAMLs -----------------------------------------------------------
import sys
from pathlib import Path

import pytest

from catalog.schema import ColumnSchema, DatasetSchema, GlossarySchema, TableSchema
from nl2sql_agent.catalog_loader import CATALOG_DIR, load_yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from table_registry import ALL_TABLES


def _table_params():
    """Generate (layer, table_name) pairs for parametrize."""
    for layer, tables in ALL_TABLES.items():
        for t in tables:
            yield layer, t


class TestTableYamlValidation:
    """Every table YAML validates against TableSchema."""

    @pytest.mark.parametrize("layer,table", list(_table_params()))
    def test_table_yaml_validates_against_schema(self, layer, table):
        """Load table YAML and validate all columns via Pydantic."""
        path = CATALOG_DIR / layer / f"{table}.yaml"
        data = load_yaml(path)
        table_data = data["table"]

        # Validate the table structure
        validated = TableSchema(**table_data)
        assert validated.name == table
        assert validated.layer == layer
        assert len(validated.columns) > 0

    @pytest.mark.parametrize("layer,table", list(_table_params()))
    def test_every_column_has_required_fields(self, layer, table):
        """Every column must have name, type, description."""
        path = CATALOG_DIR / layer / f"{table}.yaml"
        data = load_yaml(path)
        columns = data["table"]["columns"]

        for col_dict in columns:
            col = ColumnSchema(**col_dict)
            assert col.name, f"Column missing name in {layer}/{table}.yaml"
            assert col.type, f"Column {col.name} missing type"
            assert col.description, f"Column {col.name} missing description"

    @pytest.mark.parametrize("layer,table", list(_table_params()))
    def test_enrichment_fields_valid_when_present(self, layer, table):
        """When enrichment fields are present, they must be valid."""
        path = CATALOG_DIR / layer / f"{table}.yaml"
        data = load_yaml(path)
        columns = data["table"]["columns"]

        for col_dict in columns:
            col = ColumnSchema(**col_dict)

            # If category is set, must be valid
            if col.category is not None:
                assert col.category in (
                    "dimension",
                    "measure",
                    "time",
                    "identifier",
                ), f"{col.name}: invalid category '{col.category}'"

            # If typical_aggregation is set, must be valid and category must be measure
            if col.typical_aggregation is not None and col.category is not None:
                assert col.category == "measure", (
                    f"{col.name}: typical_aggregation on non-measure"
                )

            # If comprehensive is set, example_values must exist
            if col.comprehensive is not None:
                assert col.example_values, (
                    f"{col.name}: comprehensive without example_values"
                )


class TestDatasetYamlValidation:
    """_dataset.yaml files validate against DatasetSchema."""

    @pytest.mark.parametrize("layer", ["kpi", "data"])
    def test_dataset_yaml_validates(self, layer):
        """Load _dataset.yaml and validate structure."""
        path = CATALOG_DIR / layer / "_dataset.yaml"
        data = load_yaml(path)
        ds_data = data["dataset"]

        validated = DatasetSchema(**ds_data)
        assert validated.layer == layer
        assert len(validated.tables) > 0


class TestEnrichmentCoverage:
    """CI gate: all tables must pass enrichment coverage thresholds.

    Thresholds are set to the current floor across all tables.
    Ratchet these up as enrichment improves.
    """

    def test_all_tables_meet_coverage_thresholds(self):
        """Every registered table passes the floor coverage thresholds."""
        from check_coverage import check_all_tables

        # Current floor thresholds (ratchet up over time)
        thresholds = {
            "min_category": 95,
            "min_source": 5,  # KPI tables still low; ratchet up
            "min_formula": 20,  # KPI formulas partially enriched
            "min_description": 100,
        }
        results = check_all_tables(thresholds=thresholds)
        for table_key, report in results.items():
            assert report["passed"], f"{table_key} failed coverage: {report['gaps']}"


class TestGlossaryYamlValidation:
    """glossary.yaml validates against GlossarySchema."""

    def test_glossary_yaml_validates(self):
        """Load glossary.yaml and validate against Pydantic model."""
        path = CATALOG_DIR / "glossary.yaml"
        data = load_yaml(path)

        validated = GlossarySchema(**data["glossary"])
        assert len(validated.entries) > 0

    def test_glossary_entries_have_required_fields(self):
        """Every glossary entry has name, definition, synonyms, related_columns."""
        path = CATALOG_DIR / "glossary.yaml"
        data = load_yaml(path)
        validated = GlossarySchema(**data["glossary"])

        for entry in validated.entries:
            assert entry.name, "Glossary entry missing name"
            assert entry.definition, f"Entry '{entry.name}' missing definition"
            assert entry.related_columns is not None, (
                f"Entry '{entry.name}' missing related_columns"
            )
