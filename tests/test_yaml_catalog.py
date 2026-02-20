"""Tests for YAML catalog structure and content validation."""

from pathlib import Path

import pytest

from nl2sql_agent.catalog_loader import (
    CATALOG_DIR,
    EXAMPLES_DIR,
    validate_table_yaml,
    validate_dataset_yaml,
    validate_examples_yaml,
    load_yaml,
    load_all_table_yamls,
    load_all_examples,
)


class TestYamlCatalogStructure:
    """Validate that all YAML files exist and have correct structure."""

    def test_catalog_dir_exists(self):
        """catalog/ directory must exist."""
        assert CATALOG_DIR.exists(), f"Missing: {CATALOG_DIR}"

    def test_kpi_dataset_yaml_exists(self):
        """catalog/kpi/_dataset.yaml must exist."""
        path = CATALOG_DIR / "kpi" / "_dataset.yaml"
        assert path.exists(), f"Missing: {path}"

    def test_data_dataset_yaml_exists(self):
        """catalog/data/_dataset.yaml must exist."""
        path = CATALOG_DIR / "data" / "_dataset.yaml"
        assert path.exists(), f"Missing: {path}"

    def test_routing_yaml_exists(self):
        """catalog/_routing.yaml must exist."""
        path = CATALOG_DIR / "_routing.yaml"
        assert path.exists(), f"Missing: {path}"

    @pytest.mark.parametrize("table", [
        "brokertrade", "clicktrade", "markettrade", "otoswing", "quotertrade",
    ])
    def test_kpi_table_yaml_exists(self, table):
        """Every KPI table must have a YAML file."""
        path = CATALOG_DIR / "kpi" / f"{table}.yaml"
        assert path.exists(), f"Missing: {path}"

    @pytest.mark.parametrize("table", [
        "clicktrade", "markettrade", "quotertrade",
        "theodata", "swingdata", "marketdata", "marketdepth",
    ])
    def test_data_table_yaml_exists(self, table):
        """Every data table must have a YAML file."""
        path = CATALOG_DIR / "data" / f"{table}.yaml"
        assert path.exists(), f"Missing: {path}"


class TestYamlCatalogValidation:
    """Validate YAML content against the required schema."""

    @pytest.mark.parametrize("subdir,table", [
        ("kpi", "markettrade"), ("kpi", "quotertrade"), ("kpi", "brokertrade"),
        ("kpi", "clicktrade"), ("kpi", "otoswing"),
        ("data", "theodata"), ("data", "marketdata"), ("data", "marketdepth"),
        ("data", "swingdata"), ("data", "markettrade"), ("data", "quotertrade"),
        ("data", "clicktrade"),
    ])
    def test_table_yaml_validates(self, subdir, table):
        """Each table YAML must pass structural validation."""
        path = CATALOG_DIR / subdir / f"{table}.yaml"
        if not path.exists():
            pytest.skip(f"{path} not yet created")
        data = load_yaml(path)
        errors = validate_table_yaml(data, str(path))
        assert errors == [], f"Validation errors: {errors}"

    def test_kpi_dataset_yaml_validates(self):
        """KPI dataset YAML must pass structural validation."""
        path = CATALOG_DIR / "kpi" / "_dataset.yaml"
        data = load_yaml(path)
        errors = validate_dataset_yaml(data, str(path))
        assert errors == [], f"Validation errors: {errors}"

    def test_data_dataset_yaml_validates(self):
        """Data dataset YAML must pass structural validation."""
        path = CATALOG_DIR / "data" / "_dataset.yaml"
        data = load_yaml(path)
        errors = validate_dataset_yaml(data, str(path))
        assert errors == [], f"Validation errors: {errors}"

    def test_all_table_yamls_have_columns(self):
        """Every table YAML must have at least 1 column defined."""
        tables = load_all_table_yamls()
        for t in tables:
            table = t["table"]
            assert len(table.get("columns", [])) > 0, (
                f"{table['dataset']}.{table['name']} has no columns"
            )

    def test_all_table_yamls_use_project_placeholder(self):
        """Every table YAML fqn must use {project} placeholder."""
        tables = load_all_table_yamls()
        for t in tables:
            table = t["table"]
            assert "{project}" in table["fqn"], (
                f"{table['name']}: fqn must use {{project}} placeholder, got: {table['fqn']}"
            )


class TestMetadataGaps:
    """Validate Track 10 metadata additions: business_context, preferred_timestamps, trade_taxonomy."""

    @pytest.mark.parametrize("subdir,table", [
        ("kpi", "markettrade"), ("kpi", "quotertrade"), ("kpi", "brokertrade"),
        ("kpi", "clicktrade"), ("kpi", "otoswing"),
        ("data", "theodata"), ("data", "marketdata"), ("data", "marketdepth"),
        ("data", "swingdata"), ("data", "markettrade"), ("data", "quotertrade"),
        ("data", "clicktrade"),
    ])
    def test_table_has_business_context(self, subdir, table):
        """Every table YAML must have a business_context field."""
        path = CATALOG_DIR / subdir / f"{table}.yaml"
        data = load_yaml(path)
        table_data = data["table"]
        assert "business_context" in table_data, (
            f"{subdir}/{table}.yaml missing business_context"
        )
        assert len(table_data["business_context"].strip()) > 10, (
            f"{subdir}/{table}.yaml business_context is too short"
        )

    @pytest.mark.parametrize("subdir,table", [
        ("kpi", "markettrade"), ("kpi", "quotertrade"), ("kpi", "brokertrade"),
        ("kpi", "clicktrade"), ("kpi", "otoswing"),
        ("data", "theodata"), ("data", "marketdata"), ("data", "marketdepth"),
        ("data", "swingdata"), ("data", "markettrade"), ("data", "quotertrade"),
        ("data", "clicktrade"),
    ])
    def test_table_has_preferred_timestamps(self, subdir, table):
        """Every table YAML must have preferred_timestamps with a primary field."""
        path = CATALOG_DIR / subdir / f"{table}.yaml"
        data = load_yaml(path)
        table_data = data["table"]
        assert "preferred_timestamps" in table_data, (
            f"{subdir}/{table}.yaml missing preferred_timestamps"
        )
        ts = table_data["preferred_timestamps"]
        assert "primary" in ts, (
            f"{subdir}/{table}.yaml preferred_timestamps missing 'primary'"
        )
        assert len(ts["primary"]) > 0, (
            f"{subdir}/{table}.yaml preferred_timestamps.primary is empty"
        )

    @pytest.mark.parametrize("subdir", ["kpi", "data"])
    def test_dataset_has_trade_taxonomy(self, subdir):
        """Dataset YAMLs must have a trade_taxonomy section."""
        path = CATALOG_DIR / subdir / "_dataset.yaml"
        data = load_yaml(path)
        ds = data["dataset"]
        assert "trade_taxonomy" in ds, (
            f"{subdir}/_dataset.yaml missing trade_taxonomy"
        )
        taxonomy = ds["trade_taxonomy"]
        assert "description" in taxonomy, (
            f"{subdir}/_dataset.yaml trade_taxonomy missing description"
        )
        assert "markettrade" in taxonomy, (
            f"{subdir}/_dataset.yaml trade_taxonomy missing markettrade entry"
        )


class TestExamplesValidation:
    """Validate example query YAML files."""

    @pytest.mark.parametrize("filename", [
        "kpi_examples.yaml", "data_examples.yaml", "routing_examples.yaml",
    ])
    def test_example_file_exists(self, filename):
        """Each example file must exist."""
        path = EXAMPLES_DIR / filename
        assert path.exists(), f"Missing: {path}"

    @pytest.mark.parametrize("filename", [
        "kpi_examples.yaml", "data_examples.yaml", "routing_examples.yaml",
    ])
    def test_example_file_validates(self, filename):
        """Each example file must pass structural validation."""
        path = EXAMPLES_DIR / filename
        if not path.exists():
            pytest.skip(f"{path} not yet created")
        data = load_yaml(path)
        errors = validate_examples_yaml(data, str(path))
        assert errors == [], f"Validation errors: {errors}"

    def test_at_least_30_examples_total(self):
        """Must have at least 30 validated examples across all files."""
        examples = load_all_examples()
        assert len(examples) >= 30, (
            f"Only {len(examples)} examples. Need at least 30."
        )

    def test_examples_cover_kpi_and_data(self):
        """Examples must cover both KPI and data datasets."""
        examples = load_all_examples()
        datasets = {ex["dataset"] for ex in examples}
        assert "nl2sql_omx_kpi" in datasets, "No KPI examples found"
        assert "nl2sql_omx_data" in datasets, "No data examples found"

    def test_examples_use_project_placeholder_in_sql(self):
        """Every example SQL must use {project} placeholder."""
        examples = load_all_examples()
        for ex in examples:
            assert "{project}" in ex["sql"], (
                f"Example '{ex['question'][:50]}...' must use {{project}} in SQL"
            )

    def test_examples_filter_on_trade_date(self):
        """Every example SQL must filter on trade_date."""
        examples = load_all_examples()
        for ex in examples:
            assert "trade_date" in ex["sql"], (
                f"Example '{ex['question'][:50]}...' must filter on trade_date"
            )
