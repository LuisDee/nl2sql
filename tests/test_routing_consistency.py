"""Tests for routing rule consolidation and drift detection.

Verifies that:
1. load_routing_rules() returns structured data from all YAML sources
2. All routing sources (YAML, prompts, embeddings) stay in sync
3. Adding a table to YAML propagates to the generated prompt
4. Embedding descriptions are driven by YAML (no hardcoded drift)
"""

import yaml

from nl2sql_agent.catalog_loader import (
    CATALOG_DIR,
    load_routing_rules,
    load_yaml,
)
from nl2sql_agent.config import settings


class TestLoadRoutingRules:
    """Tests for the load_routing_rules() function."""

    def test_returns_dict_with_expected_keys(self) -> None:
        rules = load_routing_rules()
        assert isinstance(rules, dict)
        assert "kpi_routing" in rules
        assert "data_routing" in rules
        assert "cross_cutting" in rules

    def test_kpi_routing_has_entries(self) -> None:
        rules = load_routing_rules()
        kpi = rules["kpi_routing"]
        assert isinstance(kpi, list)
        assert len(kpi) >= 5  # at least 5 KPI tables

    def test_data_routing_has_entries(self) -> None:
        rules = load_routing_rules()
        data = rules["data_routing"]
        assert isinstance(data, list)
        assert len(data) >= 7  # at least 7 data tables

    def test_routing_entry_has_required_fields(self) -> None:
        rules = load_routing_rules()
        for entry in rules["kpi_routing"]:
            assert "patterns" in entry
            assert "table" in entry or "action" in entry
            assert isinstance(entry["patterns"], list)

    def test_cross_cutting_has_routing_descriptions(self) -> None:
        rules = load_routing_rules()
        cc = rules["cross_cutting"]
        assert isinstance(cc, dict)
        assert "kpi_vs_data_general" in cc
        assert "theodata_routing" in cc

    def test_all_kpi_tables_present(self) -> None:
        rules = load_routing_rules()
        kpi_tables = {e["table"] for e in rules["kpi_routing"] if "table" in e}
        expected = {
            "markettrade",
            "quotertrade",
            "brokertrade",
            "clicktrade",
            "otoswing",
        }
        assert expected.issubset(kpi_tables)

    def test_all_data_tables_present(self) -> None:
        rules = load_routing_rules()
        data_tables = {e["table"] for e in rules["data_routing"] if "table" in e}
        expected = {
            "theodata",
            "marketdata",
            "marketdepth",
            "swingdata",
            "markettrade",
            "quotertrade",
            "clicktrade",
        }
        assert expected.issubset(data_tables)


class TestRoutingDrift:
    """Drift detection: all routing sources must stay in sync."""

    def test_all_kpi_yaml_tables_in_prompt(self) -> None:
        """Every KPI table with routing rules must appear in the generated prompt."""
        from nl2sql_agent.prompts import _static_instruction

        rules = load_routing_rules()
        prompt = _static_instruction()

        for entry in rules["kpi_routing"]:
            if "table" in entry:
                assert entry["table"] in prompt, (
                    f"KPI table '{entry['table']}' has routing rules in YAML "
                    f"but is missing from the generated prompt"
                )

    def test_all_data_yaml_tables_in_prompt(self) -> None:
        """Every data table with routing rules must appear in the generated prompt."""
        from nl2sql_agent.prompts import _static_instruction

        rules = load_routing_rules()
        prompt = _static_instruction()

        for entry in rules["data_routing"]:
            if "table" in entry:
                assert entry["table"] in prompt, (
                    f"Data table '{entry['table']}' has routing rules in YAML "
                    f"but is missing from the generated prompt"
                )

    def test_routing_yaml_tables_match_dataset_yaml(self) -> None:
        """Tables in _dataset.yaml routing must match actual table YAML files."""
        kpi_ds = load_yaml(CATALOG_DIR / "kpi" / "_dataset.yaml")
        data_ds = load_yaml(CATALOG_DIR / "data" / "_dataset.yaml")

        kpi_routing_tables = {
            r["table"]
            for r in kpi_ds.get("dataset", {}).get("routing", [])
            if "table" in r
        }
        data_routing_tables = {
            r["table"]
            for r in data_ds.get("dataset", {}).get("routing", [])
            if "table" in r
        }

        # Check KPI table YAML files exist
        kpi_yamls = {
            f.stem
            for f in (CATALOG_DIR / "kpi").glob("*.yaml")
            if not f.name.startswith("_")
        }
        for table in kpi_routing_tables:
            assert table in kpi_yamls, (
                f"KPI routing references '{table}' but no catalog/kpi/{table}.yaml exists"
            )

        # Check data table YAML files exist
        data_yamls = {
            f.stem
            for f in (CATALOG_DIR / "data").glob("*.yaml")
            if not f.name.startswith("_")
        }
        for table in data_routing_tables:
            if table == "brokertrade":
                continue  # Known: brokertrade.yaml doesn't exist in data/
            assert table in data_yamls, (
                f"Data routing references '{table}' but no catalog/data/{table}.yaml exists"
            )


class TestEmbeddingDrift:
    """Drift detection: embedding descriptions must come from YAML."""

    def test_build_table_descriptions_covers_all_kpi_tables(self) -> None:
        """Every KPI table YAML must appear in embedding descriptions."""
        from scripts.run_embeddings import _build_table_descriptions

        descs = _build_table_descriptions(settings)
        kpi_tables = {
            d["table_name"]
            for d in descs
            if d["layer"] == "kpi" and d["source_type"] == "table"
        }
        kpi_yamls = {
            f.stem
            for f in (CATALOG_DIR / "kpi").glob("*.yaml")
            if not f.name.startswith("_")
        }
        assert kpi_yamls == kpi_tables, (
            f"Mismatch between KPI YAMLs and embedding descriptions: "
            f"missing={kpi_yamls - kpi_tables}, extra={kpi_tables - kpi_yamls}"
        )

    def test_build_table_descriptions_covers_all_data_tables(self) -> None:
        """Every enriched data table YAML must appear in embedding descriptions."""
        from scripts.run_embeddings import _build_table_descriptions

        descs = _build_table_descriptions(settings)
        data_tables = {
            d["table_name"]
            for d in descs
            if d["layer"] == "data" and d["source_type"] == "table"
        }
        # Only check tables that have non-empty descriptions (enriched tables)
        data_yamls = set()
        for f in (CATALOG_DIR / "data").glob("*.yaml"):
            if f.name.startswith("_"):
                continue
            content = yaml.safe_load(f.read_text())
            desc = content.get("table", {}).get("description", "").strip()
            if desc:
                data_yamls.add(f.stem)
        assert data_yamls == data_tables, (
            f"Mismatch between data YAMLs and embedding descriptions: "
            f"missing={data_yamls - data_tables}, extra={data_tables - data_yamls}"
        )

    def test_build_table_descriptions_includes_dataset_level(self) -> None:
        """Each layer must have a dataset-level description row."""
        from scripts.run_embeddings import _build_table_descriptions

        descs = _build_table_descriptions(settings)
        dataset_rows = [d for d in descs if d["source_type"] == "dataset"]
        layers = {d["layer"] for d in dataset_rows}
        assert "kpi" in layers, "Missing KPI dataset-level description"
        assert "data" in layers, "Missing data dataset-level description"

    def test_build_routing_descriptions_non_empty(self) -> None:
        """Routing descriptions from _routing.yaml must be non-empty."""
        from scripts.run_embeddings import _build_routing_descriptions

        descs = _build_routing_descriptions()
        assert len(descs) >= 3, (
            f"Expected at least 3 routing descriptions, got {len(descs)}"
        )
        for desc in descs:
            assert len(desc) > 50, f"Routing description too short: {desc[:50]}..."

    def test_table_descriptions_have_content(self) -> None:
        """Every table description must be non-empty."""
        from scripts.run_embeddings import _build_table_descriptions

        descs = _build_table_descriptions(settings)
        for d in descs:
            assert d["description"], (
                f"Empty description for {d['layer']}/{d['table_name'] or 'dataset'}"
            )
