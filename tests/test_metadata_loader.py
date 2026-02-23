"""Tests for the YAML metadata loader tool."""

from pathlib import Path
from unittest.mock import patch

from nl2sql_agent.tools.metadata_loader import (
    _discover_table_yaml_map,
    _resolve_yaml_path,
    load_yaml_metadata,
)


class TestResolveYamlPath:
    def test_resolves_kpi_table_with_dataset(self):
        path = _resolve_yaml_path("markettrade", "nl2sql_omx_kpi")
        assert path == "kpi/markettrade.yaml"

    def test_resolves_data_table_with_dataset(self):
        path = _resolve_yaml_path("theodata", "nl2sql_omx_data")
        assert path == "data/theodata.yaml"

    def test_resolves_unique_table_without_dataset(self):
        path = _resolve_yaml_path("theodata")
        assert path == "data/theodata.yaml"

    def test_disambiguates_quotertrade(self):
        kpi_path = _resolve_yaml_path("quotertrade", "nl2sql_omx_kpi")
        data_path = _resolve_yaml_path("quotertrade", "nl2sql_omx_data")
        assert kpi_path == "kpi/quotertrade.yaml"
        assert data_path == "data/quotertrade.yaml"

    def test_returns_none_for_unknown_table(self):
        path = _resolve_yaml_path("nonexistent_table")
        assert path is None

    def test_case_insensitive_match(self):
        path = _resolve_yaml_path("TheOData")
        assert path == "data/theodata.yaml"

    def test_resolves_brazil_kpi_via_registry(self):
        """Exchange registry lookup: Brazil KPI dataset."""
        path = _resolve_yaml_path("markettrade", "nl2sql_brazil_kpi")
        assert path == "kpi/markettrade.yaml"

    def test_resolves_asx_data_via_registry(self):
        """Exchange registry lookup: ASX data dataset resolves to market dir."""
        path = _resolve_yaml_path("theodata", "nl2sql_asx_data")
        assert path == "asx_data/theodata.yaml"

    def test_resolves_unknown_dataset_via_suffix_heuristic(self):
        """Suffix heuristic: nl2sql_newexchange_kpi → kpi."""
        path = _resolve_yaml_path("markettrade", "nl2sql_newexchange_kpi")
        assert path == "kpi/markettrade.yaml"


class TestDiscoverTableYamlMap:
    """_discover_table_yaml_map should scan catalog dirs dynamically."""

    def test_discovers_kpi_tables(self):
        table_map = _discover_table_yaml_map()
        # brokertrade and otoswing are KPI-only
        assert "brokertrade" in table_map
        assert table_map["brokertrade"] == "kpi/brokertrade.yaml"
        assert "otoswing" in table_map
        assert table_map["otoswing"] == "kpi/otoswing.yaml"

    def test_discovers_data_tables(self):
        table_map = _discover_table_yaml_map()
        assert "theodata" in table_map
        assert table_map["theodata"] == "data/theodata.yaml"
        assert "marketdepth" in table_map
        assert table_map["marketdepth"] == "data/marketdepth.yaml"

    def test_skips_dataset_yaml(self):
        """_dataset.yaml files should not be in the map."""
        table_map = _discover_table_yaml_map()
        assert "_dataset" not in table_map

    def test_discovers_all_unique_tables(self):
        table_map = _discover_table_yaml_map()
        # 5 kpi + 7 data = 12 files, 3 overlap → 9 unique keys
        assert len(table_map) >= 9

    def test_shared_tables_resolved_by_dataset(self):
        """Tables in both layers (e.g. markettrade) are in map;
        disambiguation uses _dataset_to_layer, not the map."""
        table_map = _discover_table_yaml_map()
        # markettrade exists in both — map has one entry (data wins)
        assert "markettrade" in table_map
        # _resolve_yaml_path with dataset still works for both
        assert (
            _resolve_yaml_path("markettrade", "nl2sql_omx_kpi")
            == "kpi/markettrade.yaml"
        )
        assert (
            _resolve_yaml_path("markettrade", "nl2sql_omx_data")
            == "data/markettrade.yaml"
        )


class TestLoadYamlMetadata:
    def test_returns_error_for_unknown_table(self):
        result = load_yaml_metadata("fake_table_xyz", "")
        assert result["status"] == "error"
        assert "No metadata found" in result["error_message"]

    def test_returns_error_when_file_missing(self):
        """Even if mapping exists, the file might not."""
        with patch(
            "nl2sql_agent.tools.metadata_loader.CATALOG_DIR",
            Path("/tmp/nonexistent_catalog"),  # noqa: S108
        ):
            result = load_yaml_metadata("markettrade", "nl2sql_omx_kpi")
            assert result["status"] == "error"
            assert "not found" in result["error_message"].lower()

    def test_returns_metadata_string_for_valid_table(self, tmp_path):
        """Test with a real YAML file in a temp directory."""
        kpi_dir = tmp_path / "kpi"
        kpi_dir.mkdir()
        yaml_content = (
            "table:\n"
            "  name: markettrade\n"
            "  dataset: nl2sql_omx_kpi\n"
            "  fqn: '{project}.nl2sql_omx_kpi.markettrade'\n"
            "  layer: kpi\n"
            "  description: KPI metrics for market trades\n"
            "  partition_field: trade_date\n"
            "  columns:\n"
            "    - name: edge_bps\n"
            "      type: FLOAT64\n"
            "      description: Edge in basis points\n"
        )
        (kpi_dir / "markettrade.yaml").write_text(yaml_content)

        with patch("nl2sql_agent.tools.metadata_loader.CATALOG_DIR", tmp_path):
            result = load_yaml_metadata("markettrade", "nl2sql_omx_kpi")

        assert result["status"] == "success"
        assert "edge_bps" in result["metadata"]
        assert isinstance(result["metadata"], str)  # Must be string, not dict

    def test_includes_dataset_context_for_kpi(self, tmp_path):
        """KPI tables should include _dataset.yaml context."""
        kpi_dir = tmp_path / "kpi"
        kpi_dir.mkdir()
        (kpi_dir / "markettrade.yaml").write_text(
            "table:\n  name: markettrade\n  dataset: nl2sql_omx_kpi\n"
            "  fqn: x\n  layer: kpi\n  description: test\n"
            "  partition_field: trade_date\n  columns: []\n"
        )
        (kpi_dir / "_dataset.yaml").write_text(
            "dataset:\n  name: nl2sql_omx_kpi\n  routing:\n"
            "    - patterns: [edge]\n      table: markettrade\n"
        )

        with patch("nl2sql_agent.tools.metadata_loader.CATALOG_DIR", tmp_path):
            result = load_yaml_metadata("markettrade", "nl2sql_omx_kpi")

        assert result["status"] == "success"
        assert "_dataset_context" in result["metadata"]
