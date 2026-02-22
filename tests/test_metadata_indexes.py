"""Tests for metadata structural index files (Track 23, Phase 2).

Validates schema and content of:
- metadata/proto_fields.yaml
- metadata/data_loader_transforms.yaml
- metadata/kpi_computations.yaml
"""

from pathlib import Path

import pytest
import yaml

METADATA_DIR = Path(__file__).parent.parent / "metadata"

# Proto files we expect to see for our BQ tables
EXPECTED_PROTO_MESSAGES = {
    "MarketTrade",
    "QuoterTrade",
    "BrokerTrade",
    "SwingData",
    "OroSwingData",
    "MarketEvent",
    "MarketDepth",
    "TheoData",
    "PositionEvent",
    "VtCommon",
    "TradableInstrument",
    "MarketDataExtended",
}

# Tables processed by data-loader
EXPECTED_DATA_LOADER_TABLES = {
    "markettrade",
    "quotertrade",
    "brokertrade",
    "swingdata",
    "oroswingdata",
    "marketdata",
    "marketdataext",
    "marketdepth",
    "theodata",
    "tradedata",
}

# KPI trade types
EXPECTED_KPI_TRADE_TYPES = {
    "markettrade",
    "quotertrade",
    "otoswing",
    "clicktrade",
    "brokertrade",
}


class TestProtoFieldsYaml:
    """Validate metadata/proto_fields.yaml schema and content."""

    @pytest.fixture(autouse=True)
    def load_yaml(self):
        path = METADATA_DIR / "proto_fields.yaml"
        assert path.exists(), f"Missing {path}"
        with open(path) as f:
            self.data = yaml.safe_load(f)

    def test_top_level_is_dict_with_messages_key(self):
        assert isinstance(self.data, dict)
        assert "messages" in self.data

    def test_messages_is_list(self):
        assert isinstance(self.data["messages"], list)
        assert len(self.data["messages"]) > 0

    def test_each_message_has_required_keys(self):
        for msg in self.data["messages"]:
            assert "name" in msg, f"Message missing 'name': {msg}"
            assert "file" in msg, f"Message {msg.get('name')} missing 'file'"
            assert "fields" in msg, f"Message {msg['name']} missing 'fields'"

    def test_each_field_has_required_keys(self):
        for msg in self.data["messages"]:
            for field in msg["fields"]:
                assert "name" in field, f"Field in {msg['name']} missing 'name'"
                assert "type" in field, (
                    f"Field {field.get('name')} in {msg['name']} missing 'type'"
                )
                assert "number" in field, (
                    f"Field {field['name']} in {msg['name']} missing 'number'"
                )

    def test_expected_messages_present(self):
        message_names = {m["name"] for m in self.data["messages"]}
        missing = EXPECTED_PROTO_MESSAGES - message_names
        assert not missing, f"Expected messages not found: {missing}"

    def test_field_numbers_are_positive_integers(self):
        for msg in self.data["messages"]:
            for field in msg["fields"]:
                assert isinstance(field["number"], int) and field["number"] > 0, (
                    f"Invalid field number {field['number']} for {msg['name']}.{field['name']}"
                )

    def test_vtcommon_has_many_fields(self):
        vtcommon = next(
            (m for m in self.data["messages"] if m["name"] == "VtCommon"), None
        )
        assert vtcommon is not None, "VtCommon message not found"
        assert len(vtcommon["fields"]) >= 40, (
            f"VtCommon should have 40+ fields, got {len(vtcommon['fields'])}"
        )

    def test_proto_to_bq_mapping_present(self):
        assert "proto_to_bq" in self.data, "Missing 'proto_to_bq' mapping section"
        mapping = self.data["proto_to_bq"]
        assert isinstance(mapping, dict)
        # At minimum, our core tables should be mapped
        for table in [
            "markettrade",
            "quotertrade",
            "brokertrade",
            "theodata",
            "marketdata",
            "marketdepth",
        ]:
            assert table in mapping, f"Missing proto_to_bq mapping for {table}"


class TestDataLoaderTransformsYaml:
    """Validate metadata/data_loader_transforms.yaml schema and content."""

    @pytest.fixture(autouse=True)
    def load_yaml(self):
        path = METADATA_DIR / "data_loader_transforms.yaml"
        assert path.exists(), f"Missing {path}"
        with open(path) as f:
            self.data = yaml.safe_load(f)

    def test_top_level_is_dict_with_tables_key(self):
        assert isinstance(self.data, dict)
        assert "tables" in self.data

    def test_tables_is_list(self):
        assert isinstance(self.data["tables"], list)
        assert len(self.data["tables"]) > 0

    def test_each_table_has_required_keys(self):
        for table in self.data["tables"]:
            assert "name" in table, f"Table missing 'name': {table}"
            assert "staging_file" in table, (
                f"Table {table.get('name')} missing 'staging_file'"
            )
            assert "columns" in table, f"Table {table['name']} missing 'columns'"

    def test_each_column_has_required_keys(self):
        for table in self.data["tables"]:
            for col in table["columns"]:
                assert "name" in col, f"Column in {table['name']} missing 'name'"
                assert "source_field" in col, (
                    f"Column {col.get('name')} in {table['name']} missing 'source_field'"
                )

    def test_expected_tables_present(self):
        table_names = {t["name"] for t in self.data["tables"]}
        missing = EXPECTED_DATA_LOADER_TABLES - table_names
        assert not missing, f"Expected tables not found: {missing}"

    def test_markettrade_has_key_columns(self):
        mt = next((t for t in self.data["tables"] if t["name"] == "markettrade"), None)
        assert mt is not None
        col_names = {c["name"] for c in mt["columns"]}
        for expected in [
            "trade_price",
            "trade_size",
            "trade_aggressor_side",
            "symbol",
            "instrument_hash",
        ]:
            assert expected in col_names, (
                f"markettrade missing expected column: {expected}"
            )

    def test_marketdepth_has_level_columns(self):
        md = next((t for t in self.data["tables"] if t["name"] == "marketdepth"), None)
        assert md is not None
        col_names = {c["name"] for c in md["columns"]}
        for level in range(5):
            assert f"ask_price_{level}" in col_names, (
                f"marketdepth missing ask_price_{level}"
            )
            assert f"bid_price_{level}" in col_names, (
                f"marketdepth missing bid_price_{level}"
            )


class TestKpiComputationsYaml:
    """Validate metadata/kpi_computations.yaml schema and content."""

    @pytest.fixture(autouse=True)
    def load_yaml(self):
        path = METADATA_DIR / "kpi_computations.yaml"
        assert path.exists(), f"Missing {path}"
        with open(path) as f:
            self.data = yaml.safe_load(f)

    def test_top_level_is_dict_with_trade_types_key(self):
        assert isinstance(self.data, dict)
        assert "trade_types" in self.data

    def test_trade_types_is_list(self):
        assert isinstance(self.data["trade_types"], list)
        assert len(self.data["trade_types"]) > 0

    def test_each_trade_type_has_required_keys(self):
        for tt in self.data["trade_types"]:
            assert "name" in tt, f"Trade type missing 'name': {tt}"
            assert "calculation_file" in tt, (
                f"Trade type {tt.get('name')} missing 'calculation_file'"
            )
            assert "metrics" in tt, f"Trade type {tt['name']} missing 'metrics'"

    def test_each_metric_has_required_keys(self):
        for tt in self.data["trade_types"]:
            for metric in tt["metrics"]:
                assert "name" in metric, f"Metric in {tt['name']} missing 'name'"
                assert "formula" in metric, (
                    f"Metric {metric.get('name')} in {tt['name']} missing 'formula'"
                )

    def test_expected_trade_types_present(self):
        tt_names = {t["name"] for t in self.data["trade_types"]}
        missing = EXPECTED_KPI_TRADE_TYPES - tt_names
        assert not missing, f"Expected trade types not found: {missing}"

    def test_markettrade_has_core_metrics(self):
        mt = next(
            (t for t in self.data["trade_types"] if t["name"] == "markettrade"), None
        )
        assert mt is not None
        metric_names = {m["name"] for m in mt["metrics"]}
        for expected in ["instant_edge", "instant_pnl", "instant_pnl_w_fees"]:
            assert expected in metric_names, (
                f"markettrade missing expected metric: {expected}"
            )

    def test_clicktrade_has_derivation_info(self):
        ct = next(
            (t for t in self.data["trade_types"] if t["name"] == "clicktrade"), None
        )
        assert ct is not None
        assert "source" in ct, "clicktrade should document its source (tradedata)"
        assert "filters" in ct, "clicktrade should document its filter criteria"

    def test_slippage_metrics_exist(self):
        """At least one trade type should have delta_slippage metrics."""
        has_slippage = False
        for tt in self.data["trade_types"]:
            metric_names = {m["name"] for m in tt["metrics"]}
            if any("delta_slippage" in n for n in metric_names):
                has_slippage = True
                break
        assert has_slippage, "No trade type has delta_slippage metrics"

    def test_time_intervals_documented(self):
        assert "time_intervals" in self.data, "Missing 'time_intervals' section"
        intervals = self.data["time_intervals"]
        # time_intervals can be a dict with intraday/multiday keys or a flat list
        if isinstance(intervals, dict):
            intraday = intervals.get("intraday", [])
            multiday = intervals.get("multiday", [])
            total = len(intraday) + len(multiday)
        else:
            total = len(intervals)
        assert total >= 9, f"Expected 9+ intervals, got {total}"
