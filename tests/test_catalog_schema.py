"""Tests for catalog/schema.py Pydantic validation models.

TDD Red Phase: These tests define the expected behaviour of the
ColumnSchema, TableSchema, and DatasetSchema models before implementation.
"""

import pytest

from catalog.schema import ColumnSchema, DatasetSchema, TableSchema


class TestColumnSchema:
    """ColumnSchema validates individual column definitions."""

    def test_minimal_column_valid(self):
        """A column with only required fields (name, type, description) is valid."""
        col = ColumnSchema(
            name="trade_date", type="DATE", description="The trade date."
        )
        assert col.name == "trade_date"
        assert col.type == "DATE"

    def test_fully_enriched_column_valid(self):
        """A column with all enrichment fields is valid."""
        col = ColumnSchema(
            name="instant_edge",
            type="FLOAT",
            description="Instantaneous edge at trade execution.",
            category="measure",
            typical_aggregation="AVG",
            filterable=True,
            example_values=["0.5", "1.2", "-0.3"],
            comprehensive=False,
            formula="refTv adjusted with delta/gamma corrections",
            related_columns=["instant_pnl", "instant_pnl_w_fees"],
            synonyms=["edge", "trading edge"],
            source="MarketTrade.proto::VtCommon.edge",
            business_rules="Always present on every KPI row.",
        )
        assert col.category == "measure"
        assert col.typical_aggregation == "AVG"

    def test_category_must_be_valid_enum(self):
        """Category must be one of: dimension, measure, time, identifier."""
        with pytest.raises(ValueError, match="Input should be"):
            ColumnSchema(
                name="foo",
                type="STRING",
                description="test",
                category="invalid_category",
            )

    def test_all_four_categories_accepted(self):
        """All four category values are accepted."""
        for cat in ("dimension", "measure", "time", "identifier"):
            col = ColumnSchema(
                name="test", type="STRING", description="test", category=cat
            )
            assert col.category == cat

    def test_typical_aggregation_must_be_valid_enum(self):
        """typical_aggregation must be a recognized aggregation function."""
        with pytest.raises(ValueError, match="Input should be"):
            ColumnSchema(
                name="foo",
                type="FLOAT",
                description="test",
                category="measure",
                typical_aggregation="INVALID_AGG",
            )

    def test_all_aggregation_values_accepted(self):
        """All recognized aggregation values are accepted."""
        for agg in ("SUM", "AVG", "WEIGHTED_AVG", "COUNT", "MIN", "MAX"):
            col = ColumnSchema(
                name="test",
                type="FLOAT",
                description="test",
                category="measure",
                typical_aggregation=agg,
            )
            assert col.typical_aggregation == agg

    def test_typical_aggregation_rejected_on_non_measure(self):
        """typical_aggregation is only valid when category is 'measure'."""
        with pytest.raises(ValueError, match="typical_aggregation.*only valid"):
            ColumnSchema(
                name="foo",
                type="STRING",
                description="test",
                category="dimension",
                typical_aggregation="SUM",
            )

    def test_typical_aggregation_allowed_without_category(self):
        """typical_aggregation without category is allowed (pre-enrichment state)."""
        col = ColumnSchema(
            name="foo",
            type="FLOAT",
            description="test",
            typical_aggregation="SUM",
        )
        assert col.typical_aggregation == "SUM"

    def test_comprehensive_rejected_without_example_values(self):
        """comprehensive flag requires example_values to be present."""
        with pytest.raises(ValueError, match="comprehensive.*requires example_values"):
            ColumnSchema(
                name="foo",
                type="STRING",
                description="test",
                comprehensive=True,
            )

    def test_comprehensive_accepted_with_example_values(self):
        """comprehensive is valid when example_values is present."""
        col = ColumnSchema(
            name="trade_side",
            type="STRING",
            description="Buy or sell.",
            example_values=["BUY", "SELL"],
            comprehensive=True,
        )
        assert col.comprehensive is True

    def test_synonyms_as_list(self):
        """synonyms field accepts a list of strings."""
        col = ColumnSchema(
            name="algo",
            type="STRING",
            description="Trading algorithm.",
            synonyms=["strategy", "algo name"],
        )
        assert col.synonyms == ["strategy", "algo name"]

    def test_synonyms_empty_list(self):
        """synonyms field accepts an empty list."""
        col = ColumnSchema(name="algo", type="STRING", description="test", synonyms=[])
        assert col.synonyms == []

    def test_related_columns_max_five(self):
        """related_columns is capped at 5 entries."""
        with pytest.raises(ValueError, match="related_columns.*max is 5"):
            ColumnSchema(
                name="foo",
                type="STRING",
                description="test",
                related_columns=["a", "b", "c", "d", "e", "f"],
            )

    def test_example_values_max_ten(self):
        """example_values is capped at 10 entries."""
        with pytest.raises(ValueError, match="example_values.*max is 10"):
            ColumnSchema(
                name="foo",
                type="STRING",
                description="test",
                example_values=[str(i) for i in range(11)],
            )


class TestTableSchema:
    """TableSchema validates table-level YAML structure."""

    def _make_column(self, **overrides):
        defaults = {"name": "col1", "type": "STRING", "description": "A column."}
        defaults.update(overrides)
        return defaults

    def test_minimal_table_valid(self):
        """A table with all required fields is valid."""
        table = TableSchema(
            name="markettrade",
            dataset="{kpi_dataset}",
            fqn="{project}.{kpi_dataset}.markettrade",
            layer="kpi",
            description="Market trades.",
            partition_field="trade_date",
            columns=[self._make_column()],
        )
        assert table.name == "markettrade"

    def test_table_with_optional_fields(self):
        """A table with cluster_fields, business_context, etc. is valid."""
        table = TableSchema(
            name="markettrade",
            dataset="{kpi_dataset}",
            fqn="{project}.{kpi_dataset}.markettrade",
            layer="kpi",
            description="Market trades.",
            business_context="All market trades.",
            partition_field="trade_date",
            cluster_fields=["symbol", "portfolio"],
            row_count_approx=1000000,
            columns=[self._make_column()],
            preferred_timestamps={
                "primary": "event_timestamp_ns",
                "fallback": [],
                "notes": "Use event_timestamp_ns.",
            },
        )
        assert table.cluster_fields == ["symbol", "portfolio"]

    def test_table_missing_required_field_fails(self):
        """Table without a required field raises ValidationError."""
        with pytest.raises(ValueError, match="Field required"):
            TableSchema(
                name="markettrade",
                # missing dataset, fqn, layer, description, partition_field, columns
            )

    def test_layer_must_be_kpi_or_data(self):
        """layer must be 'kpi' or 'data'."""
        with pytest.raises(ValueError, match="Input should be"):
            TableSchema(
                name="test",
                dataset="ds",
                fqn="fqn",
                layer="invalid",
                description="test",
                partition_field="trade_date",
                columns=[self._make_column()],
            )

    def test_columns_validated_as_column_schemas(self):
        """Columns in a table are validated as ColumnSchema instances."""
        table = TableSchema(
            name="test",
            dataset="ds",
            fqn="fqn",
            layer="kpi",
            description="test",
            partition_field="trade_date",
            columns=[
                {
                    "name": "instant_edge",
                    "type": "FLOAT",
                    "description": "Edge.",
                    "category": "measure",
                    "typical_aggregation": "AVG",
                }
            ],
        )
        assert table.columns[0].category == "measure"

    def test_invalid_column_in_table_fails(self):
        """A table with an invalid column raises ValidationError."""
        with pytest.raises(ValueError, match="Input should be"):
            TableSchema(
                name="test",
                dataset="ds",
                fqn="fqn",
                layer="kpi",
                description="test",
                partition_field="trade_date",
                columns=[
                    {
                        "name": "bad",
                        "type": "STRING",
                        "description": "x",
                        "category": "bogus",
                    }
                ],
            )


class TestDatasetSchema:
    """DatasetSchema validates _dataset.yaml structure."""

    def test_minimal_dataset_valid(self):
        """A dataset with required fields is valid."""
        ds = DatasetSchema(
            name="{kpi_dataset}",
            layer="kpi",
            description="KPI dataset.",
            tables=["markettrade", "quotertrade"],
        )
        assert ds.name == "{kpi_dataset}"

    def test_dataset_missing_required_field_fails(self):
        """Dataset without required field raises ValidationError."""
        with pytest.raises(ValueError, match="Field required"):
            DatasetSchema(name="test")
