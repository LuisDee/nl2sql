"""Pydantic validation models for the YAML catalog schema.

These models define the contract for enriched column/table/dataset metadata.
All new enrichment fields are optional for backwards compatibility.

Consumers:
- tests/test_catalog_validation.py (CI gate)
- scripts/validate_catalog.py (local development)
- scripts/populate_embeddings.py (optional type-safe access)

NOT consumed at runtime by catalog_loader.py — that stays as plain dict access.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, field_validator, model_validator

# -- Enum-like types ----------------------------------------------------------

Category = Literal["dimension", "measure", "time", "identifier"]
Aggregation = Literal["SUM", "AVG", "WEIGHTED_AVG", "COUNT", "MIN", "MAX"]
Layer = Literal["kpi", "data"]


# -- Column schema -------------------------------------------------------------


class ColumnSchema(BaseModel):
    """Validates a single column definition in a table YAML.

    Required fields: name, type, description.
    All enrichment fields are optional for backwards compatibility.
    """

    # Required
    name: str
    type: str
    description: str

    # Enrichment fields (optional)
    category: Category | None = None
    typical_aggregation: Aggregation | None = None
    filterable: bool | None = None
    example_values: list[str | int | float | bool] | None = None
    comprehensive: bool | None = None
    formula: str | None = None
    related_columns: list[str] | None = None

    # Existing optional fields
    synonyms: list[str] | None = None
    source: str | None = None
    business_rules: str | None = None

    @field_validator("example_values")
    @classmethod
    def _cap_example_values(
        cls, v: list[str | int | float | bool] | None
    ) -> list[str | int | float | bool] | None:
        if v is not None and len(v) > 25:
            msg = f"example_values has {len(v)} items, max is 25"
            raise ValueError(msg)
        return v

    @field_validator("related_columns")
    @classmethod
    def _cap_related_columns(cls, v: list[str] | None) -> list[str] | None:
        if v is not None and len(v) > 5:
            msg = f"related_columns has {len(v)} items, max is 5"
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def _cross_field_checks(self) -> ColumnSchema:
        # typical_aggregation only valid on measures (or when category not yet set)
        if (
            self.typical_aggregation is not None
            and self.category is not None
            and self.category != "measure"
        ):
            msg = (
                f"typical_aggregation='{self.typical_aggregation}' is only valid "
                f"when category='measure', got category='{self.category}'"
            )
            raise ValueError(msg)

        # comprehensive requires example_values
        if self.comprehensive is not None and not self.example_values:
            msg = "comprehensive flag requires example_values to be present"
            raise ValueError(msg)

        return self


# -- Table schema --------------------------------------------------------------


class TableSchema(BaseModel):
    """Validates a table-level YAML definition.

    Required fields: name, dataset, fqn, layer, description,
    partition_field, columns.
    """

    # Required
    name: str
    dataset: str
    fqn: str
    layer: Layer
    description: str
    partition_field: str
    columns: list[ColumnSchema]

    # Optional
    business_context: str | None = None
    cluster_fields: list[str] | None = None
    row_count_approx: int | None = None
    preferred_timestamps: dict[str, Any] | None = None


# -- Dataset schema ------------------------------------------------------------


class DatasetSchema(BaseModel):
    """Validates a _dataset.yaml definition.

    Required fields: name, layer, description, tables.
    """

    # Required
    name: str
    layer: Layer
    description: str
    tables: list[str]

    # Optional — flexible dict for trade_taxonomy, shared_columns, etc.
    trade_taxonomy: dict[str, Any] | None = None
    shared_columns: dict[str, Any] | None = None
    enum_reference: dict[str, Any] | None = None
    interval_expansion: dict[str, Any] | None = None
