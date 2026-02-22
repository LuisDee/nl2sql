# Spec: Metadata Extraction from Source Repos

## Overview

Uses the structural indexes from Track 23 to populate the YAML catalog with source-grounded metadata. Each enrichment field (formula, category, typical_aggregation, related_columns, example_values, filterable) is extracted from source code rather than LLM-generated.

**Dependencies:** Track 23 (Source Repo Discovery), Track 18 (YAML Schema Enrichment — defined the fields)

## High-Level Approach

1. **Formula extraction** — for each KPI column, find the computation in the KPI repo using `kpi_computations.yaml` as a guide. Write verified `formula` values into per-table YAML.
2. **Category assignment** — use proto comments + transformation logic + field names to classify columns as measure/dimension/identifier/timestamp.
3. **Relationship mapping** — use `field_lineage.yaml` to populate `related_columns` (e.g., KPI `instant_edge` relates to data-layer `trade_price`, `mid_price`).
4. **Description verification** — cross-check existing YAML descriptions against source code comments and proto comments. Flag and fix hallucinated descriptions.
5. **Gap filling** — for columns with no description, generate from source code context with LLM, validated against structural index.

## Validation

Every extracted field is validated using the **existence ratio** metric: every entity referenced in the enriched YAML must exist in either the BQ schema or the structural indexes from Track 23. Target: >95% existence ratio.

## Detailed spec to be written after Track 23 is complete.
