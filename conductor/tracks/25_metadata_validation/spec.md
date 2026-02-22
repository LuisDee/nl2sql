# Spec: Metadata Validation & Drift Detection

## Overview

Builds permanent validation infrastructure that catches metadata drift between the YAML catalog, BQ schemas, and source repos. Prevents future hallucination by establishing automated cross-referencing at multiple levels.

**Dependencies:** Track 24 (Metadata Extraction), Track 23 (Source Repo Discovery)

## Validation Layers

1. **YAML vs BQ INFORMATION_SCHEMA** — every column in YAML exists in BQ, flag columns in BQ missing from YAML
2. **YAML vs source repos** — formulas in YAML match KPI repo computations, descriptions match proto comments
3. **Examples vs catalog** — all column references in few-shot SQL examples exist in the catalog
4. **Routing/prompt vs catalog** — all column names in routing docs and prompts exist in the catalog
5. **Schema diff script** — developer-facing tool for periodic reconciliation (`scripts/validate_schemas.py`)

## Detailed spec to be written after Track 24 is complete.
