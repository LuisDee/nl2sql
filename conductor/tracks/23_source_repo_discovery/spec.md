# Spec: Source Repo Discovery & Profiling Framework

## Overview

Builds the infrastructure for LLM agents to systematically explore, document, and reference 4 source repositories that contain the ground truth for our trading data schema. The output is a structured knowledge base that future tracks (metadata extraction, validation) can consume without re-exploring the repos from scratch.

### Problem

1. **Hallucinated metadata:** The YAML catalog was LLM-generated from proto file excerpts and KPI documentation. The autopsy found hallucinated column names (`edge_bps`, `delta_bucket`, `required_edge_bps`) and unverified formulas/descriptions across 4,631 columns.
2. **No source of truth link:** The catalog has no connection to the actual source code that defines the data. When a description says "mid_price * signed_delta * multiplier" there's no way to verify that against the KPI computation code.
3. **4 massive repos:** CPP (22K files), data-library (397 files), data-loader (166 files), KPI (706 files). Dumping these into context is impractical. We need structural indexes that let agents navigate surgically.

### Solution

A three-phase approach based on 2025-2026 research (ICSE hierarchical summarization, Aider repo-map pattern, DocAgent validation):

1. **Repo profiling** — generate structured "repo cards" for each repo (purpose, tech stack, directory map, key entry points, data models, gotchas)
2. **Structural indexing** — deterministic extraction of proto field definitions, transformation mappings, and KPI computation signatures without LLM involvement
3. **Cross-repo routing** — a single routing guide mapping data concepts to repo locations, plus end-to-end field lineage (proto → silver → gold)

## Source Repositories

| Repo | Path | Files | Language | Purpose |
|------|------|-------|----------|---------|
| cpp | `repos/cpp/` | 22,861 | C++ | Raw proto definitions in `source/pb/`. Defines all streamed data types. Our data is a subset of the full proto schema. |
| data-library | `repos/data-library/` | 397 | Go | Consumes proto-streamed data using defined rules. Handles deserialization and initial processing. |
| data-loader | `repos/data-loader/` | 166 | Python/SQL | Enriches data from bronze → silver layer. Column renames, type casts, derived columns. |
| kpi | `repos/kpi/` | 706 | Python/SQL | Generates gold-layer KPI datasets. Computes instant_edge, instant_pnl, delta_slippage_*, etc. |

## Functional Requirements

### FR-1: Repo Cards (one per repo)

Each repo gets a structured profile stored as `repos/{repo}/AGENTS.md`:

```markdown
# Repo: {name}
## Purpose: One-liner
## Tech Stack: languages, frameworks, key libs
## Directory Map: top 2-3 levels with annotations
## Key Entry Points: where the important logic lives
## Data Models: core schemas/tables/protobufs
## Common Patterns: how things are structured
## Gotchas: non-obvious stuff (generated code, vendored deps, etc.)
## When to Use: "reach for this repo when you need X, Y, Z"
```

**Generation method:** Sub-agent per repo with clean context. Agent reads: directory tree (top 3 levels), README/config files, 5-10 representative files from different areas. Returns structured profile.

### FR-2: Proto Field Index

Deterministic extraction from `repos/cpp/source/pb/`:

- Parse all `.proto` files to extract: message names, field names, field types, field numbers, comments
- Output: `metadata/proto_fields.yaml` — structured YAML with one entry per message/field
- Map proto message names to BQ table names (e.g., `MarketTrade` proto → `markettrade` BQ table)
- Include proto comments as raw descriptions (no LLM interpretation)

**Format:**
```yaml
messages:
  - name: MarketTrade
    file: source/pb/markettrade.proto
    bq_table: markettrade  # mapped manually or by convention
    fields:
      - name: trade_id
        type: int64
        number: 1
        comment: "Unique trade identifier"
      - name: price
        type: double
        number: 2
        comment: ""
```

### FR-3: Data Loader Transformation Index

Extract column transformations from `repos/data-loader/`:

- Identify SQL files, Python transformation scripts, config files that define bronze → silver mappings
- For each target table, document: source columns, renames, type casts, derived columns, filters
- Output: `metadata/data_loader_transforms.yaml`

**Format:**
```yaml
tables:
  - target: markettrade
    source_proto: MarketTrade
    transformations:
      - target_column: trade_date
        source: trade_timestamp
        transform: "DATE(trade_timestamp)"
      - target_column: event_timestamp_ns
        source: event_timestamp
        transform: "TIMESTAMP_MICROS(event_timestamp)"
```

### FR-4: KPI Computation Index

Extract computation logic from `repos/kpi/`:

- Identify SQL files, Python scripts that compute KPI columns
- For each KPI column, document: formula/SQL expression, input columns, aggregation type
- Output: `metadata/kpi_computations.yaml`

**Format:**
```yaml
tables:
  - name: markettrade
    computations:
      - column: instant_edge
        formula: "(trade_price - mid_price) * signed_delta * multiplier"
        inputs: [trade_price, mid_price, signed_delta, multiplier]
        aggregation: SUM
      - column: instant_pnl
        formula: "instant_edge * contract_size"
        inputs: [instant_edge, contract_size]
        aggregation: SUM
```

### FR-5: Cross-Repo Routing Guide

A single markdown file at `metadata/ROUTING.md` that tells agents which repo to consult:

```markdown
# Source Repo Routing Guide

## By Data Concept
- Proto field definitions (raw types, field numbers) → repos/cpp/source/pb/
- Data consumption rules (deserialization, Go handlers) → repos/data-library/
- Bronze → silver transformations (renames, casts, derived cols) → repos/data-loader/
- KPI computations (formulas, aggregations) → repos/kpi/

## By BQ Table
- markettrade (data layer) → data-loader for transforms, cpp for proto
- markettrade (KPI layer) → kpi for computations, data-loader for base columns
- theodata → data-loader (unique to data layer, no KPI equivalent)
...

## Cross-Repo Dependencies
- cpp defines proto → data-library consumes proto → data-loader transforms to silver → kpi computes gold
- Proto field names may differ from BQ column names (data-loader handles renames)
```

### FR-6: Field Lineage Map

End-to-end column lineage from proto to gold:

- Output: `metadata/field_lineage.yaml`
- For key columns (instant_edge, instant_pnl, delta_slippage_*, trade_date, etc.), trace: proto field name → silver column name → gold column name + formula

**Format:**
```yaml
lineage:
  - gold_column: instant_edge
    gold_table: kpi.markettrade
    formula: "(trade_price - mid_price) * signed_delta * multiplier"
    silver_column: null  # computed in KPI, not in silver
    silver_table: data.markettrade
    inputs:
      - silver: trade_price
        proto: price
        proto_message: MarketTrade
      - silver: mid_price
        proto: mid_price
        proto_message: MarketTrade
```

## Non-Functional Requirements

- All structural indexes use deterministic extraction (tree-sitter, grep, AST parsing) — not LLM generation — for field names, types, and signatures
- LLM used only for: repo card prose, mapping ambiguous proto-to-BQ names, summarizing transformation intent
- Each sub-agent gets a clean context window (no cross-contamination between repos)
- All outputs committed to `metadata/` directory (version controlled, reviewable)
- Human review checkpoint after each phase before proceeding

## Acceptance Criteria

1. All 4 repos have `AGENTS.md` repo cards with all required sections
2. `metadata/proto_fields.yaml` contains all proto messages from `source/pb/` with fields, types, comments
3. `metadata/data_loader_transforms.yaml` documents column transformations for all silver-layer tables
4. `metadata/kpi_computations.yaml` documents formulas for all KPI columns
5. `metadata/ROUTING.md` provides a complete routing guide
6. `metadata/field_lineage.yaml` traces key columns end-to-end
7. Validation tests confirm all index files are well-formed and cross-reference correctly

## Out of Scope

- Populating YAML catalog metadata fields (Track 24)
- Validating catalog against BQ INFORMATION_SCHEMA (Track 25)
- Enriching embeddings with extracted metadata (Track 22)
- Modifying any source repo code
