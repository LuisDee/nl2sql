# Track 02: Context Layer — Specification

## Overview

Build the Two-Layer Metadata system that gives the NL2SQL agent context about what trading data means. Layer 1 is a static YAML catalog (in-repo) describing every table, column, and routing rule. Layer 2 is BigQuery embedding tables for semantic vector search at query time.

**Critical design change from the original plan**: All project IDs, dataset names, connection strings, and embedding model references are **parameterized** via `nl2sql_agent.config.settings` (loaded from `.env`). Nothing is hardcoded to a specific GCP project. This ensures switching between dev (`melodic-stone-437916-t3`) and production (`cloud-data-n-base-d4b3`) requires only changing the `.env` file.

---

## Functional Requirements

### FR1: YAML Catalog (Layer 1) — 16 files

**16 YAML files** describing the data estate:
- 2 dataset metadata files: `catalog/kpi/_dataset.yaml`, `catalog/data/_dataset.yaml`
- 1 cross-dataset routing file: `catalog/_routing.yaml`
- 13 table files (5 KPI + 8 data), one per table

Each table YAML contains: table name, dataset, layer, rich description, partition/cluster fields, columns with types/descriptions/synonyms, and business rules. **The `fqn` field is NOT hardcoded** — it stores `{dataset}.{table}` and the loader resolves the full `project.dataset.table` at runtime using `settings.gcp_project`.

Dataset YAMLs contain shared columns, routing patterns, and disambiguation rules.

### FR2: Example Queries — 3 files, 30+ examples

**3 example YAML files** with validated Q→SQL pairs:
- `examples/kpi_examples.yaml` (15+ covering all 5 KPI tables)
- `examples/data_examples.yaml` (10+ covering theodata, marketdata, marketdepth, swingdata)
- `examples/routing_examples.yaml` (5+ testing cross-dataset disambiguation)

**SQL in examples uses `{project}.{dataset}.{table}` template variables** that are resolved at load time from settings. Example SQL stored in YAML uses the format:
```sql
SELECT ... FROM `{project}.nl2sql_omx_kpi.markettrade` WHERE trade_date = '2026-02-17'
```
The catalog_loader resolves `{project}` → `settings.gcp_project` when loading examples.

### FR3: Catalog Loader (`nl2sql_agent/catalog_loader.py`)

Python module for loading and validating YAML files:
- `load_yaml(path)` — safe YAML loading
- `validate_table_yaml(data)` — structural validation
- `validate_dataset_yaml(data)` — structural validation
- `validate_examples_yaml(data)` — structural validation (no hardcoded project checks)
- `load_all_table_yamls()` — load all 13 table YAMLs
- `load_all_examples()` — load all examples
- `resolve_fqn(table_yaml, settings)` — build `project.dataset.table` from config
- `resolve_example_sql(sql, settings)` — replace `{project}` placeholder in SQL

No BigQuery imports. No GCP credentials needed. Pure YAML + validation.

### FR4: Embedding Infrastructure (SQL scripts → Python runner)

Instead of raw SQL scripts with hardcoded project IDs, create a **Python runner** (`scripts/run_embeddings.py`) that:
1. Reads config from `settings`
2. Generates parameterized SQL using f-strings with `settings.gcp_project`, `settings.metadata_dataset`, `settings.embedding_model_ref`
3. Executes each step in sequence via `BigQueryProtocol`

Steps:
1. Create metadata dataset (`nl2sql_metadata`)
2. Create 3 embedding tables (`schema_embeddings`, `column_embeddings`, `query_memory`)
3. Populate schema embeddings from YAML descriptions
4. Populate column embeddings from YAML column descriptions
5. Populate query memory from example YAMLs
6. Generate vector embeddings via `ML.GENERATE_EMBEDDING`
7. Create vector indexes
8. Run 5 validation test queries

All SQL uses `settings.gcp_project`, `settings.metadata_dataset`, `settings.embedding_model_ref` — never hardcoded values.

### FR5: Populate Script (`scripts/populate_embeddings.py`)

Reads YAML catalog + examples, inserts into BQ embedding tables via `BigQueryProtocol`. Uses `MERGE` for idempotency. All table references from `settings`.

### FR6: Tests

- `tests/test_yaml_catalog.py` — validates all 16 YAML files exist and pass structural validation
- `tests/test_catalog_loader.py` — unit tests for loader functions (no BQ needed)
- Tests do NOT check for hardcoded project names. They validate structure only.

---

## Non-Functional Requirements

- **Parameterization**: Zero hardcoded GCP project IDs, connection strings, or model references in any code or YAML
- **Idempotency**: All SQL scripts use `CREATE OR REPLACE`, `MERGE`, or `DELETE+INSERT`. Re-running never duplicates data
- **Protocol DI**: All BigQuery access through `BigQueryProtocol`
- **Config**: All config via `from nl2sql_agent.config import settings`
- **YAML safety**: Always `yaml.safe_load()`, never `yaml.load()`
- **Embedding conventions**: `RETRIEVAL_DOCUMENT` task_type for stored content, `RETRIEVAL_QUERY` for search queries. COSINE distance.

---

## Acceptance Criteria

- [ ] 16 YAML catalog files exist and pass validation
- [ ] Every table YAML has columns populated from real BQ schema inspection
- [ ] 3 example YAML files with 30+ total examples
- [ ] All example SQL uses `{project}` placeholder (not hardcoded project)
- [ ] All example SQL filters on `trade_date`
- [ ] `catalog_loader.py` resolves FQNs from settings at runtime
- [ ] `nl2sql_metadata` dataset exists in BQ (dev project)
- [ ] 3 embedding tables exist with non-NULL embeddings
- [ ] 3 vector indexes created
- [ ] 5/5 vector search test cases pass
- [ ] `pytest tests/test_yaml_catalog.py tests/test_catalog_loader.py -v` passes
- [ ] `scripts/populate_embeddings.py` runs without errors against dev project
- [ ] Switching `.env` to production project requires zero code changes

---

## Out of Scope

- Agent tool wiring (Track 03)
- System prompts / routing logic (Track 04)
- Evaluation framework (Track 05)
- Production deployment
- Docker compose setup (exists from Track 01)
