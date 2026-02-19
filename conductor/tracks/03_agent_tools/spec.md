# Track 03: Agent Tools (Phase C) — Specification

## Overview

Implement the 6 Python tool functions that give the NL2SQL agent its capabilities and wire them into the ADK agent. After this track, the agent can route questions to tables via semantic search, load YAML metadata, retrieve few-shot examples, validate SQL, execute queries, and save validated pairs for future learning.

## Functional Requirements

### FR-1: Vector Search Tables Tool (`vector_search_tables`)
- Accepts a natural language question (string)
- Executes VECTOR_SEARCH against `schema_embeddings` using BigQuery ML embedding with `RETRIEVAL_QUERY` task type
- Returns top-K results (configurable via `settings.vector_search_top_k`) with source_type, layer, dataset_name, table_name, description, distance
- Uses BigQuery parameterised queries (`@question`) to prevent SQL injection
- Returns structured dict with `status` key on success or error

### FR-2: Fetch Few-Shot Examples Tool (`fetch_few_shot_examples`)
- Accepts a natural language question (string)
- Executes VECTOR_SEARCH against `query_memory` using BigQuery ML embedding with `RETRIEVAL_QUERY` task type
- Returns top-K past validated Q→SQL pairs with question, sql_query, tables_used, dataset, complexity, routing_signal, distance
- Uses BigQuery parameterised queries for user input

### FR-3: Load YAML Metadata Tool (`load_yaml_metadata`)
- Accepts table_name and optional dataset_name
- Resolves to the correct YAML file using (dataset, table) lookup for disambiguation
- Returns YAML-formatted string of table metadata including columns, descriptions, synonyms
- Automatically appends dataset context (`_dataset.yaml`) for KPI and data tables
- Handles missing tables gracefully with informative error messages

### FR-4: SQL Dry Run Tool (`dry_run_sql`)
- Accepts a SQL query string
- Validates via BigQuery dry run (syntax, column references, permissions)
- Returns estimated bytes/MB processed on success, or error message on failure

### FR-5: SQL Executor Tool (`execute_sql`)
- Accepts a SQL query string
- Enforces read-only: rejects any query not starting with SELECT or WITH
- Auto-adds LIMIT when not present (configurable via `settings.bq_max_result_rows`, default 1000)
- Returns rows as list of dicts with truncation warning if applicable

### FR-6: Learning Loop Tool (`save_validated_query`)
- Accepts question, sql_query, tables_used (comma-separated), dataset, complexity, routing_signal — all required (no defaults per ADK best practice)
- Inserts into `query_memory` table using parameterised query
- Generates embedding for the new row using `RETRIEVAL_DOCUMENT` task type
- Uses `ARRAY_LENGTH(embedding) = 0` (not IS NULL) for BQ array semantics
- Returns success, partial_success (insert OK but embed failed), or error

### FR-7: Agent Wiring
- All 6 tools wired into `nl2sql_agent` via `tools=[]` list in `agent.py`
- `LiveBigQueryClient` instance created in `agent.py` and injected via `init_bq_service()`
- Agent instruction includes explicit tool usage order and critical SQL rules
- Project ID injected dynamically from `settings.gcp_project` (never hardcoded)

## Non-Functional Requirements

### NFR-1: Protocol-Based Dependency Injection
- All tools access BigQuery through `BigQueryProtocol` (existing in `protocols.py`)
- New `query_with_params()` method added to protocol and `LiveBigQueryClient`
- Module-level DI via `tools/_deps.py` — set once at agent startup
- No tool module imports `from google.cloud import bigquery`

### NFR-2: Structured Logging
- Every tool logs inputs (question preview, SQL preview) and outputs (row count, status)
- Uses existing `get_logger(__name__)` from `logging_config.py`

### NFR-3: Error Handling
- All tools return `dict` with `status` key — never raise exceptions to the LLM
- Error responses include `error_message` with actionable details

### NFR-4: Configuration
- All dataset/model references via `settings.*` (never hardcoded)
- New config fields: `bq_query_timeout_seconds` (30s), `bq_max_result_rows` (1000), `vector_search_top_k` (5)
- Existing fields reused: `embedding_model_ref`, `metadata_dataset`, `gcp_project`

### NFR-5: Security
- User input in SQL uses BigQuery `@param` query parameters (SQL injection prevention)
- Read-only enforcement on execute_sql (SELECT/WITH only)

## Design Decisions

1. **Search Logic**: BigQuery SQL (`VECTOR_SEARCH`) — keeps all search logic in BQ, no separate vector DB
2. **Metadata Path**: Relative via `Path(__file__)` through existing `catalog_loader.py` — portable across environments
3. **Execute Limits**: 1000 rows max — prevents accidental full table scans
4. **Protocol Extension**: Add `query_with_params()` to existing `BigQueryProtocol` rather than creating new protocol/service
5. **No Default Tool Params**: All ADK tool parameters required (no defaults) — ADK best practice for schema discovery
6. **BQ Array Semantics**: Use `ARRAY_LENGTH(col) = 0` not `IS NULL` — BQ arrays are never NULL

## Acceptance Criteria

- [ ] `protocols.py` updated with `query_with_params` method
- [ ] `clients.py` updated with `query_with_params` implementation
- [ ] `tools/` directory with 7 files (init, deps, 5 tools)
- [ ] `config.py` has new query limit fields
- [ ] `agent.py` creates `LiveBigQueryClient` and wires 6 tools
- [ ] All 6 tools return dict with `status` key
- [ ] All 6 tools log via structlog
- [ ] `execute_sql` rejects non-SELECT, auto-adds LIMIT
- [ ] `vector_search_tables` uses `RETRIEVAL_QUERY` task type
- [ ] `save_validated_query` uses `RETRIEVAL_DOCUMENT` task type
- [ ] All SQL uses `settings.*` (no hardcoded project/dataset/model)
- [ ] No tool module imports `from google.cloud import bigquery`
- [ ] All tests pass (Track 01 + 02 + 03)
- [ ] ADK web UI shows tool invocations in trace

## Out of Scope

- Agent prompt engineering / instruction tuning (Track 04)
- Evaluation framework / accuracy benchmarks (Track 05)
- Column-level vector search tool (future enhancement when column_embeddings populated)
- Async tool implementations (sync is sufficient for current scale)
