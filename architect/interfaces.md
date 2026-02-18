# Interface Contracts

> Track-to-track API and event contracts. Each interface has exactly one owner.
> Consumers implement against this specification. Mismatches are INTERFACE_MISMATCH discoveries.
>
> Updated by: `/architect-decompose` (initial), `/architect-sync` (discovery-driven changes)

---

## Agent Tools (Internal Python API)

### 03_generation_core: SQL Generation Service

**Base path:** `services.generation.py` (Class: `SQLGenerator`)

| Method | Path | Description | Request Body | Response | Auth |
|--------|------|-------------|-------------|----------|------|
| `generate_sql` | `generate_sql(query, schema_context)` | Generate SQL from NL | `{ query: str, schema_context: SchemaContext }` | `GeneratedSQL` (Pydantic) | ADK Context |

**Consumed by:** Track 06_agent_integration

### 02_metadata_engine: Metadata Service

**Base path:** `services.metadata.py` (Class: `MetadataCatalog`)

| Method | Path | Description | Request Body | Response | Auth |
|--------|------|-------------|-------------|----------|------|
| `search_schema` | `search_schema(query)` | Semantic search for tables | `{ query: str }` | `List[TableMetadata]` | None |
| `get_table_schema` | `get_table_schema(table_name)` | Get full schema details | `{ table_name: str }` | `TableMetadata` | None |

**Consumed by:** Track 03_generation_core, Track 06_agent_integration

### 04_execution_safety: Execution Service

**Base path:** `services.execution.py` (Class: `SQLExecutor`)

| Method | Path | Description | Request Body | Response | Auth |
|--------|------|-------------|-------------|----------|------|
| `validate_sql` | `validate_sql(sql)` | Dry-run validation | `{ sql: str }` | `ValidationResult` | ADK Context |
| `execute_sql` | `execute_sql(sql)` | Execute query on BigQuery | `{ sql: str }` | `ExecutionResult` | ADK Context |

**Consumed by:** Track 06_agent_integration

### 05_session_memory: Session Service

**Base path:** `services.session.py` (Class: `SessionManager`)

| Method | Path | Description | Request Body | Response | Auth |
|--------|------|-------------|-------------|----------|------|
| `save_interaction` | `save_interaction(query, sql, result)` | Persist history | `{ query, sql, result }` | `None` | ADK Context |
| `get_history` | `get_history(limit)` | Retrieve past context | `{ limit: int }` | `List[Interaction]` | ADK Context |

**Consumed by:** Track 03_generation_core, Track 06_agent_integration

---

## Shared Data Schemas

### TableMetadata

```json
{
  "name": "string — Fully qualified table name",
  "description": "string — Table description for semantic search",
  "columns": "List[ColumnMetadata] — Column definitions",
  "tags": "List[str] — Categorical tags"
}
```

**Owned by:** Track 02_metadata_engine
**Used by:** Track 03_generation_core

### GeneratedSQL

```json
{
  "sql": "string — The generated BigQuery SQL",
  "reasoning": "string — Explanation of the logic",
  "confidence": "float — Confidence score (0-1)"
}
```

**Owned by:** Track 03_generation_core
**Used by:** Track 06_agent_integration

### ExecutionResult

```json
{
  "status": "enum(success|error) — Execution outcome",
  "data": "List[Dict] — Query results (rows)",
  "row_count": "int — Number of rows returned",
  "bytes_processed": "int — Data scanned",
  "error_message": "string — User-friendly error (if failed)"
}
```

**Owned by:** Track 04_execution_safety
**Used by:** Track 06_agent_integration

---

## Contract Change Protocol

When an interface needs to change:

1. Owner proposes change in interfaces.md
2. All consumers listed under the interface are checked:
   - new: auto-inherit via header regeneration
   - in_progress: flag for developer review
   - completed: INTERFACE_MISMATCH discovery → patch phase if needed
3. Breaking changes require developer approval before applying
