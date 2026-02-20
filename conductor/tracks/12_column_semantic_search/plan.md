# Track 12: Column-Level Semantic Search — Implementation Plan

## Phase 1: Fix Known Issues & Prepare Infrastructure

### [x] Task 1.1: Fix `edge_bps` phantom column in prompts and docs `c0610f0`

The column `edge_bps` does not exist. The real column is `instant_edge` (`edge_bps` is a synonym in the YAML catalog). Fix all incorrect references:

**Files to fix:**
- `nl2sql_agent/prompts.py:85` — change `"edge (edge_bps)"` to `"edge (instant_edge)"`
- `docs/HOW_IT_ALL_WORKS.md` — all references to `edge_bps` as a column name
- `docs/NL2SQL_Agent_Overview.md` — fake YAML example showing `name: edge_bps`

### [x] Task 1.2: Add `column_search_top_k` setting `43c0785`

**File:** `nl2sql_agent/config.py`

Add:
```python
column_search_top_k: int = Field(
    default=30,
    description="Number of column-level results from vector search. Over-retrieves to let LLM filter.",
)
column_search_max_per_table: int = Field(
    default=15,
    description="Maximum columns returned per table from column search.",
)
```

### [x] Task 1.3: Enrich column embedding text in populate pipeline `dec3860`

**File:** `scripts/populate_embeddings.py`

Currently the `description` field is just the YAML column description. Enrich it to include table context and synonyms for better embedding quality:

**Current embedded text:** `"Instantaneous edge at the moment of trade"`

**New `embedding_text` field:**
```
"markettrade.instant_edge (FLOAT, kpi): Instantaneous edge (theoretical profit per unit) at the moment of trade execution. Also known as: edge, trading edge, edge_bps, capture, theoretical edge"
```

**Changes:**
- Add `embedding_text` column to `column_embeddings` table schema (in `run_embeddings.py`)
- Build enriched text in `populate_column_embeddings()`: `"{table}.{col} ({type}, {layer}): {desc}. Also known as: {synonyms}"`
- Update `generate_embeddings()` to embed `embedding_text` instead of `description`
- Existing `description` and `synonyms` columns remain unchanged (used for display)

### [x] Task 1.4: Write unit tests for enriched text generation `dec3860`

**File:** `tests/test_populate_embeddings.py` (new or extend existing)

- `test_embedding_text_includes_table_name` — enriched text contains `markettrade.instant_edge`
- `test_embedding_text_includes_synonyms` — enriched text contains synonym list
- `test_embedding_text_includes_layer` — enriched text contains `kpi` or `data`
- `test_embedding_text_handles_no_synonyms` — columns with empty synonyms don't have "Also known as:" suffix

---

## Phase 2: Column Search SQL Template & Tool

### Task 2.1: Write the column search SQL template

**File:** `nl2sql_agent/tools/vector_search.py`

New SQL template `_COLUMN_SEARCH_SQL`:

```sql
WITH question_embedding AS (
    SELECT ml_generate_embedding_result AS embedding
    FROM ML.GENERATE_EMBEDDING(
        MODEL `{embedding_model}`,
        (SELECT @question AS content),
        STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output)
    )
),
column_matches AS (
    SELECT
        base.dataset_name,
        base.table_name,
        base.column_name,
        base.column_type,
        base.description,
        base.synonyms,
        ROUND(distance, 4) AS distance
    FROM VECTOR_SEARCH(
        (SELECT * FROM `{metadata_dataset}.column_embeddings`),
        'embedding',
        (SELECT embedding FROM question_embedding),
        top_k => {column_top_k},
        distance_type => 'COSINE'
    )
),
table_scores AS (
    SELECT
        dataset_name,
        table_name,
        MIN(distance) AS best_column_distance,
        COUNT(*) AS matching_columns,
        ARRAY_AGG(
            STRUCT(column_name, column_type, description, synonyms, distance)
            ORDER BY distance
            LIMIT {max_per_table}
        ) AS top_columns
    FROM column_matches
    GROUP BY dataset_name, table_name
),
example_results AS (
    SELECT
        base.question AS past_question,
        base.sql_query,
        base.tables_used,
        base.dataset AS past_dataset,
        base.complexity,
        base.routing_signal,
        ROUND(distance, 4) AS distance
    FROM VECTOR_SEARCH(
        (SELECT * FROM `{metadata_dataset}.query_memory`),
        'embedding',
        (SELECT embedding FROM question_embedding),
        top_k => {example_top_k},
        distance_type => 'COSINE'
    )
)
SELECT
    'column_search' AS search_type,
    t.*
FROM table_scores t
ORDER BY best_column_distance ASC
LIMIT {table_limit}
```

**Key design choices:**
- Single `question_embedding` CTE shared across column search and query_memory search (one embedding generation)
- `ARRAY_AGG(...ORDER BY distance LIMIT N)` returns top N columns per table
- `MIN(distance)` per table = effective table ranking from column evidence
- `COUNT(*)` = secondary ranking signal (more matching columns = stronger signal)
- Query_memory search remains in the same CTE (reuses the embedding)

### Task 2.2: Implement `vector_search_columns` tool function

**File:** `nl2sql_agent/tools/vector_search.py`

New function:
```python
def vector_search_columns(question: str) -> dict:
    """Find relevant tables and columns for a natural language question.

    Searches column-level embeddings to find the most relevant columns,
    then aggregates by table to determine which tables to query. This
    gives both table routing AND column selection in one step.

    Returns tables ranked by their best column match, with top columns
    per table including names, types, descriptions, and synonyms.
    Also pre-fetches few-shot examples (cached for fetch_few_shot_examples).

    Falls back to schema_embeddings (table-level) search if column
    embeddings are empty or unavailable.
    """
```

**Return format:**
```python
{
    "status": "success",
    "tables": [
        {
            "dataset_name": "nl2sql_omx_kpi",
            "table_name": "markettrade",
            "best_column_distance": 0.0823,
            "matching_columns": 5,
            "top_columns": [
                {
                    "column_name": "instant_edge",
                    "column_type": "FLOAT",
                    "description": "Instantaneous edge...",
                    "synonyms": ["edge", "trading edge", ...],
                    "distance": 0.0823
                },
                ...
            ]
        },
        ...
    ],
    "examples": [...]  # Few-shot examples (cached)
}
```

**Fallback logic:**
- If column search raises an exception (table doesn't exist, no embeddings), fall back to the existing `vector_search_tables` (schema_embeddings) flow
- Log the fallback for observability

### Task 2.3: Write unit tests for column search

**File:** `tests/test_column_search.py` (new)

- `test_column_search_returns_tables_with_columns` — mock BQ response, verify table aggregation
- `test_column_search_tables_ranked_by_best_distance` — verify MIN(distance) ordering
- `test_column_search_caches_examples` — verify few-shot cache populated
- `test_column_search_falls_back_to_schema` — if column search fails, falls back to table-level
- `test_column_search_top_columns_limited` — verify max_per_table limit applied
- `test_column_search_uses_correct_sql_params` — verify @question param injected safely

---

## Phase 3: Wire Into Agent & Update Prompt

### Task 3.1: Update agent tool list

**File:** `nl2sql_agent/agent.py`

Replace or augment the tool list:
- Add `vector_search_columns` to tools
- Keep `vector_search_tables` available (used by fallback path)
- Keep `load_yaml_metadata` available (opt-in for full schema access)

**Decision: replace `vector_search_tables` in the default flow?**

Option A: Replace — `vector_search_columns` becomes the primary search tool, with `vector_search_tables` called internally as fallback only (not exposed to agent).

Option B: Add alongside — both tools available, prompt guides the agent to try `vector_search_columns` first.

**Recommendation: Option A** — simpler for the agent, fewer decisions. The fallback logic lives inside `vector_search_columns`.

### Task 3.2: Update system prompt

**File:** `nl2sql_agent/prompts.py`

Update the tool usage order:
```
0. check_semantic_cache — Check if this exact question was answered before
1. vector_search_columns — Find relevant tables AND columns via semantic search
2. (OPTIONAL) load_yaml_metadata — Only if you need full schema, business rules,
   or preferred timestamps not covered by column search results
3. fetch_few_shot_examples — Find similar past validated queries for reference
4. Write the SQL using column descriptions + examples as context
5. dry_run_sql — Validate syntax and estimate cost
6. execute_sql — Run the validated query and return results
```

Update the SQL generation rules:
```
- Use the column names from vector_search_columns results.
  The top_columns include exact column names, types, descriptions, and synonyms.
- If you need columns not returned by the search (e.g., trade_date for filtering),
  call load_yaml_metadata for the full schema.
- Do not guess column names — if unsure, search or load metadata first.
```

Fix `edge_bps` references (Task 1.1).

### Task 3.3: Update `__init__.py` exports

**File:** `nl2sql_agent/tools/__init__.py`

Add `vector_search_columns` to exports.

### Task 3.4: Update tests for changed tool list

**File:** `tests/test_agent_init.py`

Update `test_agent_has_correct_tools` (or equivalent) to reflect new tool list.

**File:** `tests/test_mcp_server.py`

If tool count changed, update `TOOL_PROGRESS_MESSAGES` in `mcp_server.py` and related tests.

---

## Phase 4: Embedding Pipeline Updates

### Task 4.1: Add `embedding_text` column to table schema

**File:** `scripts/run_embeddings.py`

In the `create_tables()` function, add `embedding_text STRING` to the `column_embeddings` table schema. This is the enriched text that gets embedded (separate from the display `description`).

For existing deployments: use `ALTER TABLE ADD COLUMN IF NOT EXISTS` instead of `CREATE OR REPLACE` to avoid data loss.

### Task 4.2: Build enriched text in populate pipeline

**File:** `scripts/populate_embeddings.py`

In `populate_column_embeddings()`, build the `embedding_text` field:

```python
# Build enriched text for embedding (not the display description)
parts = [f"{table_name}.{col_name} ({col_type}, {layer})"]
if description:
    parts.append(description)
if synonyms:
    parts.append(f"Also known as: {', '.join(synonyms)}")
embedding_text = ": ".join(parts[:1]) + ". " + ". ".join(parts[1:])
```

Include `embedding_text` in the MERGE statement.

### Task 4.3: Update embedding generation to use enriched text

**File:** `scripts/run_embeddings.py`

In `generate_embeddings()`, change column embeddings UPDATE to:

```sql
UPDATE `{fqn}.column_embeddings` t
SET embedding = (
  SELECT ml_generate_embedding_result
  FROM ML.GENERATE_EMBEDDING(
    MODEL `{model}`,
    (SELECT COALESCE(t.embedding_text, t.description) AS content),
    STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_DOCUMENT' AS task_type)
  )
)
WHERE ARRAY_LENGTH(t.embedding) = 0;
```

Uses `COALESCE(embedding_text, description)` for backwards compatibility — existing rows without `embedding_text` still work.

### Task 4.4: Document re-embedding requirement

After this track, ALL column embeddings need regeneration because the embedded text has changed. Document the steps:

```bash
# 1. Re-populate with enriched text
python scripts/populate_embeddings.py

# 2. Force re-embedding by clearing existing embeddings
# (populate_embeddings.py MERGE sets embedding=[] on update)

# 3. Generate new embeddings
python scripts/run_embeddings.py --step generate-embeddings
```

---

## Phase 5: Documentation & Integration

### Task 5.1: Update HOW_IT_ALL_WORKS.md

- Update the flow diagram (Part 4) to show column search instead of table-only search
- Add explanation of two-tier retrieval and why it's better
- Update the "Summary: The Whole Thing in One Picture" diagram
- Fix all `edge_bps` references
- Add notes about re-running embedding pipeline after this track

### Task 5.2: Update AGENTS.md files

- `nl2sql_agent/tools/AGENTS.md` — document new tool and updated flow
- `nl2sql_agent/AGENTS.md` — update tool count and data flow

### Task 5.3: Update conductor/tracks.md

Add Track 12 entry, mark as complete when done.

### Task 5.4: Add MCP progress message for new tool

**File:** `nl2sql_agent/mcp_server.py`

Add to `TOOL_PROGRESS_MESSAGES`:
```python
"vector_search_columns": "Searching tables and columns...",
```

Update/remove `vector_search_tables` entry if it's no longer directly exposed.

---

## Phase 6: Verification

### Task 6.1: Run full unit test suite

```bash
pytest tests/ -v
```

All existing tests must pass (with updates from Phase 3).

### Task 6.2: Integration test with live BQ

```bash
pytest -m integration tests/integration/ -v
```

### Task 6.3: Manual verification (after embedding pipeline run on target project)

1. Re-run embedding pipeline:
   ```bash
   python scripts/populate_embeddings.py
   python scripts/run_embeddings.py --step generate-embeddings
   ```

2. Test routing rescue cases:
   - "what is the rho exposure?" → should find theodata (via column `rho`)
   - "NHR adjustment on trades" → should find markettrade (via `nhr_adjustment_bid`)
   - "pricing model latency" → should find theodata (via `theo_compute_timestamp_*`)
   - "average edge today" → should find markettrade (via `instant_edge` synonyms)

3. Test that returned columns are relevant (not 774 random columns)

4. Test fallback: if column_embeddings table doesn't exist, agent still works via schema_embeddings

---

## Summary

| Phase | Tasks | Key Outcome |
|-------|-------|-------------|
| 1 | Fix phantom `edge_bps`, add settings, enrich embedding text | Infrastructure ready |
| 2 | SQL template + tool function + tests | Column search works |
| 3 | Wire into agent, update prompt, update exports | Agent uses column search |
| 4 | Pipeline updates for enriched embeddings | Better embedding quality |
| 5 | Documentation, AGENTS.md, MCP progress | Everything documented |
| 6 | Full test suite + manual verification | Ship it |
