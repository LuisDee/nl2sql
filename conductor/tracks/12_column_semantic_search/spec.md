# Track 12: Column-Level Semantic Search (Two-Tier Retrieval)

## Problem

The agent currently routes questions to tables using **table-level descriptions only** (17 rows in `schema_embeddings`). These descriptions are hand-written summaries that cannot cover all 4,631 columns across 12 tables. When a trader asks about a concept that exists in a column but isn't mentioned in the table description, routing fails:

| Trader asks... | In table description? | In column metadata? | Result today |
|---|---|---|---|
| "rho exposure" | No | Yes — column `rho` in theodata | **Wrong table** |
| "NHR adjustment" | No | Yes — `nhr_adjustment_bid/ask` in markettrade | **Wrong table** |
| "pricing model latency" | No | Yes — `theo_compute_timestamp_*` | **Wrong table** |
| "time to expiry" / "DTE" | No | Yes — column `tte`, synonyms | **Wrong table** |
| "forward price" | No | Yes — column `forward` in theodata | **Wrong table** |

Additionally, after routing, the agent dumps the **entire YAML catalog** (200-376KB, 800+ columns per KPI table) into the LLM context. This wastes tokens and forces the LLM to find the needle in a haystack.

## Solution

Add **column-level semantic search** using the existing `column_embeddings` BigQuery table. Search columns, aggregate up to tables — one query, both signals.

### Architecture: Single Combined Query (Option A)

Based on industry research (RESQL 2025, TailorSQL VLDB 2025, Azure NL2SQL best practices), we use a single BQ round-trip that:

1. Embeds the question once
2. Runs `VECTOR_SEARCH` against `column_embeddings` (4,631 rows)
3. Aggregates column matches by table → derives table ranking
4. Returns top tables WITH their most relevant columns (not all 800)

This replaces the current `schema_embeddings`-only search and eliminates the need to dump full YAML metadata. The existing `schema_embeddings` search becomes a **fallback** for edge cases.

```
BEFORE (table-only):
  question → embed → VECTOR_SEARCH(schema_embeddings, 17 rows) → table
           → load_yaml_metadata (ALL 774 columns) → LLM picks columns → SQL

AFTER (column-first with table aggregation):
  question → embed → VECTOR_SEARCH(column_embeddings, 4631 rows)
           → aggregate by table → top 5 tables, top 15-20 columns each
           → LLM uses relevant columns only → SQL
           → fallback to schema_embeddings if column search empty
```

### Why not Option B (sequential) or C (parallel)?

Each BQ `VECTOR_SEARCH` call costs 1-2 seconds (includes `ML.GENERATE_EMBEDDING` Vertex AI round-trip). Two calls = 2-4 seconds just for retrieval. Option A gives both signals in a single round-trip.

## Research Backing

- **RESQL (2025)**: Flat index of `table.column: description` chunks, single retrieval, 94.2% accuracy
- **TailorSQL (VLDB 2025, Amazon)**: Separate column documents, aggregated to tables. 2x accuracy improvement
- **Bidirectional Retrieval (2025)**: Table-first + column-first merged. Column-first alone = 82% recall; combined = 90.6%. Column signal rescues missed tables
- **"Death of Schema Linking" (2024)**: Full schema works for small schemas, but with 800+ columns per table, retrieval is essential
- **Azure NL2SQL Best Practices**: Unified index with column-level entries, custom scoring profiles
- **ICLR 2025 (TnT)**: Structure-enriched column embeddings outperform text-only by 14.4% on execution accuracy

## Scope

### In Scope
- Enrich `column_embeddings` text to include table context + synonyms (better embeddings)
- New `vector_search_columns` tool (or modify existing `vector_search_tables`)
- New combined SQL template: column search + table aggregation + query_memory
- Update agent prompt to use column-level results instead of full YAML dump
- New config setting: `column_search_top_k` (default 30)
- Update embedding pipeline to regenerate with enriched text
- Fix `edge_bps` phantom column in prompts.py and documentation
- Unit tests for new tool + updated flow
- Update docs (HOW_IT_ALL_WORKS.md, AGENTS.md)

### Out of Scope
- Removing `load_yaml_metadata` tool entirely (keep as fallback for full schema access)
- Removing `schema_embeddings` table (keep as fallback)
- Changing the embedding model
- Real-time column embedding updates (offline pipeline is sufficient)

## Key Design Decisions

### 1. Enrich embedded text (critical for quality)

Currently `column_embeddings` embeds only `t.description`. Research (RESQL, TailorSQL) shows embedding richer text dramatically improves retrieval:

**Current:** `"Instantaneous edge at the moment of trade"`
**Proposed:** `"markettrade.instant_edge (FLOAT, KPI): Instantaneous edge at the moment of trade. Also known as: edge, trading edge, edge_bps, capture, theoretical edge"`

Format: `"{table}.{column} ({type}, {layer}): {description}. Also known as: {synonyms}"`

### 2. Over-retrieve, don't aggressively filter

Retrieve `top_k=30` columns, aggregate to ~5 tables with ~15-20 columns each. Let the LLM select from this curated set. Per the "Death of Schema Linking" paper: aggressive filtering hurts capable models.

### 3. Keep `load_yaml_metadata` as opt-in fallback

The LLM can still call `load_yaml_metadata` if column search didn't surface enough context (e.g., needs business rules, preferred timestamps, routing hints). But the default path no longer requires it.

### 4. Keep `schema_embeddings` search as safety net

If `column_embeddings` is empty or the search returns no results (e.g., new project before embeddings are generated), fall back to the current table-description-only flow.

## Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `nl2sql_agent/tools/vector_search.py` | Modify | Add column search SQL template, new tool function, update combined search |
| `nl2sql_agent/tools/__init__.py` | Modify | Export new tool |
| `nl2sql_agent/agent.py` | Modify | Wire new tool into agent |
| `nl2sql_agent/prompts.py` | Modify | Update tool usage order, fix `edge_bps` phantom, adjust metadata guidance |
| `nl2sql_agent/config.py` | Modify | Add `column_search_top_k` setting |
| `scripts/run_embeddings.py` | Modify | Enrich column embedding text to include table+synonyms |
| `scripts/populate_embeddings.py` | Modify | Build enriched `embedding_text` field |
| `tests/test_vector_search.py` | Modify | Tests for column search |
| `tests/test_mcp_server.py` | Modify | Update tool count assertion if tool list changes |
| `docs/HOW_IT_ALL_WORKS.md` | Modify | Update flow diagrams, add column search explanation |

## Verification

```bash
# 1. Unit tests pass
pytest tests/ -v

# 2. Column search returns relevant results (integration)
pytest -m integration tests/integration/ -v

# 3. After re-running embedding pipeline on target project:
python scripts/populate_embeddings.py
python scripts/run_embeddings.py --step generate-embeddings

# 4. Manual: ask "what is the rho exposure?" → should route to theodata
# 5. Manual: ask "NHR adjustment" → should find markettrade columns
```

## Dependencies

- Track 09 (production hardening) — complete
- Track 10 (metadata gaps) — complete (enriched YAML with synonyms)
- Column embeddings table must be populated in target project

## Risks

| Risk | Mitigation |
|------|------------|
| Column search returns irrelevant columns | Over-retrieve (top_k=30) + let LLM filter; tune threshold |
| Embedding regeneration needed across projects | Document pipeline steps clearly; `--step generate-embeddings` is idempotent |
| YAML metadata tool becomes orphaned | Keep it — useful for full schema access, business rules, edge cases |
| `ARRAY_LENGTH(embedding) = 0` bug | Fix as part of embedding text enrichment (known issue from autopsy) |
