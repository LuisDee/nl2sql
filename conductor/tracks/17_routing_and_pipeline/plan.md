# Track 17: Routing Consolidation & Pipeline Testing — Implementation Plan

## Phase 1: Routing Loader

### [x] Task 1.1: Write tests for routing loader (TDD red) `e9bed75`

**Create:** `tests/test_routing_consistency.py`

Tests:
- `load_routing_rules()` returns structured dict with `kpi` and `data` keys
- Each has a list of `{patterns, table, notes}` entries
- Cross-cutting rules loaded from `_routing.yaml` (kpi_vs_data, theodata_only, union_warning)
- All tables from `kpi/_dataset.yaml` routing section present
- All tables from `data/_dataset.yaml` routing section present
- Graceful fallback if `_routing.yaml` missing

**Run:** `pytest tests/test_routing_consistency.py -v` — expect failures.

### [x] Task 1.2: Add load_routing_rules() to catalog_loader.py (TDD green) `e9bed75`

**File:** `nl2sql_agent/catalog_loader.py`

**Add:**
```python
@functools.lru_cache(maxsize=1)
def load_routing_rules() -> dict[str, Any]:
    """Load routing rules from all YAML sources.

    Combines:
    - catalog/_routing.yaml (cross-cutting rules)
    - catalog/kpi/_dataset.yaml (KPI pattern→table)
    - catalog/data/_dataset.yaml (data pattern→table)

    Returns dict with 'cross_cutting', 'kpi_routing', 'data_routing' keys.
    """
```

Also add `clear_routing_cache()` for test isolation.

**Run:** `pytest tests/test_routing_consistency.py -v` — all green.

**Commit:** `feat(catalog): add load_routing_rules() for consolidated routing`

---

## Phase 2: YAML-Driven Prompt Routing

### [x] Task 2.1: Write tests for YAML-driven prompt (TDD red) `7ef5561`

**File:** `tests/test_prompts.py`

Add tests:
- Generated prompt contains all KPI table names from `kpi/_dataset.yaml` routing
- Generated prompt contains all data table names from `data/_dataset.yaml` routing
- Generated prompt contains theodata-only warning
- Generated prompt contains UNION double-counting warning
- If a new table is added to `kpi/_dataset.yaml`, it appears in the prompt

**Run:** `pytest tests/test_prompts.py -v` — new tests fail.

### [x] Task 2.2: Refactor prompts.py routing section (TDD green) `7ef5561`

**File:** `nl2sql_agent/prompts.py`

**Change:** Replace the hardcoded routing rules block (lines ~131-139) with a function that reads from YAML:

```python
def _build_routing_section() -> str:
    """Generate routing rules from YAML catalog sources."""
    rules = load_routing_rules()

    lines = ["## ROUTING RULES (Critical — follow these exactly)", ""]

    # Cross-cutting rules from _routing.yaml
    lines.append(f"1. **Default**: If trade type is unspecified, use `{{kpi}}.markettrade`.")
    lines.append(f"2. **KPI vs Data**: {rules['cross_cutting'].get('kpi_vs_data', '...')}")
    lines.append(f"3. **Theo/Vol/Greeks**: ALWAYS route to `{{data}}.theodata`. ONLY source.")

    # KPI table routing from kpi/_dataset.yaml
    for rule in rules.get("kpi_routing", []):
        patterns = ", ".join(rule["patterns"][:3])
        lines.append(f"- **{rule['table']}**: {patterns}")

    # UNION warning (critical, always include)
    lines.append(rules['cross_cutting'].get('union_warning', ''))

    return "\n".join(lines)
```

**Important:** The generated output must be semantically identical to the current hardcoded text. Diff the old vs new prompt and verify no routing behavior changes.

Cache the routing section (it's static across turns):
```python
@functools.lru_cache(maxsize=1)
def _routing_section() -> str:
    return _build_routing_section()
```

**Run:** `pytest tests/test_prompts.py -v` — all green.

**Commit:** `refactor(prompts): generate routing rules from YAML catalog`

---

## Phase 3: YAML-Driven Embedding Descriptions

### [x] Task 3.1: Refactor run_embeddings.py descriptions `bafa534`

**File:** `scripts/run_embeddings.py`

The `populate_schema_embeddings()` function (lines ~183-256) has hardcoded table descriptions and routing guidance. Replace with YAML-driven generation:

```python
def _build_table_descriptions(s: Settings) -> list[dict]:
    """Build table description rows from YAML catalog."""
    descriptions = []
    for layer in ("kpi", "data"):
        layer_dir = CATALOG_DIR / layer
        for yaml_file in sorted(layer_dir.glob("*.yaml")):
            if yaml_file.name.startswith("_"):
                continue
            content = load_yaml(yaml_file)
            table = content.get("table", {})
            descriptions.append({
                "source_type": "table",
                "layer": layer,
                "dataset_name": getattr(s, f"{layer}_dataset"),
                "table_name": table.get("name", yaml_file.stem),
                "description": table.get("description", ""),
            })
    return descriptions
```

**Also:** Load routing guidance text from `_routing.yaml` instead of hardcoding the multi-line routing string.

**Keep:** The SQL INSERT structure unchanged. Only the data source changes (hardcoded strings → YAML reads).

### [x] Task 3.2: Write drift detection test `bafa534`

**File:** `tests/test_routing_consistency.py`

**Add comprehensive drift test:**

```python
def test_all_tables_in_routing_appear_in_prompt():
    """Every table with routing rules in YAML must appear in the generated prompt."""
    rules = load_routing_rules()
    prompt = build_nl2sql_instruction(mock_context)

    for rule in rules.get("kpi_routing", []):
        assert rule["table"] in prompt, f"KPI table {rule['table']} missing from prompt"
    for rule in rules.get("data_routing", []):
        assert rule["table"] in prompt, f"Data table {rule['table']} missing from prompt"

def test_routing_yaml_and_dataset_yaml_consistent():
    """Cross-cutting rules in _routing.yaml mention the same tables as _dataset.yaml."""
    routing = load_yaml(CATALOG_DIR / "_routing.yaml")
    kpi_dataset = load_yaml(CATALOG_DIR / "kpi" / "_dataset.yaml")
    data_dataset = load_yaml(CATALOG_DIR / "data" / "_dataset.yaml")

    kpi_tables = {r["table"] for r in kpi_dataset.get("routing", [])}
    data_tables = {r["table"] for r in data_dataset.get("routing", [])}

    # All tables mentioned in routing YAML should exist in dataset YAMLs
    # (or the test flags potential drift)
    ...
```

**Commit:** `refactor(embeddings): YAML-driven descriptions, add drift detection`

---

## Phase 4: Embedding Pipeline Tests

### [x] Task 4.1: Write unit tests for run_embeddings.py `96f988e`

**Create:** `tests/test_run_embeddings.py`

Test each pipeline step WITHOUT executing BQ:

**Table creation tests:**
- `create_embedding_tables()` generates valid SQL with `CREATE TABLE IF NOT EXISTS`
- SQL contains expected column definitions (embedding ARRAY<FLOAT64>, etc.)
- `--force` flag switches to `CREATE OR REPLACE TABLE`

**Schema population tests:**
- `populate_schema_embeddings()` generates INSERT SQL with expected structure
- Each row has source_type, layer, dataset_name, table_name, description
- Routing guidance row is included
- All tables from catalog are represented

**Column population tests:**
- `populate_column_embeddings()` generates INSERT SQL for columns
- Column descriptions include synonyms
- Correct number of columns extracted from YAML

**Symbol population tests:**
- `populate_symbols()` reads CSV correctly
- MERGE SQL uses correct table structure
- Batching works (4,806 rows split into batches)

**Embedding generation tests:**
- SQL template contains `WHERE t.embedding IS NULL OR ARRAY_LENGTH(t.embedding) = 0`
  (after Track 13 fix)
- `RETRIEVAL_DOCUMENT` task type used for stored content
- All 3 tables (schema, column, query_memory) processed

**Approach:** Mock `get_bq_service()` and capture the SQL strings passed to it. Assert on structure, not exact SQL (use `in` and regex, not exact string match).

### [x] Task 4.2: Write unit tests for populate_embeddings.py `96f988e`

**Create:** `tests/test_populate_embeddings.py`

Similar approach: mock BQ client, verify SQL generation and row structure for the populate script.

**Run:** `pytest tests/test_run_embeddings.py tests/test_populate_embeddings.py -v` — all green.

**Commit:** `test: add unit tests for embedding pipeline (run_embeddings, populate_embeddings)`

---

## Phase 5: Verification

### [x] Task 5.1: Verify prompt output unchanged

**Manual check:**
1. Generate the prompt with the old hardcoded code (save to file)
2. Generate the prompt with the new YAML-driven code (save to file)
3. Diff them — should be semantically identical (whitespace/ordering changes OK)

### [x] Task 5.2: Run full test suite

**Command:** `pytest tests/ -v`

**Expected:** 380+ tests passing (370 existing + ~10-15 new).

### [x] Task 5.3: Run drift detection

**Command:** `pytest tests/test_routing_consistency.py -v`

**Expected:** All drift tests pass — all routing sources are in sync.

**Commit:** `chore: verify routing consolidation and pipeline tests`

---

## Summary

| Phase | Tasks | Key Outcome |
|-------|-------|-------------|
| 1 | 1.1–1.2 | `load_routing_rules()` in catalog_loader |
| 2 | 2.1–2.2 | Prompt routing section reads from YAML |
| 3 | 3.1–3.2 | Embedding descriptions from YAML + drift detection |
| 4 | 4.1–4.2 | Embedding pipeline has >80% test coverage |
| 5 | 5.1–5.3 | Verified: same behavior, all tests green |

## Verification Commands

```bash
# Drift detection
pytest tests/test_routing_consistency.py -v

# Pipeline tests
pytest tests/test_run_embeddings.py tests/test_populate_embeddings.py -v

# Full suite
pytest tests/ -v

# Coverage for new files
pytest tests/test_run_embeddings.py --cov=scripts --cov-report=term
```
