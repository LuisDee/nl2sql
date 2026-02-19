# Track Specification: Eval & Hardening

## Overview
This track hardens the agent by implementing a robust evaluation framework, a self-correcting retry loop for SQL generation, and a learning loop that improves performance over time.

## Components

### 1. Gold Standard Evaluation Set (`eval/gold_queries.yaml`)
- **Status:** Existing (Partial/Complete)
- **Requirement:** A YAML file containing at least 50 pairs of `question` and `expected_sql` (or `expected_results_summary`).
- **Structure:**
  ```yaml
  - question: "Show me the top 5 traders by volume"
    expected_sql: "SELECT ..."
    category: "simple"
    difficulty: "easy"
  ```

### 2. Online Evaluation Runner (`eval/run_eval.py`)
- **Current State:** Supports offline syntax validation and dry-run checks.
- **New Requirement:** "Online Mode"
    -   **Execution:** Must instantiate the `nl2sql_agent` and run `agent.run(question)`.
    -   **Validation:**
        1.  **Routing:** Did it call `generate_sql`?
        2.  **Syntax:** Is the generated SQL valid BQ syntax?
        3.  **Accuracy (Strict):** Does the result set match the gold standard result set?
    -   **Reporting:** Output a summary table (Pass/Fail, Latency, Error).

### 3. Retry Logic (LoopAgent)
- **Requirement:** The agent must automatically recover from `dry_run_sql` failures.
- **Behavior:**
    1.  Agent generates SQL.
    2.  `dry_run_sql` tool validates it.
    3.  If `dry_run_sql` returns an error (e.g., "Column X not found"), the agent *must* receive this error in the observation.
    4.  The agent must generate a *new* SQL query fixing the error.
    5.  Repeat up to 3 times.
- **Implementation:** Currently implemented via `after_tool_log` callbacks. This track will formalize and verify this behavior.

### 4. Semantic Caching & Learning Loop
- **Requirement:**
    -   **Cache Read:** Before generating SQL, check `query_memory` for semantically similar questions (cosine distance < 0.2).
    -   **Cache Write:** When a user validates a query (via UI/feedback), save it to `query_memory` with embeddings.
- **Tools:** `check_semantic_cache`, `save_validated_query`.

## Acceptance Criteria
1.  `run_eval.py` runs successfully in "online" mode against the `dev_agent_test` dataset.
2.  The agent successfully self-corrects a deliberately broken query (e.g., asking for a non-existent column that sounds real) within 3 turns.
3.  Routing accuracy on the Gold Set is > 90%.
