# Track 09: Production Hardening — Spec

## Problem

Two critical production bugs:

1. **JSON Serialization Crash**: `execute_sql` returns BigQuery rows with `pandas.Timestamp`, `datetime.date`, `Decimal`, `pd.NaT` etc. ADK persists state via `json.dumps()` which crashes on these types.

2. **Blunt Loop Prevention**: `max_tool_calls_per_turn=30` blocks legitimate complex queries. The real problem is the LLM repeating the same failed action.

## Solution

1. Shared `sanitize_row()` utility applied at the source (sql_executor.py) and defense-in-depth (clients.py). Converts all non-serializable types to JSON-safe equivalents.

2. Repetition detection: hash `(tool_name, args)`, track consecutive identical calls in state, block after N repeats. Keep high safety net (50 calls) for truly pathological cases.

## Acceptance Criteria

- All BigQuery result types (Timestamp, date, time, Decimal, NaT, bytes, numpy scalars) serializable via json.dumps
- Repetition detection blocks after 3 consecutive identical tool calls
- Different tool calls (even 20+) proceed without false positives
- All existing tests continue to pass
- 265+ total tests pass
