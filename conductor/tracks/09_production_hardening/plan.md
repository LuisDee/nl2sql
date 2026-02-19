# Track 09: Implementation Plan

## Status: COMPLETE

## Phase 1: JSON Serialization Safety Layer
- [x] `nl2sql_agent/serialization.py` — sanitize_value/sanitize_row/sanitize_rows
- [x] Handles: pd.Timestamp, datetime, date, time, Decimal, bytes, NaT, numpy int/float/bool, NaN, nested dicts/lists
- [x] Applied in `nl2sql_agent/tools/sql_executor.py` (primary fix)
- [x] Applied in `nl2sql_agent/clients.py` (defense-in-depth)
- [x] Tests: `tests/test_serialization.py` (17 tests)

## Phase 2: Smart Loop Detection
- [x] `_tool_call_hash()` — stable MD5 hash of tool name + args
- [x] Repetition detection in `before_tool_guard` — consecutive identical hash count
- [x] `max_consecutive_repeats=3` config (replaces blunt counter behavior)
- [x] `max_tool_calls_per_turn` raised to 50 (safety net only)
- [x] `check_semantic_cache` resets both counter and history
- [x] Tests: `tests/test_repetition_detection.py` (13 tests)
- [x] Updated: `tests/test_circuit_breaker.py` (threshold 30→50)

## Files Modified
- `nl2sql_agent/tools/sql_executor.py`
- `nl2sql_agent/clients.py`
- `nl2sql_agent/callbacks.py`
- `nl2sql_agent/config.py`
- `tests/test_circuit_breaker.py`

## Files Created
- `nl2sql_agent/serialization.py`
- `tests/test_serialization.py`
- `tests/test_repetition_detection.py`
- `conductor/tracks/09_production_hardening/spec.md`
- `conductor/tracks/09_production_hardening/plan.md`
- `conductor/tracks/09_production_hardening/metadata.json`

## Verification
- 265 tests pass, 0 failures
