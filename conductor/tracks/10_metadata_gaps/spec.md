# Track 10: Metadata Gaps — Spec

## Problem

End-to-end testing revealed two critical metadata gaps that cause the agent to waste round-trips, misinterpret data, and produce misleading results.

### Gap 1: Trade Type Taxonomy (semantic correctness)

The agent treats all trade tables as independent, equal data sources. In reality:

- **markettrade** = ALL market trades across all participants (includes Mako's own)
- **otoswing** = Mako's automated OTO swing takeouts (subset of markettrade)
- **brokertrade** = Mako's broker-facilitated trades (subset of markettrade)
- **clicktrade** = Mako's screen/click trades (subset of markettrade)
- **quotertrade** = Mako's quote fills (subset of markettrade)

**Impact**: When the user asks "total PnL across all trade types", the agent sums all tables — double-counting Mako's trades that appear in both markettrade AND the specific trade type tables. This produces misleading totals.

The agent also lacks context about what each trade type *means* for the business, so it can't give intelligent commentary (e.g. "otoswing is your automated takeout strategy").

### Gap 2: Preferred Timestamps Per Table (query efficiency)

Each data table has specific timestamp columns that should be used for time-based filtering (partition keys, event timestamps, close timestamps). The agent doesn't know which ones to use, leading to:

- Trial-and-error column selection (7 wasted round-trips in the order book depth query)
- Using wrong timestamps (epoch 1970-01-01 values from exchange_timestamp)
- Falling back to SELECT * exploration queries to discover the right column

**Impact**: Simple time-filtered queries take 5-10x more tool calls than necessary.

### Gap 3: ATM Strike Resolution (domain logic)

The agent guessed "strike 100 is probably ATM" instead of querying the underlying price first and finding the nearest strike. This is a common pattern for options queries that should be documented as a recommended approach.

## Acceptance Criteria

1. Every table YAML includes a `business_context` field explaining its role in the trade taxonomy
2. Every table YAML includes a `preferred_timestamps` field listing the best columns for time filtering
3. Dataset-level `_dataset.yaml` files explain the relationship between trade types (overlap warning)
4. Agent uses preferred timestamps on first attempt (no trial-and-error)
5. Agent warns about double-counting when summing across markettrade + specific trade types
6. Catalog validation tests updated to check new fields exist
