# Track 10: Implementation Plan

## Status: IN PROGRESS

## Phase 1: Trade Type Taxonomy Metadata

### Task 1.1: Update dataset-level YAML with taxonomy overview
- Add `trade_taxonomy` section to `catalog/data/_dataset.yaml` and `catalog/kpi/_dataset.yaml`
- Explain the relationship: markettrade is the superset, other tables are Mako-specific subsets
- Add double-counting warning for cross-table aggregations

### Task 1.2: Add `business_context` to each table YAML
- `markettrade`: "All market trades across all participants. Includes Mako's own trades which also appear in otoswing, quotertrade, clicktrade, brokertrade."
- `otoswing` / `swingdata`: "Mako's automated OTO swing takeouts. These trades also appear in markettrade."
- `brokertrade`: "Mako's broker-facilitated trades. These trades also appear in markettrade."
- `clicktrade`: "Mako's screen/click trades. These trades also appear in markettrade."
- `quotertrade`: "Mako's quote fills. These trades also appear in markettrade."
- `marketdepth`: "Order book depth snapshots. Not a trade table."
- `marketdata`: "Market data ticks/snapshots. Not a trade table."
- `theodata`: "Theoretical pricing data. Not a trade table."

### Task 1.3: Update system prompt to surface taxonomy
- Add taxonomy context to `prompts.py` system instruction so the agent warns about double-counting
- Alternatively: ensure `load_yaml_metadata` surfaces the `business_context` field prominently

## Phase 2: Preferred Timestamps Per Table

### Task 2.1: Add `preferred_timestamps` to each table YAML

**Data Layer (`nl2sql_omx_data`):**

| Table | Primary | Fallback Chain |
|-------|---------|----------------|
| `clicktrade` | `TransactionTimestamp` | — |
| `marketdata` | `DataTimestamp` | `HardwareNicRxTimestamp → MakoIngressTimestamp → SoftwareRxTimestamp → ExchangeTimestamp` |
| `marketdepth` | `DataTimestamp` | `HardwareNicRxTimestamp → MakoIngressTimestamp → SoftwareRxTimestamp → ExchangeTimestamp` |
| `markettrade` | `EventTimestamp` | `HardwareNicRxTimestamp` |
| `quotertrade` | `EventTimestamp` | `HardwareNicRxTimestamp` |
| `swingdata` | `EventTimestamp` | `HardwareNicRxTimestamp` |
| `theodata` | `TheoEventTxTimestamp` | `WhenTimestamp` |

**KPI Layer (`nl2sql_omx_kpi`) — all 5 tables:**

| Table | Primary |
|-------|---------|
| `brokertrade` | `event_timestamp_ns` |
| `clicktrade` | `event_timestamp_ns` |
| `markettrade` | `event_timestamp_ns` |
| `otoswing` | `event_timestamp_ns` |
| `quotertrade` | `event_timestamp_ns` |

### Task 2.2: Surface preferred timestamps in metadata loader
- When `load_yaml_metadata` returns table info, include `preferred_timestamps` prominently
- Agent should see this before writing any time-filtered SQL

### Task 2.3: Add ATM strike resolution pattern
- Add example queries in the catalog showing the pattern:
  1. Query underlying price at target time
  2. Find nearest strike to underlying price
  3. Then query options data for that strike
- This teaches the agent the correct approach via few-shot examples

## Phase 3: Validation & Testing

### Task 3.1: Update catalog validation tests
- Assert every table YAML has `business_context` field
- Assert every table YAML has `preferred_timestamps` field
- Assert dataset YAMLs have `trade_taxonomy` section

### Task 3.2: Add prompt/metadata integration tests
- Verify system prompt includes taxonomy warning
- Verify metadata loader surfaces new fields

## Files to Modify
- `catalog/data/_dataset.yaml`
- `catalog/kpi/_dataset.yaml`
- `catalog/data/*.yaml` (all 7 data table YAMLs)
- `catalog/kpi/*.yaml` (all 5 KPI table YAMLs)
- `nl2sql_agent/prompts.py` (taxonomy context)
- `nl2sql_agent/tools/metadata_loader.py` (surface new fields)

## Files to Create
- None expected (all changes to existing files)

## Dependencies
- ~~Timestamp mappings from user (Phase 2 blocked)~~ RECEIVED
- Trade taxonomy confirmation from user (Phase 1 ready)
