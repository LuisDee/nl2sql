# Track 10: Implementation Plan

## Status: COMPLETE

## Phase 1: Trade Type Taxonomy Metadata [checkpoint: ccd5b46]

### [x] Task 1.1: Update dataset-level YAML with taxonomy overview `ccd5b46`
- Added `trade_taxonomy` section to `catalog/data/_dataset.yaml` and `catalog/kpi/_dataset.yaml`
- Explains relationship: markettrade is the superset, other tables are Mako-specific subsets
- Double-counting warning for cross-table aggregations

### [x] Task 1.2: Add `business_context` to each table YAML `ccd5b46`
- All 12 table YAMLs updated with `business_context` field
- All 12 table YAMLs updated with `preferred_timestamps` field (Phase 2 combined)

### [x] Task 1.3: Update system prompt to surface taxonomy `ccd5b46`
- Added double-counting warning to routing rule 5 in `prompts.py`
- Added timestamp column guidance to SQL generation rules

## Phase 2: Preferred Timestamps Per Table [checkpoint: ccd5b46]

### [x] Task 2.1: Add `preferred_timestamps` to each table YAML `ccd5b46`

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

### [x] Task 2.2: Surface preferred timestamps in metadata loader `ccd5b46`
- No code change needed — metadata_loader returns full YAML as string, new fields auto-surfaced

### [x] Task 2.3: Add ATM strike resolution pattern `ccd5b46`
- Added complex example to `examples/data_examples.yaml`: CTE-based ATM resolution via theodata.under → nearest strike → marketdepth query
- Added preferred timestamp usage example for marketdepth

## Phase 3: Validation & Testing [checkpoint: ccd5b46]

### [x] Task 3.1: Update catalog validation tests `ccd5b46`
- 12 parametrized tests for `business_context` field presence
- 12 parametrized tests for `preferred_timestamps` field presence
- 2 parametrized tests for `trade_taxonomy` in dataset YAMLs

### [x] Task 3.2: Add prompt/metadata integration tests `ccd5b46`
- `test_contains_double_counting_warning` — verifies "double-count" in prompt
- `test_contains_timestamp_guidance` — verifies DataTimestamp, EventTimestamp, event_timestamp_ns in prompt

## Verification
- 293 tests pass, 0 failures
