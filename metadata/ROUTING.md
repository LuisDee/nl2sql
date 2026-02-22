# Cross-Repo Routing Guide

How data flows from C++ protobuf definitions to BigQuery KPI tables, and which repo owns what.

## 1. Cross-Repo Data Pipeline

```
CPP repo (proto3 definitions)
    |
    | binary proto messages via Kafka
    v
data-library repo (Go schema handlers)
    |
    | proto.Unmarshal -> SchemaHandler.BuildData -> Parquet
    v
GCS (Hive-partitioned Parquet files)
    |
    | dbt + DuckDB reads from GCS
    v
data-loader repo (dbt staging/intermediate/mart transforms)
    |
    | Exports enriched Parquet back to GCS -> BQ external tables
    v
BigQuery data-layer tables (10 tables)
    |
    | dbt for BigQuery reads data-layer tables
    v
KPI repo (dbt calculations: edge, PnL, slippage decomposition)
    |
    v
BigQuery KPI tables (5 tables)
```

| Stage | Repo | Language | Engine | Output |
|-------|------|----------|--------|--------|
| Proto definitions | cpp | C++ / proto3 | N/A | `.proto` files |
| Proto -> Parquet | data-library | Go | Kafka consumer | Parquet on GCS |
| Bronze -> Silver | data-loader | SQL (dbt) | DuckDB | Enriched Parquet on GCS / BQ |
| Silver -> Gold | kpi | SQL (dbt) | BigQuery | KPI tables in BQ |

## 2. By-BQ-Table Routing

### Data-Layer Tables (10)

| BQ Table | Proto Message | Proto File | Go Schema Handler | Data-Loader Staging | Data-Loader Mart | Notes |
|----------|--------------|------------|-------------------|---------------------|------------------|-------|
| markettrade | `MarketTrade` | `data/MarketTrade.proto` | `schema/marketTrade.go` | `stg_raw__markettrade.sql` | `markettrade.sql` | VtCommon embedded as `props`. Instrument via instrumentHash. |
| quotertrade | `QuoterTrade` | `data/QuoterTrade.proto` | `schema/quoteTrade.go` | `stg_raw__quotertrade.sql` | `quotertrade.sql` | VtCommon embedded as `props`. Instrument via instrumentHash. |
| brokertrade | `BrokerTrade` | `data/OffFloorData.proto` | **None** (`SchemaBuilder: nil`) | `stg_raw__brokertrade.sql` | `brokertrade.sql` | No Go schema handler. From `csa_offfloor` Kafka topic. 9-step intermediate pipeline in data-loader. No VtCommon. |
| swingdata | `SwingData` | `data/SwingData.proto` | `schema/swingData.go` | `stg_raw__swingdata.sql` | `swingdata.sql` | VtCommon embedded as `props`. Separate from oroswingdata. |
| oroswingdata | `OroSwingData` | `data/OroSwingData.proto` | `schema/oroSwingData.go` | `stg_raw__oroswingdata.sql` | `oroswingdata.sql` | VtCommon embedded as `props`. Source for `otoswing` KPI table. Note "Oro" not "Oto" in proto. |
| marketdata | `MarketEvent` | `data/OboMarketData.proto` | `schema/marketData.go` | `stg_raw__marketdata.sql` | `marketdata.sql` | No VtCommon. Raw order-book events. NOT MarketDataExtended. |
| marketdataext | `MarketDataExtended` | `data/MarketDataExtended.proto` | (check schema) | `stg_raw__marketdataext.sql` | `marketdataext.sql` | No VtCommon. Extended market data with buyer/seller IDs. |
| marketdepth | `MarketDepth` | `data/MarketDepth.proto` | `schema/marketDepth.go` | `stg_raw__marketdepth.sql` | `marketdepth.sql` | No VtCommon. 5-level book expanded to 30 columns. |
| theodata | `TheoData` | `data/theoData.proto` | `schema/theoData.go` | `stg_raw__theodata.sql` | `theodata.sql` | No VtCommon. Greeks, vol, theoretical values from theoServer. |
| tradedata | `PositionEvent` | `data/PosData.proto` | (check schema) | `stg_raw__tradedata.sql` | `tradedata.sql` | No VtCommon. Source for `clicktrade` KPI table (filtered subset). |

### KPI Tables (5)

| KPI Table | Source Data-Layer Table | KPI Calculation File | Notes |
|-----------|------------------------|---------------------|-------|
| markettrade | markettrade | `markettrade/kpi_markettrade_calculations.sql` | Baseline model, simplest formulas. buy_sell_multiplier: -1 for BUY, +1 for SELL (reversed). |
| quotertrade | quotertrade | `quotertrade/kpi_quotertrade_calculations.sql` | Side-dependent TV (tv_bid/tv_ask). Has mid_tv, mark-to-mid PnL, NHR adjustment. |
| brokertrade | brokertrade | `brokertrade/kpi_brokertrade_calculations.sql` | 9 fee methods. market_mako_multiplier. Ref trade components. Uses trade_tv (not gamma-adjusted tv). |
| clicktrade | tradedata (filtered) | `clicktrade/kpi_clicktrade_calculations.sql` | Derived from tradedata. Filters: algorithm IN (VTM_TAKEOUT, CLICK_ORDER, ...), position_type NOT IN (80,69,65,68,79), inst_type_name IN (OPTION, COMBO). Uses tv_theo, fees=0. |
| otoswing | oroswingdata | `otoswing/kpi_otoswing_calculations.sql` | Most complex. Dual gamma adjustments. Regular + fired-at variants. Uses swing_mid_tv. |

## 3. By-Data-Concept Routing

| Question | Authoritative Source | Repo | Path |
|----------|---------------------|------|------|
| What columns does table X have? | dbt staging/mart SQL + BQ schema | data-loader | `models/staging/raw/stg_raw__<table>.sql`, `models/marts/raw_daily/<table>.sql` |
| What does column Y mean? | Proto field comments + Go schema handler | cpp, data-library | `source/pb/data/<Proto>.proto`, `schema/<handler>.go` |
| How is KPI metric Z calculated? | KPI calculation SQL | kpi | `models/staging/kpi/components/calculations/<type>/` |
| What proto field maps to BQ column X? | `proto_to_bq` mapping + staging transforms | nl2sql-agent metadata, data-loader | `metadata/proto_fields.yaml`, `metadata/data_loader_transforms.yaml` |
| What are the proto field types? | Proto definitions | cpp | `source/pb/data/<Proto>.proto` |
| How does proto -> Parquet conversion work? | Go schema handlers | data-library | `schema/<handler>.go`, `schema/vtCommon.go` |
| How is deduplication done? | dbt intermediate/mart SQL | data-loader | `models/intermediate/raw/int_raw__<table>_*.sql` |
| What instrument enrichment is applied? | dbt instrument join | data-loader | `models/intermediate/raw/int_raw__instruments_agg.sql` |
| What time intervals exist for KPIs? | Interval definitions | kpi | `models/staging/kpi/components/reference_data/config/kpi_slippage_time_intervals.sql`, `macros/get_slippages.sql` |
| What is the buy/sell sign convention? | KPI calculation SQL | kpi, nl2sql-agent metadata | `metadata/kpi_computations.yaml` (comparison_notes.buy_sell_multiplier) |
| What fee methods exist? | Brokertrade KPI SQL | kpi | `brokertrade/kpi_brokertrade_calculations.sql` |
| What VtCommon fields are shared? | VtCommon proto + Go handler | cpp, data-library | `source/pb/data/VtCommon.proto`, `schema/vtCommon.go` |

## 4. Special Cases

### clicktrade: derived from tradedata, not a separate proto

`clicktrade` is NOT a proto message. It is a filtered subset of `tradedata` (from `PosData.proto` / `PositionEvent`):

- Algorithm filter: `algorithm IN ('VTM_TAKEOUT', 'CLICK_ORDER', 'CLICK_TAKEOUT', 'BOOKSCREEN', 'VTM_ORDER', 'VTM_DIME', 'COMBO_TAKEOUT', 'COMBO_ORDER', 'COMBO')`
- Position type exclusion: `position_type NOT IN (80, 69, 65, 68, 79)`
- Instrument type filter: `inst_type_name IN ('OPTION', 'COMBO')`

The filtering and KPI computation happen entirely in the KPI repo. The data-loader only processes the raw `tradedata` table.

### oroswingdata vs swingdata: separate proto messages, separate pipelines

| | swingdata | oroswingdata |
|---|-----------|-------------|
| Proto | `SwingData` (`data/SwingData.proto`) | `OroSwingData` (`data/OroSwingData.proto`) |
| Go handler | `schema/swingData.go` | `schema/oroSwingData.go` |
| Staging | `stg_raw__swingdata.sql` | `stg_raw__oroswingdata.sql` |
| KPI table | None | `otoswing` |
| Note | OTO swing from option-tick/under-move takeouts | Source for otoswing KPI. Note proto uses "Oro" prefix, KPI uses "oto" prefix. Has TriggerSource enum (FPGA vs software). |

### brokertrade: no Go schema handler in data-library

`BrokerTrade` has `SchemaBuilder: nil` in `data-library/schema/schema.go`. The proto definition exists (`OffFloorData.pb.go`) and the Kafka topic is `csa_offfloor`, but ingestion uses a different path (dynamic schema or separate service). The data-loader processes brokertrade through a 9-step intermediate pipeline, the most complex transformation in the system.

### contract_size: streamed from TradableInstrument proto, not computed

`contract_size` is part of the instrument metadata streamed via the `TradableInstrument` proto message. It is NOT computed by the KPI repo. It arrives in BQ via the instrument enrichment JOIN in the data-loader mart layer (`int_raw__instruments_agg`).

### VtCommon: embedded in trading messages but NOT in market data

VtCommon (57 shared fields: algo, greeks, base values, adjustments, fees, timestamps) is embedded as `props` field (number 1) in:
- MarketTrade, QuoterTrade, SwingData, OroSwingData

VtCommon is NOT present in:
- MarketEvent (marketdata), MarketDataExtended (marketdataext), MarketDepth (marketdepth), TheoData (theodata), PositionEvent (tradedata), BrokerTrade (brokertrade)

This is why marketdata/marketdepth/theodata/tradedata/brokertrade have significantly fewer columns than the trading tables.
