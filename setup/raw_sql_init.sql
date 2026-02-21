-- ============================================================
-- DATASETS (run in shell first)
-- ============================================================
-- bq mk --location=europe-west2 cloud-data-n-base-d4b3:nl2sql_omx_kpi
-- bq mk --location=europe-west2 cloud-data-n-base-d4b3:nl2sql_omx_data


-- ============================================================
-- nl2sql_omx_kpi (source = views, partition by trade_date only)
-- ============================================================

CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_kpi.brokertrade`
PARTITION BY trade_date
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_kpi.brokertrade`
WHERE trade_date = '2026-02-17';

CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_kpi.clicktrade`
PARTITION BY trade_date
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_kpi.clicktrade`
WHERE trade_date = '2026-02-17';

CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_kpi.markettrade`
PARTITION BY trade_date
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_kpi.markettrade`
WHERE trade_date = '2026-02-17';

CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_kpi.otoswing`
PARTITION BY trade_date
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_kpi.otoswing`
WHERE trade_date = '2026-02-17';

CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_kpi.quotertrade`
PARTITION BY trade_date
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_kpi.quotertrade`
WHERE trade_date = '2026-02-17';


-- ============================================================
-- nl2sql_omx_data (preserve clustering from source tables)
-- ============================================================

-- CLUSTER BY portfolio, symbol, term, instrument_hash
CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_data.brokertrade`
PARTITION BY trade_date
CLUSTER BY portfolio, symbol, term, instrument_hash
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_data.brokertrade`
WHERE trade_date = '2026-02-17';

-- CLUSTER BY portfolio, symbol, term, instrument_hash
CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_data.clicktrade`
PARTITION BY trade_date
CLUSTER BY portfolio, symbol, term, instrument_hash
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_data.clicktrade`
WHERE trade_date = '2026-02-17';

-- CLUSTER BY portfolio, symbol, term, instrument_hash
CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_data.markettrade`
PARTITION BY trade_date
CLUSTER BY portfolio, symbol, term, instrument_hash
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_data.markettrade`
WHERE trade_date = '2026-02-17';

CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_data.swingdata`
PARTITION BY trade_date
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_data.swingdata`
WHERE trade_date = '2026-02-17';

-- CLUSTER BY portfolio, symbol, term, instrument_hash
CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_data.quotertrade`
PARTITION BY trade_date
CLUSTER BY portfolio, symbol, term, instrument_hash
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_data.quotertrade`
WHERE trade_date = '2026-02-17';

-- CLUSTER BY portfolio, symbol, term, instrument_hash
CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_data.theodata`
PARTITION BY trade_date
CLUSTER BY portfolio, symbol, term, instrument_hash
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_data.theodata`
WHERE trade_date = '2026-02-17';

-- CLUSTER BY symbol, term, instrument_hash
CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_data.marketdata`
PARTITION BY trade_date
CLUSTER BY symbol, term, instrument_hash
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_data.marketdata`
WHERE trade_date = '2026-02-17';

-- CLUSTER BY symbol, term, instrument_hash
CREATE OR REPLACE TABLE `cloud-data-n-base-d4b3.nl2sql_omx_data.marketdepth`
PARTITION BY trade_date
CLUSTER BY symbol, term, instrument_hash
AS
SELECT * FROM `cloud-data-p-base-7b7e.omx_data.marketdepth`
WHERE trade_date = '2026-02-17';
