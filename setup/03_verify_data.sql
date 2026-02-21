-- ============================================================
-- Row counts — KPI tables
-- ============================================================
SELECT 'kpi.brokertrade' AS table_name, COUNT(*) AS row_count
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.brokertrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'kpi.clicktrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.clicktrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'kpi.markettrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.markettrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'kpi.otoswing', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.otoswing`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'kpi.quotertrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.quotertrade`
WHERE trade_date = '2026-02-17'
ORDER BY table_name;

-- ============================================================
-- Row counts — DATA tables
-- ============================================================
SELECT 'data.brokertrade' AS table_name, COUNT(*) AS row_count
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.brokertrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.clicktrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.clicktrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.markettrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.markettrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.swingdata', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.swingdata`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.quotertrade', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.quotertrade`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.theodata', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.theodata`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.marketdata', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.marketdata`
WHERE trade_date = '2026-02-17'
UNION ALL
SELECT 'data.marketdepth', COUNT(*)
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.marketdepth`
WHERE trade_date = '2026-02-17'
ORDER BY table_name;

-- ============================================================
-- Schema inspection — KPI markettrade (check column names)
-- ============================================================
SELECT column_name, data_type, is_nullable
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'markettrade'
ORDER BY ordinal_position;

-- Schema inspection — KPI brokertrade (must have account field)
SELECT column_name, data_type, is_nullable
FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'brokertrade'
ORDER BY ordinal_position;

-- Schema inspection — DATA theodata
SELECT column_name, data_type, is_nullable
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'theodata'
ORDER BY ordinal_position;

-- Schema inspection — DATA quotertrade (raw activity)
SELECT column_name, data_type, is_nullable
FROM `cloud-data-n-base-d4b3.nl2sql_omx_data.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'quotertrade'
ORDER BY ordinal_position;

-- Sample distinct values for routing-critical columns
SELECT DISTINCT delta_bucket FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.markettrade` WHERE trade_date = '2026-02-17';
SELECT DISTINCT account FROM `cloud-data-n-base-d4b3.nl2sql_omx_kpi.brokertrade` WHERE trade_date = '2026-02-17';
