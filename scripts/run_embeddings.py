"""Create and populate BigQuery embedding infrastructure.

Replaces standalone SQL scripts with a parameterized Python runner that reads
all project/model/connection references from settings.

Usage:
    python scripts/run_embeddings.py --step create-dataset
    python scripts/run_embeddings.py --step verify-model
    python scripts/run_embeddings.py --step create-tables
    python scripts/run_embeddings.py --step populate-schema
    python scripts/run_embeddings.py --step generate-embeddings
    python scripts/run_embeddings.py --step create-indexes
    python scripts/run_embeddings.py --step test-search
    python scripts/run_embeddings.py --step all          # runs all steps in order
"""

import argparse
import sys

from nl2sql_agent.config import Settings
from nl2sql_agent.logging_config import setup_logging, get_logger
from nl2sql_agent.protocols import BigQueryProtocol
from nl2sql_agent.clients import LiveBigQueryClient

setup_logging()
logger = get_logger(__name__)


def create_metadata_dataset(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 1: Create the metadata dataset. Idempotent via IF NOT EXISTS."""
    sql = f"""
    CREATE SCHEMA IF NOT EXISTS `{s.gcp_project}.{s.metadata_dataset}`
    OPTIONS (
      description = 'NL2SQL agent metadata: schema embeddings, column embeddings, query memory',
      location = '{s.bq_location}'
    );
    """
    bq.execute_query(sql)
    logger.info("created_metadata_dataset", project=s.gcp_project, dataset=s.metadata_dataset)


def verify_embedding_model(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 2: Verify the embedding model exists and is accessible."""
    parts = s.embedding_model_ref.split(".")
    model_project = parts[0]
    model_dataset = parts[1]
    model_name = parts[2]

    sql = f"""
    SELECT model_name, model_type, creation_time
    FROM `{model_project}.{model_dataset}.INFORMATION_SCHEMA.MODELS`
    WHERE model_name = '{model_name}';
    """
    result = bq.execute_query(sql)
    logger.info("verified_embedding_model", model_ref=s.embedding_model_ref, rows=len(result))


def create_embedding_tables(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 3: Create the 3 embedding tables. Idempotent via CREATE OR REPLACE."""
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"

    sqls = [
        f"""
        CREATE OR REPLACE TABLE `{fqn}.schema_embeddings` (
          id STRING DEFAULT GENERATE_UUID(),
          source_type STRING NOT NULL,
          layer STRING,
          dataset_name STRING,
          table_name STRING,
          description STRING NOT NULL,
          embedding ARRAY<FLOAT64>,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
        f"""
        CREATE OR REPLACE TABLE `{fqn}.column_embeddings` (
          id STRING DEFAULT GENERATE_UUID(),
          dataset_name STRING NOT NULL,
          table_name STRING NOT NULL,
          column_name STRING NOT NULL,
          column_type STRING NOT NULL,
          description STRING NOT NULL,
          synonyms ARRAY<STRING>,
          embedding ARRAY<FLOAT64>,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
        f"""
        CREATE OR REPLACE TABLE `{fqn}.query_memory` (
          id STRING DEFAULT GENERATE_UUID(),
          question STRING NOT NULL,
          sql_query STRING NOT NULL,
          tables_used ARRAY<STRING>,
          dataset STRING NOT NULL,
          complexity STRING,
          routing_signal STRING,
          validated_by STRING,
          validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
          success_count INT64 DEFAULT 1,
          embedding ARRAY<FLOAT64>
        );
        """,
    ]
    for sql in sqls:
        bq.execute_query(sql)
    logger.info("created_embedding_tables", fqn=fqn)


def populate_schema_embeddings(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 4: Populate schema_embeddings from catalog descriptions.

    Idempotent: MERGE on (source_type, dataset_name, table_name).
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"

    # KPI dataset rows
    kpi_sql = f"""
    MERGE `{fqn}.schema_embeddings` AS target
    USING (
      SELECT * FROM UNNEST([
        STRUCT(
          'dataset' AS source_type, 'kpi' AS layer, 'nl2sql_omx_kpi' AS dataset_name,
          CAST(NULL AS STRING) AS table_name,
          'KPI Key Performance Indicators dataset for OMX options trading. Gold layer. Contains one table per trade type: markettrade (exchange trades, default), quotertrade (auto-quoter fills), brokertrade (broker trades with account field for broker comparison), clicktrade (manual click trades), otoswing (OTO swing trades). All share columns: edge (trading edge), instant_pnl (immediate PnL profit loss), instant_pnl_w_fees (PnL with fees), delta slippage at multiple intervals. For total PnL across all types use UNION ALL.' AS description
        ),
        STRUCT('table', 'kpi', 'nl2sql_omx_kpi', 'markettrade',
          'KPI metrics for market exchange trades on OMX options. One row per trade. Contains edge (difference between machine fair value and trade price), instant_pnl (immediate PnL, profit loss), instant_pnl_w_fees, delta_slippage at 1s/1m/5m/30m/1h/eod intervals, portfolio, symbol, term, trade_date. Default KPI table when trade type is not specified.'),
        STRUCT('table', 'kpi', 'nl2sql_omx_kpi', 'quotertrade',
          'KPI metrics for auto-quoter originated trade fills on OMX options. Same KPI columns as markettrade: edge, instant_pnl, delta slippage. Use for quoter performance, quoter edge, quoter PnL analysis.'),
        STRUCT('table', 'kpi', 'nl2sql_omx_kpi', 'brokertrade',
          'KPI metrics for broker facilitated trades. Same KPI columns as markettrade plus account field. Use when comparing broker performance or when question mentions broker account names like BGC or MGN. NOTE: may be empty for some dates.'),
        STRUCT('table', 'kpi', 'nl2sql_omx_kpi', 'clicktrade',
          'KPI metrics for manually initiated click trades. Same KPI columns as markettrade. Use when question mentions click trades or manual trades.'),
        STRUCT('table', 'kpi', 'nl2sql_omx_kpi', 'otoswing',
          'KPI metrics for OTO swing trades. Same KPI columns as markettrade. Use when question mentions OTO, swing, or otoswing trades.')
      ])
    ) AS source
    ON target.source_type = source.source_type
       AND COALESCE(target.dataset_name, '') = COALESCE(source.dataset_name, '')
       AND COALESCE(target.table_name, '') = COALESCE(source.table_name, '')
    WHEN MATCHED THEN
      UPDATE SET description = source.description, embedding = NULL, updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (source_type, layer, dataset_name, table_name, description)
      VALUES (source.source_type, source.layer, source.dataset_name, source.table_name, source.description);
    """
    bq.execute_query(kpi_sql)

    # DATA dataset rows
    data_sql = f"""
    MERGE `{fqn}.schema_embeddings` AS target
    USING (
      SELECT * FROM UNNEST([
        STRUCT(
          'dataset' AS source_type, 'data' AS layer, 'nl2sql_omx_data' AS dataset_name,
          CAST(NULL AS STRING) AS table_name,
          'Raw OMX options trading and market data. Silver layer. Contains trade execution details, theoretical options pricing snapshots (theo delta vol vega), market data feeds, order book depth. Higher granularity than KPI but without computed performance metrics like edge or instant_pnl.' AS description
        ),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'theodata',
          'Theoretical options pricing snapshots. Contains tv (theoretical fair price, fair value, theo, machine price), delta (delta greek, hedge ratio), vol (annualised implied volatility as decimal, also called IV, implied vol, sigma), vega, gamma, theta, strike, symbol, term, portfolio, trade_date. ONLY exists in data dataset. Use for any question about vol, IV, implied volatility, delta, greeks, fair value, or theoretical pricing.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'marketdata',
          'Market data feed snapshots for OMX options. Top-of-book prices, volumes. Use for market price questions, exchange data, price feeds.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'marketdepth',
          'Full order book depth for OMX options. Multiple price levels with bid/ask sizes at each level. Use for order book questions, depth analysis, bid-ask spread at different levels.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'swingdata',
          'Raw OTO swing trade data. Use for swing-specific raw data questions.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'markettrade',
          'Raw market trade execution details. Silver layer -- exact timestamps, prices, sizes, fill information. NOT KPI enriched. Use when question asks about raw execution details, not performance metrics.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'quotertrade',
          'Raw quoter trade execution details. Silver layer. Use for raw quoter execution data, not KPI performance metrics.'),
        STRUCT('table', 'data', 'nl2sql_omx_data', 'clicktrade',
          'Raw click trade execution details. Silver layer.')
      ])
    ) AS source
    ON target.source_type = source.source_type
       AND COALESCE(target.dataset_name, '') = COALESCE(source.dataset_name, '')
       AND COALESCE(target.table_name, '') = COALESCE(source.table_name, '')
    WHEN MATCHED THEN
      UPDATE SET description = source.description, embedding = NULL, updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (source_type, layer, dataset_name, table_name, description)
      VALUES (source.source_type, source.layer, source.dataset_name, source.table_name, source.description);
    """
    bq.execute_query(data_sql)

    # Routing descriptions
    routing_sql = f"""
    MERGE `{fqn}.schema_embeddings` AS target
    USING (
      SELECT * FROM UNNEST([
        STRUCT(
          'routing' AS source_type, CAST(NULL AS STRING) AS layer,
          CAST(NULL AS STRING) AS dataset_name, CAST(NULL AS STRING) AS table_name,
          'The nl2sql_omx_kpi dataset (gold layer) has KPI-enriched trade data with computed metrics: edge, instant_pnl, delta slippage. The nl2sql_omx_data dataset (silver layer) has raw trade and market data. Both contain tables with the same names (clicktrade, markettrade, quotertrade). Use kpi for performance edge PnL slippage questions. Use data for raw execution timestamps market data and theoretical pricing.' AS description
        ),
        STRUCT('routing', NULL, NULL, NULL,
          'Questions about theoretical pricing, implied volatility IV vol sigma, delta, vega, gamma, greeks, fair value, machine price, or theo should route to nl2sql_omx_data.theodata. This table only exists in the data dataset. It does not exist in kpi.'),
        STRUCT('routing', NULL, NULL, NULL,
          'The kpi dataset has 5 tables one per trade origin: markettrade (exchange trades the default), quotertrade (auto-quoter fills), brokertrade (broker trades with account field), clicktrade (manual), otoswing (OTO swing). When trade type is unspecified use markettrade. When comparing brokers use brokertrade. For all trades use UNION ALL across all 5.')
      ])
    ) AS source
    ON FALSE
    WHEN NOT MATCHED THEN
      INSERT (source_type, layer, dataset_name, table_name, description)
      VALUES (source.source_type, source.layer, source.dataset_name, source.table_name, source.description);
    """
    bq.execute_query(routing_sql)

    # Clean up duplicate routing rows from previous runs
    cleanup_sql = f"""
    DELETE FROM `{fqn}.schema_embeddings`
    WHERE source_type = 'routing'
      AND id NOT IN (
        SELECT MAX(id) FROM `{fqn}.schema_embeddings`
        WHERE source_type = 'routing'
        GROUP BY description
      );
    """
    bq.execute_query(cleanup_sql)
    logger.info("populated_schema_embeddings")


def generate_embeddings(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 5: Generate embeddings for all rows that don't have them yet.

    Idempotent: WHERE ARRAY_LENGTH(embedding) = 0.
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"
    model = s.embedding_model_ref

    sqls = [
        # Schema embeddings
        f"""
        UPDATE `{fqn}.schema_embeddings` t
        SET embedding = (
          SELECT ml_generate_embedding_result
          FROM ML.GENERATE_EMBEDDING(
            MODEL `{model}`,
            (SELECT t.description AS content),
            STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_DOCUMENT' AS task_type)
          )
        )
        WHERE ARRAY_LENGTH(t.embedding) = 0;
        """,
        # Column embeddings
        f"""
        UPDATE `{fqn}.column_embeddings` t
        SET embedding = (
          SELECT ml_generate_embedding_result
          FROM ML.GENERATE_EMBEDDING(
            MODEL `{model}`,
            (SELECT t.description AS content),
            STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_DOCUMENT' AS task_type)
          )
        )
        WHERE ARRAY_LENGTH(t.embedding) = 0;
        """,
        # Query memory
        f"""
        UPDATE `{fqn}.query_memory` t
        SET embedding = (
          SELECT ml_generate_embedding_result
          FROM ML.GENERATE_EMBEDDING(
            MODEL `{model}`,
            (SELECT t.question AS content),
            STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type)
          )
        )
        WHERE ARRAY_LENGTH(t.embedding) = 0;
        """,
    ]
    for sql in sqls:
        bq.execute_query(sql)
    logger.info("generated_embeddings")


def create_vector_indexes(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 6: Create TREE_AH vector indexes. Idempotent via IF NOT EXISTS.

    NOTE: BigQuery requires >=5000 rows for TREE_AH to activate.
    With <5000 rows, BQ falls back to brute-force (still works).
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"

    sqls = [
        f"""
        CREATE VECTOR INDEX IF NOT EXISTS idx_schema_embeddings
        ON `{fqn}.schema_embeddings`(embedding)
        OPTIONS (index_type = 'TREE_AH', distance_type = 'COSINE');
        """,
        f"""
        CREATE VECTOR INDEX IF NOT EXISTS idx_column_embeddings
        ON `{fqn}.column_embeddings`(embedding)
        OPTIONS (index_type = 'TREE_AH', distance_type = 'COSINE');
        """,
        f"""
        CREATE VECTOR INDEX IF NOT EXISTS idx_query_memory
        ON `{fqn}.query_memory`(embedding)
        OPTIONS (index_type = 'TREE_AH', distance_type = 'COSINE');
        """,
    ]
    for sql in sqls:
        bq.execute_query(sql)
    logger.info("created_vector_indexes")


def test_vector_search(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 7: Run 5 test cases for vector search quality.

    Checks that expected table/row appears in top-5 results.
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"
    model = s.embedding_model_ref

    test_cases = [
        ("TEST 1: edge -> KPI", "what was the edge on our trade?", "schema_embeddings"),
        ("TEST 2: IV -> theodata", "how did implied vol change over the last month?", "schema_embeddings"),
        ("TEST 3: broker -> brokertrade", "how have broker trades by BGC performed compared to MGN?", "schema_embeddings"),
        ("TEST 4: depth -> marketdepth", "what did the order book look like for the ATM strike?", "schema_embeddings"),
        ("TEST 5: few-shot retrieval", "show me PnL breakdown by symbol", "query_memory"),
    ]

    for test_name, query_text, table in test_cases:
        if table == "schema_embeddings":
            sql = f"""
            SELECT '{test_name}' AS test_name,
                   base.source_type, base.layer, base.dataset_name, base.table_name,
                   ROUND(distance, 4) AS dist
            FROM VECTOR_SEARCH(
              TABLE `{fqn}.{table}`, 'embedding',
              (SELECT ml_generate_embedding_result AS embedding
               FROM ML.GENERATE_EMBEDDING(
                 MODEL `{model}`,
                 (SELECT '{query_text}' AS content),
                 STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type)
               )),
              top_k => 5, distance_type => 'COSINE')
            ORDER BY distance;
            """
        else:
            sql = f"""
            SELECT '{test_name}' AS test_name,
                   base.question, base.tables_used, base.dataset,
                   ROUND(distance, 4) AS dist
            FROM VECTOR_SEARCH(
              TABLE `{fqn}.{table}`, 'embedding',
              (SELECT ml_generate_embedding_result AS embedding
               FROM ML.GENERATE_EMBEDDING(
                 MODEL `{model}`,
                 (SELECT '{query_text}' AS content),
                 STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type)
               )),
              top_k => 5, distance_type => 'COSINE')
            ORDER BY distance;
            """
        result = bq.execute_query(sql)
        print(f"\n{test_name}")
        for _, row in result.iterrows():
            print(f"  {dict(row)}")


STEPS = {
    "create-dataset": create_metadata_dataset,
    "verify-model": verify_embedding_model,
    "create-tables": create_embedding_tables,
    "populate-schema": populate_schema_embeddings,
    "generate-embeddings": generate_embeddings,
    "create-indexes": create_vector_indexes,
    "test-search": test_vector_search,
}

ALL_STEPS_ORDER = [
    "create-dataset", "verify-model", "create-tables",
    "populate-schema", "generate-embeddings", "create-indexes", "test-search",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run embedding infrastructure steps")
    parser.add_argument("--step", required=True, choices=list(STEPS.keys()) + ["all"],
                        help="Which step to run (or 'all')")
    args = parser.parse_args()

    settings = Settings()
    bq = LiveBigQueryClient(project=settings.gcp_project, location=settings.bq_location)

    if args.step == "all":
        for step_name in ALL_STEPS_ORDER:
            logger.info("running_step", step=step_name)
            STEPS[step_name](bq, settings)
    else:
        STEPS[args.step](bq, settings)

    logger.info("done")


if __name__ == "__main__":
    main()
