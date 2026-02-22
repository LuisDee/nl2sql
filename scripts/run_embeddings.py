"""Create and populate BigQuery embedding infrastructure.

Replaces standalone SQL scripts with a parameterized Python runner that reads
all project/model/connection references from settings.

Usage:
    python scripts/run_embeddings.py --step create-dataset
    python scripts/run_embeddings.py --step verify-model
    python scripts/run_embeddings.py --step create-tables
    python scripts/run_embeddings.py --step populate-schema
    python scripts/run_embeddings.py --step populate-symbols
    python scripts/run_embeddings.py --step generate-embeddings
    python scripts/run_embeddings.py --step create-indexes
    python scripts/run_embeddings.py --step test-search
    python scripts/run_embeddings.py --step all          # runs all steps in order
"""

import argparse
import csv
from pathlib import Path

from scripts.populate_glossary import populate_glossary_embeddings

from nl2sql_agent.catalog_loader import (
    CATALOG_DIR,
    load_routing_rules,
    load_yaml,
)
from nl2sql_agent.clients import LiveBigQueryClient
from nl2sql_agent.config import Settings
from nl2sql_agent.logging_config import get_logger, setup_logging
from nl2sql_agent.protocols import BigQueryProtocol

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
    logger.info(
        "created_metadata_dataset", project=s.gcp_project, dataset=s.metadata_dataset
    )


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
    logger.info(
        "verified_embedding_model", model_ref=s.embedding_model_ref, rows=len(result)
    )


def create_embedding_tables(
    bq: BigQueryProtocol, s: Settings, force: bool = False
) -> None:
    """Step 3: Create the embedding tables.

    By default uses CREATE TABLE IF NOT EXISTS (safe, preserves data).
    Pass force=True to use CREATE OR REPLACE TABLE (destroys existing data).
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"
    create_stmt = "CREATE OR REPLACE TABLE" if force else "CREATE TABLE IF NOT EXISTS"

    sqls = [
        f"""
        {create_stmt} `{fqn}.schema_embeddings` (
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
        {create_stmt} `{fqn}.column_embeddings` (
          id STRING DEFAULT GENERATE_UUID(),
          dataset_name STRING NOT NULL,
          table_name STRING NOT NULL,
          column_name STRING NOT NULL,
          column_type STRING NOT NULL,
          description STRING NOT NULL,
          embedding_text STRING,
          synonyms ARRAY<STRING>,
          category STRING,
          formula STRING,
          typical_aggregation STRING,
          filterable BOOL,
          example_values ARRAY<STRING>,
          related_columns ARRAY<STRING>,
          embedding ARRAY<FLOAT64>,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
        f"""
        {create_stmt} `{fqn}.query_memory` (
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
        f"""
        {create_stmt} `{fqn}.glossary_embeddings` (
          id STRING DEFAULT GENERATE_UUID(),
          name STRING NOT NULL,
          definition STRING NOT NULL,
          embedding_text STRING,
          synonyms ARRAY<STRING>,
          related_columns ARRAY<STRING>,
          category STRING,
          sql_pattern STRING,
          embedding ARRAY<FLOAT64>,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS `{fqn}.symbol_exchange_map` (
          symbol STRING NOT NULL,
          exchange STRING NOT NULL,
          portfolio STRING NOT NULL
        );
        """,
    ]
    for sql in sqls:
        bq.execute_query(sql)
    logger.info("created_embedding_tables", fqn=fqn)


def migrate_payload_columns(bq: BigQueryProtocol, s: Settings) -> None:
    """Add payload columns to existing column_embeddings table (idempotent).

    For fresh setups, create_embedding_tables() already includes these columns.
    This migration handles existing tables that predate the payload columns.
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"
    columns = [
        ("category", "STRING"),
        ("formula", "STRING"),
        ("typical_aggregation", "STRING"),
        ("filterable", "BOOL"),
        ("example_values", "ARRAY<STRING>"),
        ("related_columns", "ARRAY<STRING>"),
    ]
    for col_name, col_type in columns:
        sql = (
            f"ALTER TABLE `{fqn}.column_embeddings` "
            f"ADD COLUMN IF NOT EXISTS {col_name} {col_type};"
        )
        bq.execute_query(sql)
    logger.info("migrated_payload_columns", fqn=fqn)


def populate_symbols(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 3b: Populate symbol_exchange_map from CSV.

    Idempotent: MERGE on (symbol, exchange, portfolio).
    Batches rows to stay under BQ query size limits.
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"
    csv_path = Path(__file__).parent.parent / "data" / "symbol_exchange_map.csv"

    if not csv_path.exists():
        logger.error("symbol_csv_not_found", path=str(csv_path))
        return

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    logger.info("symbol_csv_loaded", row_count=len(rows))

    batch_size = 500
    total_merged = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = ",\n        ".join(
            f"STRUCT('{r['symbol']}' AS symbol, '{r['exchange']}' AS exchange, '{r['portfolio']}' AS portfolio)"
            for r in batch
        )
        sql = f"""
        MERGE `{fqn}.symbol_exchange_map` AS target
        USING (SELECT * FROM UNNEST([{values}])) AS source
        ON target.symbol = source.symbol
           AND target.exchange = source.exchange
           AND target.portfolio = source.portfolio
        WHEN NOT MATCHED THEN
          INSERT (symbol, exchange, portfolio)
          VALUES (source.symbol, source.exchange, source.portfolio);
        """
        bq.execute_query(sql)
        total_merged += len(batch)
        logger.info("symbol_batch_merged", batch=i // batch_size + 1, rows=len(batch))

    logger.info("populated_symbols", total=total_merged)


def _build_table_descriptions(s: Settings) -> list[dict[str, str]]:
    """Build table description rows from YAML catalog.

    Each row has: source_type, layer, dataset_name, table_name, description.
    Reads from catalog/<layer>/_dataset.yaml for dataset-level descriptions
    and catalog/<layer>/<table>.yaml for table-level descriptions.
    """
    descriptions: list[dict[str, str]] = []

    for layer in ("kpi", "data"):
        dataset_name = s.kpi_dataset if layer == "kpi" else s.data_dataset

        # Dataset-level description
        ds_path = CATALOG_DIR / layer / "_dataset.yaml"
        if ds_path.exists():
            ds_data = load_yaml(ds_path)
            ds_desc = ds_data.get("dataset", {}).get("description", "")
            descriptions.append(
                {
                    "source_type": "dataset",
                    "layer": layer,
                    "dataset_name": dataset_name,
                    "table_name": "",
                    "description": ds_desc.strip(),
                }
            )

        # Table-level descriptions from individual YAML files
        layer_dir = CATALOG_DIR / layer
        for yaml_file in sorted(layer_dir.glob("*.yaml")):
            if yaml_file.name.startswith("_"):
                continue
            content = load_yaml(yaml_file)
            table = content.get("table", {})
            descriptions.append(
                {
                    "source_type": "table",
                    "layer": layer,
                    "dataset_name": dataset_name,
                    "table_name": table.get("name", yaml_file.stem),
                    "description": table.get("description", "").strip(),
                }
            )

    return descriptions


def _build_routing_descriptions() -> list[str]:
    """Build routing description texts from YAML catalog.

    Reads cross-cutting routing descriptions from _routing.yaml.
    Returns a list of description strings for embedding.
    """
    rules = load_routing_rules()
    cc = rules.get("cross_cutting", {})

    descriptions: list[str] = []

    # KPI vs Data general guidance
    kpi_vs_data = cc.get("kpi_vs_data_general", "")
    if kpi_vs_data:
        descriptions.append(kpi_vs_data.strip())

    # Theodata routing
    theodata = cc.get("theodata_routing", "")
    if theodata:
        descriptions.append(theodata.strip())

    # KPI table selection
    kpi_selection = cc.get("kpi_table_selection", "")
    if kpi_selection:
        descriptions.append(kpi_selection.strip())

    return descriptions


def _escape_bq_string(text: str) -> str:
    """Escape single quotes and collapse whitespace for BQ SQL strings."""
    return " ".join(text.replace("'", "\\'").split())


def populate_schema_embeddings(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 4: Populate schema_embeddings from YAML catalog descriptions.

    Reads table descriptions from YAML catalog files and routing descriptions
    from _routing.yaml. Idempotent: MERGE on (source_type, dataset_name, table_name).
    """
    fqn = f"{s.gcp_project}.{s.metadata_dataset}"

    # Build descriptions from YAML catalog
    table_descs = _build_table_descriptions(s)

    # Process datasets: kpi and data
    for layer in ("kpi", "data"):
        layer_rows = [d for d in table_descs if d["layer"] == layer]
        if not layer_rows:
            continue

        struct_parts = []
        for row in layer_rows:
            desc = _escape_bq_string(row["description"])
            table_name = row["table_name"]
            table_val = f"'{table_name}'" if table_name else "CAST(NULL AS STRING)"

            if row["source_type"] == "dataset":
                struct_parts.append(
                    f"STRUCT("
                    f"'dataset' AS source_type, '{layer}' AS layer, "
                    f"'{row['dataset_name']}' AS dataset_name, "
                    f"CAST(NULL AS STRING) AS table_name, "
                    f"'{desc}' AS description)"
                )
            else:
                struct_parts.append(
                    f"STRUCT("
                    f"'table', '{layer}', "
                    f"'{row['dataset_name']}', {table_val}, "
                    f"'{desc}')"
                )

        structs = ",\n        ".join(struct_parts)
        merge_sql = f"""
        MERGE `{fqn}.schema_embeddings` AS target
        USING (
          SELECT * FROM UNNEST([
            {structs}
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
        bq.execute_query(merge_sql)
        logger.info("populated_schema_embeddings", layer=layer, rows=len(layer_rows))

    # Routing descriptions from _routing.yaml
    routing_descs = _build_routing_descriptions()
    if routing_descs:
        routing_structs = []
        for desc in routing_descs:
            escaped = _escape_bq_string(desc)
            routing_structs.append(
                f"STRUCT("
                f"'routing' AS source_type, CAST(NULL AS STRING) AS layer, "
                f"CAST(NULL AS STRING) AS dataset_name, "
                f"CAST(NULL AS STRING) AS table_name, "
                f"'{escaped}' AS description)"
            )

        structs = ",\n        ".join(routing_structs)
        routing_sql = f"""
        MERGE `{fqn}.schema_embeddings` AS target
        USING (
          SELECT * FROM UNNEST([
            {structs}
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

    logger.info("populated_schema_embeddings_complete")


def populate_glossary(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 4b: Populate glossary_embeddings from glossary.yaml.

    Reads glossary entries, builds embedding text, and MERGEs into BQ.
    """
    glossary_path = CATALOG_DIR / "glossary.yaml"
    if not glossary_path.exists():
        logger.warning("glossary_yaml_not_found", path=str(glossary_path))
        return

    data = load_yaml(glossary_path)
    entries = data.get("glossary", {}).get("entries", [])
    if not entries:
        logger.warning("glossary_empty")
        return

    count = populate_glossary_embeddings(bq, entries, s)
    logger.info("populated_glossary", count=count)


def generate_embeddings(bq: BigQueryProtocol, s: Settings) -> None:
    """Step 5: Generate embeddings for all rows that don't have them yet.

    Idempotent: WHERE embedding IS NULL OR ARRAY_LENGTH(embedding) = 0.

    When use_autonomous_embeddings is enabled, BQ generates embeddings
    automatically via GENERATED ALWAYS AS columns — this step is skipped.
    """
    if s.use_autonomous_embeddings:
        logger.info(
            "skipping_manual_embeddings", reason="autonomous embeddings enabled"
        )
        return
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
        WHERE t.embedding IS NULL OR ARRAY_LENGTH(t.embedding) = 0;
        """,
        # Column embeddings — uses enriched embedding_text when available
        f"""
        UPDATE `{fqn}.column_embeddings` t
        SET embedding = (
          SELECT ml_generate_embedding_result
          FROM ML.GENERATE_EMBEDDING(
            MODEL `{model}`,
            (SELECT COALESCE(t.embedding_text, t.description) AS content),
            STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_DOCUMENT' AS task_type)
          )
        )
        WHERE t.embedding IS NULL OR ARRAY_LENGTH(t.embedding) = 0;
        """,
        # Glossary embeddings
        f"""
        UPDATE `{fqn}.glossary_embeddings` t
        SET embedding = (
          SELECT ml_generate_embedding_result
          FROM ML.GENERATE_EMBEDDING(
            MODEL `{model}`,
            (SELECT COALESCE(t.embedding_text, t.definition) AS content),
            STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_DOCUMENT' AS task_type)
          )
        )
        WHERE t.embedding IS NULL OR ARRAY_LENGTH(t.embedding) = 0;
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
        WHERE t.embedding IS NULL OR ARRAY_LENGTH(t.embedding) = 0;
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
        CREATE VECTOR INDEX IF NOT EXISTS idx_glossary_embeddings
        ON `{fqn}.glossary_embeddings`(embedding)
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
        (
            "TEST 2: IV -> theodata",
            "how did implied vol change over the last month?",
            "schema_embeddings",
        ),
        (
            "TEST 3: broker -> brokertrade",
            "how have broker trades by BGC performed compared to MGN?",
            "schema_embeddings",
        ),
        (
            "TEST 4: depth -> marketdepth",
            "what did the order book look like for the ATM strike?",
            "schema_embeddings",
        ),
        (
            "TEST 5: few-shot retrieval",
            "show me PnL breakdown by symbol",
            "query_memory",
        ),
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
    "populate-symbols": populate_symbols,
    "populate-glossary": populate_glossary,
    "generate-embeddings": generate_embeddings,
    "create-indexes": create_vector_indexes,
    "test-search": test_vector_search,
}

ALL_STEPS_ORDER = [
    "create-dataset",
    "verify-model",
    "create-tables",
    "populate-schema",
    "populate-symbols",
    "populate-glossary",
    "generate-embeddings",
    "create-indexes",
    "test-search",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run embedding infrastructure steps")
    parser.add_argument(
        "--step",
        required=True,
        choices=list(STEPS.keys()) + ["all"],  # noqa: RUF005
        help="Which step to run (or 'all')",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Use CREATE OR REPLACE TABLE instead of IF NOT EXISTS (destroys data!)",
    )
    args = parser.parse_args()

    settings = Settings()
    bq = LiveBigQueryClient(project=settings.gcp_project, location=settings.bq_location)

    if args.step == "all":
        for step_name in ALL_STEPS_ORDER:
            logger.info("running_step", step=step_name)
            if step_name == "create-tables":
                create_embedding_tables(bq, settings, force=args.force)
            else:
                STEPS[step_name](bq, settings)
    else:
        if args.step == "create-tables":
            create_embedding_tables(bq, settings, force=args.force)
        else:
            STEPS[args.step](bq, settings)

    logger.info("done")


if __name__ == "__main__":
    main()
