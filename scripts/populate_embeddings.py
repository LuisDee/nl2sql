"""Populate BigQuery embedding tables from YAML catalog and example files.

Reads YAML catalog files and example files, then inserts rows into
{settings.metadata_dataset}.column_embeddings and query_memory.

Uses BigQueryProtocol for all BQ operations (testable with FakeBigQueryClient).
Uses settings for all project/dataset references -- no hardcoded values.

Usage:
    python scripts/populate_embeddings.py
"""

from nl2sql_agent.catalog_loader import (
    load_all_table_yamls,
    load_all_examples,
    resolve_example_sql,
)
from nl2sql_agent.config import Settings
from nl2sql_agent.logging_config import setup_logging, get_logger
from nl2sql_agent.protocols import BigQueryProtocol
from nl2sql_agent.clients import LiveBigQueryClient

setup_logging()
logger = get_logger(__name__)


def _escape_sql_string(value: str) -> str:
    """Escape single quotes for use in BigQuery SQL string literals."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def populate_column_embeddings(bq: BigQueryProtocol, tables: list[dict], settings: Settings) -> int:
    """Insert column-level descriptions into column_embeddings table.

    Idempotent: MERGE on (dataset_name, table_name, column_name).

    Args:
        bq: BigQuery client (protocol).
        tables: List of parsed table YAML dicts.
        settings: Application settings (for project/dataset refs).

    Returns:
        Number of columns inserted/updated.
    """
    fqn = f"{settings.gcp_project}.{settings.metadata_dataset}"
    count = 0
    for table_data in tables:
        t = table_data["table"]
        dataset_name = t["dataset"]
        table_name = t["name"]

        for col in t.get("columns", []):
            col_name = col["name"]
            col_type = col.get("type", "STRING")
            description = _escape_sql_string(col.get("description", "").strip())
            synonyms = col.get("synonyms") or []
            synonyms_str = ", ".join(f"'{_escape_sql_string(s)}'" for s in synonyms)
            synonyms_array = f"[{synonyms_str}]" if synonyms_str else "[]"

            sql = f"""
            MERGE `{fqn}.column_embeddings` AS target
            USING (SELECT '{dataset_name}' AS dataset_name, '{table_name}' AS table_name,
                          '{col_name}' AS column_name) AS source
            ON target.dataset_name = source.dataset_name
               AND target.table_name = source.table_name
               AND target.column_name = source.column_name
            WHEN MATCHED THEN
              UPDATE SET description = '{description}',
                         column_type = '{col_type}',
                         synonyms = {synonyms_array},
                         embedding = NULL,
                         updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              INSERT (dataset_name, table_name, column_name, column_type, description, synonyms)
              VALUES (source.dataset_name, source.table_name, source.column_name,
                      '{col_type}', '{description}', {synonyms_array});
            """
            bq.execute_query(sql)
            count += 1

    logger.info("populated_column_embeddings", count=count)
    return count


def populate_query_memory(bq: BigQueryProtocol, examples: list[dict], settings: Settings) -> int:
    """Insert validated Q->SQL pairs into query_memory table.

    Resolves {project} in SQL before storing. Idempotent: MERGE on question.

    Args:
        bq: BigQuery client (protocol).
        examples: List of example dicts.
        settings: Application settings (for project/dataset refs).

    Returns:
        Number of examples inserted/updated.
    """
    fqn = f"{settings.gcp_project}.{settings.metadata_dataset}"
    count = 0
    for ex in examples:
        question = _escape_sql_string(ex["question"])
        resolved_sql = resolve_example_sql(ex["sql"], settings.gcp_project)
        sql_query = _escape_sql_string(resolved_sql.strip())
        tables_str = ", ".join(f"'{t}'" for t in ex["tables_used"])
        dataset = ex["dataset"]
        complexity = ex.get("complexity", "simple")
        routing_signal = _escape_sql_string(ex.get("routing_signal", ""))
        validated_by = ex.get("validated_by", "")

        merge_sql = f"""
        MERGE `{fqn}.query_memory` AS target
        USING (SELECT '{question}' AS question) AS source
        ON target.question = source.question
        WHEN MATCHED THEN
          UPDATE SET sql_query = '{sql_query}',
                     tables_used = [{tables_str}],
                     dataset = '{dataset}',
                     complexity = '{complexity}',
                     routing_signal = '{routing_signal}',
                     validated_by = '{validated_by}',
                     embedding = NULL,
                     validated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (question, sql_query, tables_used, dataset, complexity, routing_signal, validated_by)
          VALUES (source.question, '{sql_query}', [{tables_str}],
                  '{dataset}', '{complexity}', '{routing_signal}', '{validated_by}');
        """
        bq.execute_query(merge_sql)
        count += 1

    logger.info("populated_query_memory", count=count)
    return count


def main() -> None:
    settings = Settings()
    bq = LiveBigQueryClient(project=settings.gcp_project, location=settings.bq_location)

    logger.info("loading_yaml_catalog")
    tables = load_all_table_yamls()
    logger.info("loaded_tables", count=len(tables))

    logger.info("loading_examples")
    examples = load_all_examples()
    logger.info("loaded_examples", count=len(examples))

    col_count = populate_column_embeddings(bq, tables, settings)
    ex_count = populate_query_memory(bq, examples, settings)

    logger.info("populate_complete", columns=col_count, examples=ex_count)
    print(f"Populated {col_count} column embeddings, {ex_count} query memory rows")


if __name__ == "__main__":
    main()
