"""Populate BigQuery embedding tables from YAML catalog and example files.

Reads YAML catalog files and example files, then inserts rows into
{settings.metadata_dataset}.column_embeddings and query_memory.

Uses BigQueryProtocol for all BQ operations (testable with FakeBigQueryClient).
Uses settings for all project/dataset references -- no hardcoded values.

Usage:
    python scripts/populate_embeddings.py
"""

from nl2sql_agent.catalog_loader import (
    load_all_examples,
    load_all_table_yamls,
    resolve_example_sql,
    resolve_placeholders,
)
from nl2sql_agent.clients import LiveBigQueryClient
from nl2sql_agent.config import Settings
from nl2sql_agent.logging_config import get_logger, setup_logging
from nl2sql_agent.protocols import BigQueryProtocol

setup_logging()
logger = get_logger(__name__)

# BigQuery DML has a ~12MB query size limit; 500 rows per batch stays well under
BATCH_SIZE = 500


def _escape_sql_string(value: str) -> str:
    """Escape backslashes, single quotes, and newlines for BigQuery."""
    return (
        value.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _batched(iterable: list, n: int):
    """Yield successive n-sized chunks from iterable."""
    for i in range(0, len(iterable), n):
        yield iterable[i : i + n]


def build_embedding_text(
    table_name: str,
    column_name: str,
    column_type: str,
    layer: str,
    description: str,
    synonyms: list[str] | None,
    *,
    category: str | None = None,
    filterable: bool | None = None,
    example_values: list | None = None,
) -> str:
    """Build enriched text for column embedding.

    Produces a single string optimised for semantic search that includes
    table context, column metadata, description, and synonyms.

    Enrichment fields (when present):
    - [category] tag aids retrieval by signalling dimension/measure/time/identifier
    - example_values (≤5) for filterable dimensions aids exact-match queries

    Fields NOT embedded (generation context only, returned as payload):
    - formula, related_columns, typical_aggregation
    """
    header = f"{table_name}.{column_name} ({column_type}, {layer})"
    parts: list[str] = []

    if category:
        parts.append(f"[{category}]")

    if description:
        parts.append(description)

    if synonyms:
        parts.append(f"Also known as: {', '.join(synonyms)}")

    # Only include example_values for filterable dimensions (≤5 values)
    if filterable and category == "dimension" and example_values:
        vals = [str(v) for v in example_values[:5]]
        parts.append(f"Values: {', '.join(vals)}")

    if not parts:
        return header
    return f"{header}: {'. '.join(parts)}"


def populate_column_embeddings(
    bq: BigQueryProtocol, tables: list[dict], settings: Settings
) -> int:
    """Insert column-level descriptions into column_embeddings table.

    Idempotent: MERGE on (dataset_name, table_name, column_name).
    Batched: groups rows into UNNEST-based MERGE statements for throughput.

    Args:
        bq: BigQuery client (protocol).
        tables: List of parsed table YAML dicts.
        settings: Application settings (for project/dataset refs).

    Returns:
        Number of columns inserted/updated.
    """
    fqn = f"{settings.gcp_project}.{settings.metadata_dataset}"

    # Flatten all columns into a single list of row dicts
    rows = []
    for table_data in tables:
        t = table_data["table"]
        dataset_name = resolve_placeholders(
            t["dataset"],
            kpi_dataset=settings.kpi_dataset,
            data_dataset=settings.data_dataset,
        )
        table_name = t["name"]

        layer = t.get("layer", "")

        for col in t.get("columns", []):
            col_name = col["name"]
            col_type = col.get("type", "STRING")
            description = col.get("description", "").strip()
            synonyms = col.get("synonyms") or []
            embedding_text = build_embedding_text(
                table_name=table_name,
                column_name=col_name,
                column_type=col_type,
                layer=layer,
                description=description,
                synonyms=synonyms,
                category=col.get("category"),
                filterable=col.get("filterable"),
                example_values=col.get("example_values"),
            )
            rows.append(
                {
                    "dataset_name": dataset_name,
                    "table_name": table_name,
                    "column_name": col_name,
                    "column_type": col_type,
                    "description": description,
                    "synonyms": synonyms,
                    "embedding_text": embedding_text,
                }
            )

    count = 0
    for batch in _batched(rows, BATCH_SIZE):
        struct_rows = []
        for r in batch:
            desc = _escape_sql_string(r["description"])
            emb_text = _escape_sql_string(r["embedding_text"])
            synonyms_str = ", ".join(
                f"'{_escape_sql_string(s)}'" for s in r["synonyms"]
            )
            synonyms_array = (
                f"[{synonyms_str}]" if synonyms_str else "CAST([] AS ARRAY<STRING>)"
            )
            struct_rows.append(
                f"STRUCT('{r['dataset_name']}' AS dataset_name, "
                f"'{r['table_name']}' AS table_name, "
                f"'{r['column_name']}' AS column_name, "
                f"'{r['column_type']}' AS column_type, "
                f"'{desc}' AS description, "
                f"'{emb_text}' AS embedding_text, "
                f"{synonyms_array} AS synonyms)"
            )

        unnest_list = ",\n            ".join(struct_rows)

        sql = f"""
        MERGE `{fqn}.column_embeddings` AS target
        USING (
            SELECT * FROM UNNEST([
            {unnest_list}
            ])
        ) AS source
        ON target.dataset_name = source.dataset_name
           AND target.table_name = source.table_name
           AND target.column_name = source.column_name
        WHEN MATCHED THEN
          UPDATE SET description = source.description,
                     column_type = source.column_type,
                     synonyms = source.synonyms,
                     embedding_text = source.embedding_text,
                     embedding = NULL,
                     updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (dataset_name, table_name, column_name, column_type, description, synonyms, embedding_text)
          VALUES (source.dataset_name, source.table_name, source.column_name,
                  source.column_type, source.description, source.synonyms, source.embedding_text);
        """
        bq.execute_query(sql)
        count += len(batch)
        logger.info("column_embeddings_batch", batch_size=len(batch), total=count)

    logger.info("populated_column_embeddings", count=count)
    return count


def populate_query_memory(
    bq: BigQueryProtocol, examples: list[dict], settings: Settings
) -> int:
    """Insert validated Q->SQL pairs into query_memory table.

    Resolves {project} in SQL before storing. Idempotent: MERGE on question.
    Batched: groups rows into UNNEST-based MERGE statements for throughput.

    Args:
        bq: BigQuery client (protocol).
        examples: List of example dicts.
        settings: Application settings (for project/dataset refs).

    Returns:
        Number of examples inserted/updated.
    """
    fqn = f"{settings.gcp_project}.{settings.metadata_dataset}"

    # Pre-resolve all examples
    rows = []
    for ex in examples:
        resolved_sql = resolve_example_sql(
            ex["sql"],
            settings.gcp_project,
            kpi_dataset=settings.kpi_dataset,
            data_dataset=settings.data_dataset,
        )
        resolved_dataset = resolve_placeholders(
            ex["dataset"],
            kpi_dataset=settings.kpi_dataset,
            data_dataset=settings.data_dataset,
        )
        rows.append(
            {
                "question": ex["question"],
                "sql_query": resolved_sql.strip(),
                "tables_used": ex["tables_used"],
                "dataset": resolved_dataset,
                "complexity": ex.get("complexity", "simple"),
                "routing_signal": ex.get("routing_signal", ""),
                "validated_by": ex.get("validated_by", ""),
            }
        )

    count = 0
    for batch in _batched(rows, BATCH_SIZE):
        struct_rows = []
        for r in batch:
            question = _escape_sql_string(r["question"])
            sql_query = _escape_sql_string(r["sql_query"])
            tables_str = ", ".join(f"'{t}'" for t in r["tables_used"])
            routing_signal = _escape_sql_string(r["routing_signal"])

            struct_rows.append(
                f"STRUCT('{question}' AS question, "
                f"'{sql_query}' AS sql_query, "
                f"[{tables_str}] AS tables_used, "
                f"'{r['dataset']}' AS dataset, "
                f"'{r['complexity']}' AS complexity, "
                f"'{routing_signal}' AS routing_signal, "
                f"'{r['validated_by']}' AS validated_by)"
            )

        unnest_list = ",\n            ".join(struct_rows)

        merge_sql = f"""
        MERGE `{fqn}.query_memory` AS target
        USING (
            SELECT * FROM UNNEST([
            {unnest_list}
            ])
        ) AS source
        ON target.question = source.question
        WHEN MATCHED THEN
          UPDATE SET sql_query = source.sql_query,
                     tables_used = source.tables_used,
                     dataset = source.dataset,
                     complexity = source.complexity,
                     routing_signal = source.routing_signal,
                     validated_by = source.validated_by,
                     embedding = NULL,
                     validated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (question, sql_query, tables_used, dataset, complexity, routing_signal, validated_by)
          VALUES (source.question, source.sql_query, source.tables_used,
                  source.dataset, source.complexity, source.routing_signal, source.validated_by);
        """
        bq.execute_query(merge_sql)
        count += len(batch)
        logger.info("query_memory_batch", batch_size=len(batch), total=count)

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
