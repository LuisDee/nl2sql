"""Populate BigQuery glossary_embeddings table from glossary.yaml.

Reads catalog/glossary.yaml, builds embedding text for each entry,
and MERGEs into {settings.metadata_dataset}.glossary_embeddings.

Uses BigQueryProtocol for all BQ operations (testable with FakeBigQueryClient).

Usage:
    python scripts/populate_glossary.py
"""

from nl2sql_agent.catalog_loader import CATALOG_DIR, load_yaml
from nl2sql_agent.clients import LiveBigQueryClient
from nl2sql_agent.config import Settings
from nl2sql_agent.logging_config import get_logger, setup_logging
from nl2sql_agent.protocols import BigQueryProtocol

setup_logging()
logger = get_logger(__name__)

BATCH_SIZE = 500


def _escape_sql_string(value: str) -> str:
    """Escape backslashes, single quotes, and newlines for BigQuery."""
    return (
        value.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def build_glossary_embedding_text(
    name: str,
    definition: str,
    synonyms: list[str],
) -> str:
    """Build embedding text for a glossary entry.

    Template: {name}: {definition}. Also known as: {synonyms}
    Synonyms section omitted when empty.
    """
    text = f"{name}: {definition}"
    if synonyms:
        text += f" Also known as: {', '.join(synonyms)}"
    return text


def populate_glossary_embeddings(
    bq: BigQueryProtocol, entries: list[dict], settings: Settings
) -> int:
    """Insert glossary entries into glossary_embeddings table.

    Idempotent: MERGE on name.
    Batched: groups rows into UNNEST-based MERGE statements.

    Args:
        bq: BigQuery client (protocol).
        entries: List of glossary entry dicts from glossary.yaml.
        settings: Application settings (for project/dataset refs).

    Returns:
        Number of entries inserted/updated.
    """
    fqn = f"{settings.gcp_project}.{settings.metadata_dataset}"

    rows = []
    for entry in entries:
        name = entry["name"]
        definition = entry["definition"]
        synonyms = entry.get("synonyms") or []
        related_columns = entry.get("related_columns") or []

        embedding_text = build_glossary_embedding_text(name, definition, synonyms)

        rows.append(
            {
                "name": name,
                "definition": definition,
                "embedding_text": embedding_text,
                "synonyms": synonyms,
                "related_columns": related_columns,
                "category": entry.get("category"),
                "sql_pattern": entry.get("sql_pattern"),
            }
        )

    count = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        struct_rows = []

        for r in batch:
            name = _escape_sql_string(r["name"])
            definition = _escape_sql_string(r["definition"])
            emb_text = _escape_sql_string(r["embedding_text"])

            synonyms_str = ", ".join(
                f"'{_escape_sql_string(s)}'" for s in r["synonyms"]
            )
            synonyms_array = (
                f"[{synonyms_str}]" if synonyms_str else "CAST([] AS ARRAY<STRING>)"
            )

            rc_str = ", ".join(
                f"'{_escape_sql_string(c)}'" for c in r["related_columns"]
            )
            rc_array = f"[{rc_str}]" if rc_str else "CAST([] AS ARRAY<STRING>)"

            cat = (
                f"'{_escape_sql_string(r['category'])}'"
                if r["category"]
                else "CAST(NULL AS STRING)"
            )
            sql_pat = (
                f"'{_escape_sql_string(r['sql_pattern'])}'"
                if r["sql_pattern"]
                else "CAST(NULL AS STRING)"
            )

            struct_rows.append(
                f"STRUCT('{name}' AS name, "
                f"'{definition}' AS definition, "
                f"'{emb_text}' AS embedding_text, "
                f"{synonyms_array} AS synonyms, "
                f"{rc_array} AS related_columns, "
                f"{cat} AS category, "
                f"{sql_pat} AS sql_pattern)"
            )

        unnest_list = ",\n            ".join(struct_rows)

        sql = f"""
        MERGE `{fqn}.glossary_embeddings` AS target
        USING (
            SELECT * FROM UNNEST([
            {unnest_list}
            ])
        ) AS source
        ON target.name = source.name
        WHEN MATCHED THEN
          UPDATE SET definition = source.definition,
                     embedding_text = source.embedding_text,
                     synonyms = source.synonyms,
                     related_columns = source.related_columns,
                     category = source.category,
                     sql_pattern = source.sql_pattern,
                     embedding = NULL,
                     updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (name, definition, embedding_text, synonyms, related_columns,
                  category, sql_pattern)
          VALUES (source.name, source.definition, source.embedding_text,
                  source.synonyms, source.related_columns, source.category,
                  source.sql_pattern);
        """
        bq.execute_query(sql)
        count += len(batch)
        logger.info("glossary_embeddings_batch", batch_size=len(batch), total=count)

    logger.info("populated_glossary_embeddings", count=count)
    return count


def main() -> None:
    settings = Settings()
    bq = LiveBigQueryClient(project=settings.gcp_project, location=settings.bq_location)

    logger.info("loading_glossary")
    glossary_path = CATALOG_DIR / "glossary.yaml"
    data = load_yaml(glossary_path)
    entries = data.get("glossary", {}).get("entries", [])
    logger.info("loaded_glossary_entries", count=len(entries))

    count = populate_glossary_embeddings(bq, entries, settings)
    print(f"Populated {count} glossary embeddings")


if __name__ == "__main__":
    main()
