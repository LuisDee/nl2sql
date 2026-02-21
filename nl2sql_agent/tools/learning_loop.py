"""Learning loop tool — saves validated question→SQL pairs for future retrieval.

When a trader confirms a query result is correct, this tool saves the
question and SQL to query_memory. The embedding is generated immediately
so the pair is available for future few-shot retrieval.
"""

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.tools._deps import get_bq_service

logger = get_logger(__name__)

_INSERT_SQL = """
INSERT INTO `{metadata_dataset}.query_memory`
    (question, sql_query, tables_used, dataset, complexity, routing_signal, validated_by)
VALUES
    (@question, @sql_query, SPLIT(@tables_used, ','), @dataset, @complexity, @routing_signal, 'trader')
"""

_EMBED_NEW_ROWS_SQL = """
UPDATE `{metadata_dataset}.query_memory` t
SET embedding = (
    SELECT ml_generate_embedding_result
    FROM ML.GENERATE_EMBEDDING(
        MODEL `{embedding_model}`,
        (SELECT t.question AS content),
        STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_DOCUMENT' AS task_type)
    )
)
WHERE t.embedding IS NULL OR ARRAY_LENGTH(t.embedding) = 0
"""


def save_validated_query(
    question: str,
    sql_query: str,
    tables_used: str,
    dataset: str,
    complexity: str,
    routing_signal: str,
) -> dict:
    """Save a validated question→SQL pair to query memory for future retrieval.

    Call this tool when the trader confirms the query result was correct.
    The pair will be embedded and available for future few-shot retrieval,
    improving accuracy for similar questions.

    Args:
        question: The original natural language question.
        sql_query: The SQL query that produced correct results.
        tables_used: Comma-separated list of table names used.
            Example: "markettrade" or "markettrade,brokertrade"
        dataset: The dataset used. Example: "nl2sql_omx_kpi" or "nl2sql_omx_data".
        complexity: Query complexity level: "simple", "medium", or "complex".
        routing_signal: Brief note on why this table was chosen.

    Returns:
        Dict with 'status' and confirmation message.
    """
    bq = get_bq_service()

    fq_metadata = f"{settings.gcp_project}.{settings.metadata_dataset}"

    logger.info(
        "save_validated_query_start",
        question=question[:100],
        tables_used=tables_used,
    )

    # Step 1: Insert the row
    insert_sql = _INSERT_SQL.format(metadata_dataset=fq_metadata)

    try:
        bq.query_with_params(
            insert_sql,
            params=[
                {"name": "question", "type": "STRING", "value": question},
                {"name": "sql_query", "type": "STRING", "value": sql_query},
                {"name": "tables_used", "type": "STRING", "value": tables_used},
                {"name": "dataset", "type": "STRING", "value": dataset},
                {"name": "complexity", "type": "STRING", "value": complexity},
                {"name": "routing_signal", "type": "STRING", "value": routing_signal},
            ],
        )
    except Exception as e:
        logger.error("save_validated_query_insert_error", error=str(e))
        return {"status": "error", "error_message": f"Failed to insert: {e}"}

    tables_array = [t.strip() for t in tables_used.split(",")]

    # Step 2: Generate embedding for the new row
    # Skip when autonomous embeddings are enabled (BQ generates them automatically)
    if settings.use_autonomous_embeddings:
        logger.info(
            "save_validated_query_complete",
            tables_used=tables_array,
            embedding="autonomous",
        )
        return {
            "status": "success",
            "message": (
                f"Saved validated query. Tables: {tables_array}. "
                "Embedding will be generated automatically by BigQuery."
            ),
        }

    embed_sql = _EMBED_NEW_ROWS_SQL.format(
        metadata_dataset=fq_metadata,
        embedding_model=settings.embedding_model_ref,
    )

    try:
        bq.execute_query(embed_sql)
        logger.info(
            "save_validated_query_complete",
            tables_used=tables_array,
        )
        return {
            "status": "success",
            "message": (
                f"Saved validated query. Tables: {tables_array}. "
                "This will improve future answers to similar questions."
            ),
        }
    except Exception as e:
        # Insert succeeded but embedding failed — still a partial success
        logger.warning("save_validated_query_embed_error", error=str(e))
        return {
            "status": "partial_success",
            "message": (
                f"Query saved but embedding generation failed: {e}. "
                "The query is stored and will be embedded on next batch run."
            ),
        }
