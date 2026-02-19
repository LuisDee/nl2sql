"""Vector search tools for semantic table and example routing.

These tools use BigQuery VECTOR_SEARCH to find the most relevant tables
and past validated queries for a given natural language question.

The embedding model and metadata dataset are configured via settings:
    settings.embedding_model_ref  (e.g. project.dataset.model)
    settings.metadata_dataset     (e.g. nl2sql_metadata)
"""

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.tools._deps import get_bq_service

logger = get_logger(__name__)


# --- VECTOR_SEARCH SQL Templates ---
# These are parameterised SQL strings. The @question parameter is injected
# via BigQuery query parameters (safe from SQL injection).
#
# IMPORTANT: We use RETRIEVAL_QUERY task type for the search query
# (not RETRIEVAL_DOCUMENT — that's for stored content).

_SCHEMA_SEARCH_SQL = """
SELECT
    base.source_type,
    base.layer,
    base.dataset_name,
    base.table_name,
    base.description,
    ROUND(distance, 4) AS distance
FROM VECTOR_SEARCH(
    (SELECT * FROM `{metadata_dataset}.schema_embeddings`),
    'embedding',
    (
        SELECT ml_generate_embedding_result AS embedding
        FROM ML.GENERATE_EMBEDDING(
            MODEL `{embedding_model}`,
            (SELECT @question AS content),
            STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output)
        )
    ),
    top_k => {top_k},
    distance_type => 'COSINE'
)
ORDER BY distance ASC
"""

_QUERY_MEMORY_SEARCH_SQL = """
SELECT
    base.question AS past_question,
    base.sql_query,
    base.tables_used,
    base.dataset AS past_dataset,
    base.complexity,
    base.routing_signal,
    ROUND(distance, 4) AS distance
FROM VECTOR_SEARCH(
    (SELECT * FROM `{metadata_dataset}.query_memory`),
    'embedding',
    (
        SELECT ml_generate_embedding_result AS embedding
        FROM ML.GENERATE_EMBEDDING(
            MODEL `{embedding_model}`,
            (SELECT @question AS content),
            STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output)
        )
    ),
    top_k => {top_k},
    distance_type => 'COSINE'
)
ORDER BY distance ASC
"""


def vector_search_tables(question: str) -> dict:
    """Find the most relevant BigQuery tables for a natural language question.

    Use this tool FIRST for every data question. It searches table and dataset
    descriptions using semantic similarity to determine which tables contain
    the data needed to answer the question. Results include table names,
    dataset names, descriptions, and relevance scores.

    Examples of when to use this tool:
    - "what was the edge on our trade?" → finds KPI tables
    - "how did implied vol change?" → finds theodata
    - "broker BGC vs MGN performance" → finds kpi brokertrade
    - "what levels were we quoting at 11:15?" → finds data quotertrade

    Args:
        question: The trader's natural language question about trading data.

    Returns:
        Dict with 'status' and 'results' (list of matching tables with
        source_type, layer, dataset_name, table_name, description, distance).
    """
    bq = get_bq_service()

    fq_metadata = f"{settings.gcp_project}.{settings.metadata_dataset}"
    sql = _SCHEMA_SEARCH_SQL.format(
        metadata_dataset=fq_metadata,
        embedding_model=settings.embedding_model_ref,
        top_k=settings.vector_search_top_k,
    )

    logger.info("vector_search_tables_start", question=question[:100])

    try:
        rows = bq.query_with_params(
            sql,
            params=[{"name": "question", "type": "STRING", "value": question}],
        )
        logger.info("vector_search_tables_complete", result_count=len(rows))
        return {"status": "success", "results": rows}
    except Exception as e:
        logger.error("vector_search_tables_error", error=str(e))
        return {"status": "error", "error_message": str(e), "results": []}


def fetch_few_shot_examples(question: str) -> dict:
    """Find similar past validated SQL queries to use as few-shot examples.

    Use this tool AFTER vector_search_tables to find proven question→SQL
    patterns that are similar to the current question. These examples help
    generate accurate SQL by showing correct table names, column names,
    WHERE clauses, and aggregation patterns.

    The results include the original question, the validated SQL query,
    which tables were used, complexity level, and a routing signal explaining
    why that table was chosen.

    Args:
        question: The trader's natural language question about trading data.

    Returns:
        Dict with 'status' and 'examples' (list of past validated queries
        with past_question, sql_query, tables_used, complexity, distance).
    """
    bq = get_bq_service()

    fq_metadata = f"{settings.gcp_project}.{settings.metadata_dataset}"
    sql = _QUERY_MEMORY_SEARCH_SQL.format(
        metadata_dataset=fq_metadata,
        embedding_model=settings.embedding_model_ref,
        top_k=settings.vector_search_top_k,
    )

    logger.info("fetch_few_shot_start", question=question[:100])

    try:
        rows = bq.query_with_params(
            sql,
            params=[{"name": "question", "type": "STRING", "value": question}],
        )
        logger.info("fetch_few_shot_complete", example_count=len(rows))
        return {"status": "success", "examples": rows}
    except Exception as e:
        logger.error("fetch_few_shot_error", error=str(e))
        return {"status": "error", "error_message": str(e), "examples": []}
