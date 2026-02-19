"""Vector search tools for semantic table and example routing.

These tools use BigQuery VECTOR_SEARCH to find the most relevant tables
and past validated queries for a given natural language question.

Performance: A combined CTE generates the embedding ONCE, then runs both
schema and query_memory VECTOR_SEARCH in a single round-trip.  The result
is cached in _deps so fetch_few_shot_examples() is a Python-level cache hit
when called for the same question.

The embedding model and metadata dataset are configured via settings:
    settings.embedding_model_ref  (e.g. project.dataset.model)
    settings.metadata_dataset     (e.g. nl2sql_metadata)
"""

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.tools._deps import (
    cache_vector_result,
    get_bq_service,
    get_cached_vector_result,
)

logger = get_logger(__name__)


# --- VECTOR_SEARCH SQL Templates ---
# These are parameterised SQL strings. The @question parameter is injected
# via BigQuery query parameters (safe from SQL injection).
#
# IMPORTANT: We use RETRIEVAL_QUERY task type for the search query
# (not RETRIEVAL_DOCUMENT — that's for stored content).

# Combined query: single embedding CTE, two VECTOR_SEARCH operations.
_COMBINED_SEARCH_SQL = """
WITH question_embedding AS (
    SELECT ml_generate_embedding_result AS embedding
    FROM ML.GENERATE_EMBEDDING(
        MODEL `{embedding_model}`,
        (SELECT @question AS content),
        STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output)
    )
),
schema_results AS (
    SELECT
        'schema' AS search_type,
        base.source_type,
        base.layer,
        base.dataset_name,
        base.table_name,
        base.description,
        ROUND(distance, 4) AS distance
    FROM VECTOR_SEARCH(
        (SELECT * FROM `{metadata_dataset}.schema_embeddings`),
        'embedding',
        (SELECT embedding FROM question_embedding),
        top_k => {top_k},
        distance_type => 'COSINE'
    )
),
example_results AS (
    SELECT
        'example' AS search_type,
        '' AS source_type,
        '' AS layer,
        base.dataset AS dataset_name,
        base.question AS table_name,
        base.sql_query AS description,
        ROUND(distance, 4) AS distance
    FROM VECTOR_SEARCH(
        (SELECT * FROM `{metadata_dataset}.query_memory`),
        'embedding',
        (SELECT embedding FROM question_embedding),
        top_k => {top_k},
        distance_type => 'COSINE'
    )
)
SELECT * FROM schema_results
UNION ALL
SELECT * FROM example_results
ORDER BY search_type, distance ASC
"""

# Fallback: schema-only search (used if combined query fails).
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

# Fallback: examples-only search (used if cache miss after failed combined).
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


def _split_combined_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split combined query rows into schema results and example results."""
    schema_rows = []
    example_rows = []
    for row in rows:
        search_type = row.get("search_type", "")
        if search_type == "schema":
            schema_rows.append({
                "source_type": row["source_type"],
                "layer": row["layer"],
                "dataset_name": row["dataset_name"],
                "table_name": row["table_name"],
                "description": row["description"],
                "distance": row["distance"],
            })
        elif search_type == "example":
            example_rows.append({
                "past_question": row["table_name"],
                "sql_query": row["description"],
                "tables_used": row.get("tables_used", ""),
                "past_dataset": row["dataset_name"],
                "complexity": row.get("complexity", ""),
                "routing_signal": row.get("routing_signal", ""),
                "distance": row["distance"],
            })
    return schema_rows, example_rows


def vector_search_tables(question: str) -> dict:
    """Find the most relevant BigQuery tables for a natural language question.

    Use this tool FIRST for every data question. It searches table and dataset
    descriptions using semantic similarity to determine which tables contain
    the data needed to answer the question. Results include table names,
    dataset names, descriptions, and relevance scores.

    This also pre-fetches few-shot examples in the same query to avoid a
    second embedding call. The examples are cached for fetch_few_shot_examples().

    Examples of when to use this tool:
    - "what was the edge on our trade?" -> finds KPI tables
    - "how did implied vol change?" -> finds theodata
    - "broker BGC vs MGN performance" -> finds kpi brokertrade
    - "what levels were we quoting at 11:15?" -> finds data quotertrade

    Args:
        question: The trader's natural language question about trading data.

    Returns:
        Dict with 'status' and 'results' (list of matching tables with
        source_type, layer, dataset_name, table_name, description, distance).
    """
    bq = get_bq_service()

    fq_metadata = f"{settings.gcp_project}.{settings.metadata_dataset}"
    combined_sql = _COMBINED_SEARCH_SQL.format(
        metadata_dataset=fq_metadata,
        embedding_model=settings.embedding_model_ref,
        top_k=settings.vector_search_top_k,
    )

    logger.info("vector_search_tables_start", question=question[:100])

    try:
        rows = bq.query_with_params(
            combined_sql,
            params=[{"name": "question", "type": "STRING", "value": question}],
        )
        schema_rows, example_rows = _split_combined_rows(rows)

        # Cache example results for fetch_few_shot_examples()
        cache_vector_result(question, {"examples": example_rows})

        logger.info(
            "vector_search_combined_complete",
            schema_count=len(schema_rows),
            example_count=len(example_rows),
        )
        return {"status": "success", "results": schema_rows}

    except Exception as e:
        logger.warning("combined_search_failed_falling_back", error=str(e))
        # Fallback: schema-only search
        try:
            fallback_sql = _SCHEMA_SEARCH_SQL.format(
                metadata_dataset=fq_metadata,
                embedding_model=settings.embedding_model_ref,
                top_k=settings.vector_search_top_k,
            )
            rows = bq.query_with_params(
                fallback_sql,
                params=[{"name": "question", "type": "STRING", "value": question}],
            )
            logger.info("vector_search_tables_fallback_complete", result_count=len(rows))
            return {"status": "success", "results": rows}
        except Exception as e2:
            logger.error("vector_search_tables_error", error=str(e2))
            return {"status": "error", "error_message": str(e2), "results": []}


def fetch_few_shot_examples(question: str) -> dict:
    """Find similar past validated SQL queries to use as few-shot examples.

    Use this tool AFTER vector_search_tables to find proven question->SQL
    patterns that are similar to the current question. These examples help
    generate accurate SQL by showing correct table names, column names,
    WHERE clauses, and aggregation patterns.

    If vector_search_tables() was already called for the same question,
    examples are returned from cache (no extra embedding call).

    The results include the original question, the validated SQL query,
    which tables were used, complexity level, and a routing signal explaining
    why that table was chosen.

    Args:
        question: The trader's natural language question about trading data.

    Returns:
        Dict with 'status' and 'examples' (list of past validated queries
        with past_question, sql_query, tables_used, complexity, distance).
    """
    # Check cache first (populated by vector_search_tables combined query)
    cached = get_cached_vector_result(question)
    if cached is not None:
        examples = cached.get("examples", [])
        logger.info("fetch_few_shot_cache_hit", example_count=len(examples))
        return {"status": "success", "examples": examples}

    # Cache miss: fall back to independent query
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
