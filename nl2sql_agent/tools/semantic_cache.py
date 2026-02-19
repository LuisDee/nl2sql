"""Semantic cache tool for NL2SQL agent.

Checks if a near-identical question was previously answered by searching
query_memory with a very tight cosine distance threshold. On cache hit,
returns the cached SQL so the agent can skip vector search and metadata
loading steps.

Uses the same VECTOR_SEARCH pattern as vector_search.py.
"""

from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.tools._deps import get_bq_service

logger = get_logger(__name__)

_CACHE_SEARCH_SQL = """
SELECT
    base.question AS cached_question,
    base.sql_query AS cached_sql,
    base.tables_used,
    base.dataset AS cached_dataset,
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
    top_k => 1,
    distance_type => 'COSINE'
)
ORDER BY distance ASC
LIMIT 1
"""


def check_semantic_cache(question: str) -> dict:
    """Check if this question was previously answered with high confidence.

    Searches query_memory for a near-exact match (cosine distance below
    the configured threshold). If found, returns the cached SQL so you
    can skip directly to dry_run_sql.

    Use this tool FIRST before vector_search_tables. If it returns
    cache_hit=True, use the cached_sql directly (skip steps 1-3).

    Args:
        question: The trader's natural language question.

    Returns:
        Dict with 'cache_hit' (bool). If True, includes 'cached_sql',
        'cached_question', 'cached_dataset', and 'distance'.
    """
    try:
        bq = get_bq_service()
    except RuntimeError:
        logger.warning("semantic_cache_no_bq_service")
        return {"cache_hit": False, "reason": "BigQuery service not available"}

    fq_metadata = f"{settings.gcp_project}.{settings.metadata_dataset}"
    sql = _CACHE_SEARCH_SQL.format(
        metadata_dataset=fq_metadata,
        embedding_model=settings.embedding_model_ref,
    )

    logger.info("semantic_cache_search_start", question=question[:100])

    try:
        rows = bq.query_with_params(
            sql,
            params=[{"name": "question", "type": "STRING", "value": question}],
        )

        if not rows:
            logger.info("semantic_cache_miss", reason="no_results")
            return {"cache_hit": False, "reason": "no matching queries in memory"}

        best = rows[0]
        distance = best.get("distance", 1.0)
        threshold = settings.semantic_cache_threshold

        if distance <= threshold:
            logger.info(
                "semantic_cache_hit",
                distance=distance,
                threshold=threshold,
                cached_question=best.get("cached_question", "")[:80],
            )
            return {
                "cache_hit": True,
                "cached_sql": best["cached_sql"],
                "cached_question": best["cached_question"],
                "cached_dataset": best.get("cached_dataset", ""),
                "tables_used": best.get("tables_used", []),
                "distance": distance,
            }
        else:
            logger.info(
                "semantic_cache_miss",
                distance=distance,
                threshold=threshold,
            )
            return {
                "cache_hit": False,
                "reason": f"closest match distance {distance} exceeds threshold {threshold}",
            }

    except Exception as e:
        logger.error("semantic_cache_error", error=str(e))
        return {"cache_hit": False, "reason": f"cache lookup error: {str(e)}"}
