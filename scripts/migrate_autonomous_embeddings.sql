-- Migration: Enable autonomous embedding generation in BigQuery
--
-- BigQuery's GENERATED ALWAYS AS ... STORED columns automatically generate
-- embeddings on INSERT, removing the need for the manual UPDATE step.
-- The asynchronous = TRUE option means embeddings are generated in the
-- background, not blocking the INSERT.
--
-- PREREQUISITES:
--   1. Embedding model must be accessible from the metadata dataset
--   2. Vertex AI connection must be set up
--
-- USAGE:
--   Replace {project}, {metadata_dataset}, and {embedding_model} with actual values.
--   Run in BigQuery console or via bq CLI.
--
-- ROLLBACK:
--   ALTER TABLE `{project}.{metadata_dataset}.query_memory`
--   DROP COLUMN IF EXISTS embedding_auto;
--
-- After running this migration, set USE_AUTONOMOUS_EMBEDDINGS=true in .env

-- query_memory: auto-embed the question field
ALTER TABLE `{project}.{metadata_dataset}.query_memory`
ADD COLUMN IF NOT EXISTS embedding_auto ARRAY<FLOAT64>
GENERATED ALWAYS AS (
  ML.GENERATE_EMBEDDING(
    MODEL `{embedding_model}`,
    (SELECT question AS content),
    STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output)
  ).ml_generate_embedding_result
) STORED OPTIONS(asynchronous = TRUE);

-- schema_embeddings: auto-embed the description field
ALTER TABLE `{project}.{metadata_dataset}.schema_embeddings`
ADD COLUMN IF NOT EXISTS embedding_auto ARRAY<FLOAT64>
GENERATED ALWAYS AS (
  ML.GENERATE_EMBEDDING(
    MODEL `{embedding_model}`,
    (SELECT description AS content),
    STRUCT('RETRIEVAL_DOCUMENT' AS task_type, TRUE AS flatten_json_output)
  ).ml_generate_embedding_result
) STORED OPTIONS(asynchronous = TRUE);

-- column_embeddings: auto-embed the enriched text (or fallback to description)
ALTER TABLE `{project}.{metadata_dataset}.column_embeddings`
ADD COLUMN IF NOT EXISTS embedding_auto ARRAY<FLOAT64>
GENERATED ALWAYS AS (
  ML.GENERATE_EMBEDDING(
    MODEL `{embedding_model}`,
    (SELECT COALESCE(embedding_text, description) AS content),
    STRUCT('RETRIEVAL_DOCUMENT' AS task_type, TRUE AS flatten_json_output)
  ).ml_generate_embedding_result
) STORED OPTIONS(asynchronous = TRUE);
