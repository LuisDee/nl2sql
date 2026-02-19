"""NL2SQL agent tools package.

All tools are plain functions that ADK wraps as FunctionTool automatically.
Import and use init_bq_service() in agent.py to set up dependencies.
"""

from nl2sql_agent.tools._deps import init_bq_service
from nl2sql_agent.tools.vector_search import vector_search_tables, fetch_few_shot_examples
from nl2sql_agent.tools.metadata_loader import load_yaml_metadata
from nl2sql_agent.tools.sql_validator import dry_run_sql
from nl2sql_agent.tools.sql_executor import execute_sql
from nl2sql_agent.tools.learning_loop import save_validated_query

__all__ = [
    "init_bq_service",
    "vector_search_tables",
    "fetch_few_shot_examples",
    "load_yaml_metadata",
    "dry_run_sql",
    "execute_sql",
    "save_validated_query",
]
