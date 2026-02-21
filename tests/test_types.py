"""Tests for TypedDict return type annotations on all agent tools.

Verifies that tool return type annotations reference the TypedDicts
defined in nl2sql_agent.types, not bare `dict`.
"""

import typing

from nl2sql_agent.types import (
    CacheHitResult,
    CacheMissResult,
    ColumnSearchResult,
    DryRunInvalidResult,
    DryRunValidResult,
    ErrorResult,
    ExchangeMultipleResult,
    ExchangeResolvedResult,
    ExecuteSuccessResult,
    FewShotResult,
    MetadataSuccessResult,
    SaveQueryResult,
    VectorSearchResult,
)


class TestTypesImport:
    """All TypedDict types must be importable."""

    def test_all_types_importable(self):
        assert CacheHitResult is not None
        assert CacheMissResult is not None
        assert ExchangeResolvedResult is not None
        assert ExchangeMultipleResult is not None
        assert VectorSearchResult is not None
        assert ColumnSearchResult is not None
        assert FewShotResult is not None
        assert MetadataSuccessResult is not None
        assert DryRunValidResult is not None
        assert DryRunInvalidResult is not None
        assert ExecuteSuccessResult is not None
        assert SaveQueryResult is not None
        assert ErrorResult is not None


class TestToolReturnAnnotations:
    """Each tool must have a TypedDict-based return annotation, not bare `dict`."""

    def _get_return_annotation(self, func):
        hints = typing.get_type_hints(func)
        return hints.get("return")

    def test_check_semantic_cache_annotated(self):
        from nl2sql_agent.tools.semantic_cache import check_semantic_cache

        ret = self._get_return_annotation(check_semantic_cache)
        assert ret is not None and ret is not dict, (
            "check_semantic_cache must have a TypedDict return annotation, not bare dict"
        )

    def test_vector_search_tables_annotated(self):
        from nl2sql_agent.tools.vector_search import vector_search_tables

        ret = self._get_return_annotation(vector_search_tables)
        assert ret is not None and ret is not dict, (
            "vector_search_tables must have a TypedDict return annotation, not bare dict"
        )

    def test_vector_search_columns_annotated(self):
        from nl2sql_agent.tools.vector_search import vector_search_columns

        ret = self._get_return_annotation(vector_search_columns)
        assert ret is not None and ret is not dict, (
            "vector_search_columns must have a TypedDict return annotation, not bare dict"
        )

    def test_fetch_few_shot_examples_annotated(self):
        from nl2sql_agent.tools.vector_search import fetch_few_shot_examples

        ret = self._get_return_annotation(fetch_few_shot_examples)
        assert ret is not None and ret is not dict, (
            "fetch_few_shot_examples must have a TypedDict return annotation, not bare dict"
        )

    def test_load_yaml_metadata_annotated(self):
        from nl2sql_agent.tools.metadata_loader import load_yaml_metadata

        ret = self._get_return_annotation(load_yaml_metadata)
        assert ret is not None and ret is not dict, (
            "load_yaml_metadata must have a TypedDict return annotation, not bare dict"
        )

    def test_dry_run_sql_annotated(self):
        from nl2sql_agent.tools.sql_validator import dry_run_sql

        ret = self._get_return_annotation(dry_run_sql)
        assert ret is not None and ret is not dict, (
            "dry_run_sql must have a TypedDict return annotation, not bare dict"
        )

    def test_execute_sql_annotated(self):
        from nl2sql_agent.tools.sql_executor import execute_sql

        ret = self._get_return_annotation(execute_sql)
        assert ret is not None and ret is not dict, (
            "execute_sql must have a TypedDict return annotation, not bare dict"
        )

    def test_save_validated_query_annotated(self):
        from nl2sql_agent.tools.learning_loop import save_validated_query

        ret = self._get_return_annotation(save_validated_query)
        assert ret is not None and ret is not dict, (
            "save_validated_query must have a TypedDict return annotation, not bare dict"
        )

    def test_resolve_exchange_annotated(self):
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        ret = self._get_return_annotation(resolve_exchange)
        assert ret is not None and ret is not dict, (
            "resolve_exchange must have a TypedDict return annotation, not bare dict"
        )
