"""Tests for exchange resolution: registry loader + resolve_exchange tool."""

from unittest.mock import patch

import pytest

from nl2sql_agent.catalog_loader import (
    clear_exchange_cache,
    clear_yaml_cache,
    load_exchange_registry,
)


class TestLoadExchangeRegistry:
    """Tests for catalog_loader.load_exchange_registry()."""

    def setup_method(self):
        clear_exchange_cache()
        clear_yaml_cache()

    def test_loads_all_10_exchanges(self):
        registry = load_exchange_registry()
        assert len(registry["exchanges"]) == 10

    def test_has_default_exchange(self):
        registry = load_exchange_registry()
        assert registry["default_exchange"] == "omx"

    def test_each_exchange_has_required_keys(self):
        registry = load_exchange_registry()
        for name, info in registry["exchanges"].items():
            assert "aliases" in info, f"{name} missing aliases"
            assert "kpi_dataset" in info, f"{name} missing kpi_dataset"
            assert "data_dataset" in info, f"{name} missing data_dataset"

    def test_canonical_name_in_aliases(self):
        """Each exchange's canonical name should appear in its own aliases."""
        registry = load_exchange_registry()
        for name, info in registry["exchanges"].items():
            assert name in info["aliases"], f"{name} not in its own aliases"

    def test_aliases_are_lowercase(self):
        registry = load_exchange_registry()
        for name, info in registry["exchanges"].items():
            for alias in info["aliases"]:
                assert alias == alias.lower(), (
                    f"Alias '{alias}' for {name} not lowercase"
                )

    def test_dataset_naming_convention(self):
        registry = load_exchange_registry()
        for name, info in registry["exchanges"].items():
            assert info["kpi_dataset"] == f"nl2sql_{name}_kpi"
            assert info["data_dataset"] == f"nl2sql_{name}_data"

    def test_file_not_found_raises(self, tmp_path):
        clear_exchange_cache()
        clear_yaml_cache()
        with (
            patch("nl2sql_agent.catalog_loader.CATALOG_DIR", tmp_path),
            pytest.raises(FileNotFoundError),
        ):
            load_exchange_registry()

    def test_caching_works(self):
        """Second call should use cache (no file re-read)."""
        r1 = load_exchange_registry()
        r2 = load_exchange_registry()
        assert r1 is r2  # Same object reference = cached


class TestResolveExchangeAliasLookup:
    """Tests for exchange_resolver.resolve_exchange — alias tier."""

    def test_canonical_name(self):
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        result = resolve_exchange("brazil")
        assert result["status"] == "resolved"
        assert result["exchange"] == "brazil"
        assert result["kpi_dataset"] == "nl2sql_brazil_kpi"
        assert result["data_dataset"] == "nl2sql_brazil_data"

    def test_alias_lookup(self):
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        result = resolve_exchange("bovespa")
        assert result["status"] == "resolved"
        assert result["exchange"] == "brazil"

    def test_case_insensitive(self):
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        result = resolve_exchange("BOVESPA")
        assert result["status"] == "resolved"
        assert result["exchange"] == "brazil"

    def test_default_fallback(self):
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        result = resolve_exchange("totally_unknown_xyz")
        assert result["status"] == "default"
        assert result["exchange"] == "omx"

    def test_all_exchanges_resolvable(self):
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        expected = [
            "arb",
            "asx",
            "eurex",
            "canada",
            "euronext",
            "brazil",
            "ice",
            "omx",
            "nse",
            "korea",
        ]
        for name in expected:
            result = resolve_exchange(name)
            assert result["status"] == "resolved", f"Failed to resolve {name}"
            assert result["exchange"] == name

    def test_result_has_required_keys(self):
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        result = resolve_exchange("asx")
        for key in ["status", "exchange", "kpi_dataset", "data_dataset"]:
            assert key in result, f"Missing key: {key}"

    def test_tsx_resolves_to_canada(self):
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        result = resolve_exchange("tsx")
        assert result["exchange"] == "canada"

    def test_krx_resolves_to_korea(self):
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        result = resolve_exchange("krx")
        assert result["exchange"] == "korea"


class TestResolveExchangeSymbolLookup:
    """Tests for exchange_resolver.resolve_exchange — symbol tier (BQ)."""

    def test_symbol_single_exchange(self):
        """Symbol found on exactly one exchange."""
        from nl2sql_agent.tools import _deps
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        class MockBQ:
            def query_with_params(self, sql, params=None):
                return [{"exchange": "brazil", "portfolio": "LATAM_OPT"}]

        _deps._bq_service = MockBQ()
        try:
            result = resolve_exchange("VALE3")
            assert result["status"] == "resolved"
            assert result["exchange"] == "brazil"
        finally:
            _deps._bq_service = None

    def test_symbol_multiple_exchanges(self):
        """Symbol found on multiple exchanges returns all matches."""
        from nl2sql_agent.tools import _deps
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        class MockBQ:
            def query_with_params(self, sql, params=None):
                return [
                    {"exchange": "arb", "portfolio": "ARB_PORT"},
                    {"exchange": "eurex", "portfolio": "EU_PORT"},
                    {"exchange": "omx", "portfolio": "OMX_PORT"},
                ]

        _deps._bq_service = MockBQ()
        try:
            result = resolve_exchange("ABBS")
            assert result["status"] == "multiple"
            assert len(result["matches"]) == 3
        finally:
            _deps._bq_service = None

    def test_symbol_not_found_falls_back_to_default(self):
        """Unknown symbol with no BQ match falls back to default exchange."""
        from nl2sql_agent.tools import _deps
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        class MockBQ:
            def query_with_params(self, sql, params=None):
                return []

        _deps._bq_service = MockBQ()
        try:
            result = resolve_exchange("ZZZZZ_FAKE")
            assert result["status"] == "default"
            assert result["exchange"] == "omx"
        finally:
            _deps._bq_service = None

    def test_bq_error_degrades_gracefully(self):
        """BQ failure should not crash — fall back to default."""
        from nl2sql_agent.tools import _deps
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        class MockBQ:
            def query_with_params(self, sql, params=None):
                raise RuntimeError("BQ connection lost")

        _deps._bq_service = MockBQ()
        try:
            result = resolve_exchange("VALE3")
            assert result["status"] == "default"
            assert result["exchange"] == "omx"
        finally:
            _deps._bq_service = None

    def test_no_bq_service_degrades_gracefully(self):
        """When BQ service not initialized, symbol lookup is skipped."""
        from nl2sql_agent.tools import _deps
        from nl2sql_agent.tools.exchange_resolver import resolve_exchange

        _deps._bq_service = None
        result = resolve_exchange("VALE3")
        assert result["status"] == "default"
        assert result["exchange"] == "omx"
