"""Tests for configuration loading via pydantic-settings."""

from pathlib import Path

from nl2sql_agent.config import Settings


class TestSettings:
    """Test that Settings loads correctly from environment variables."""

    def test_settings_loads_from_env(self):
        """Settings should load all required fields from environment."""
        s = Settings()
        assert s.litellm_api_key == "test-key-not-real"
        assert s.litellm_api_base == "http://localhost:4000"
        assert s.gcp_project == "cloud-data-n-base-d4b3"
        assert s.bq_location == "europe-west2"
        assert s.kpi_dataset == "nl2sql_omx_kpi"

    def test_settings_defaults(self):
        """Settings should have correct default values."""
        s = Settings()
        assert s.litellm_model == "openai/gemini-3-flash-preview"
        assert s.litellm_model_complex == "openai/gemini-3-pro-preview"
        assert s.bq_location == "europe-west2"
        assert s.kpi_dataset == "nl2sql_omx_kpi"
        assert s.data_dataset == "nl2sql_omx_data"
        assert s.metadata_dataset == "nl2sql_metadata"
        assert s.embedding_model == "text-embedding-005"

    def test_settings_missing_required_field_raises(self, monkeypatch):
        """Settings should raise ValidationError if required field is missing."""
        monkeypatch.delenv("LITELLM_API_KEY", raising=False)
        import pytest
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Settings(_env_file=None)

    def test_settings_ignores_extra_env_vars(self, monkeypatch):
        """Settings should not crash on unknown env vars."""
        monkeypatch.setenv("SOME_RANDOM_THING", "foobar")
        s = Settings()  # Should not raise
        assert s.gcp_project == "cloud-data-n-base-d4b3"


class TestEnvExample:
    """Verify .env.example exists and covers all Settings fields."""

    ENV_EXAMPLE = Path(__file__).parent.parent / ".env.example"

    def test_env_example_exists(self):
        assert self.ENV_EXAMPLE.exists(), ".env.example must exist in project root"

    def test_env_example_covers_required_fields(self):
        """All required Settings fields must appear in .env.example."""
        content = self.ENV_EXAMPLE.read_text()
        # Required fields (no default in Settings)
        for field in ["LITELLM_API_KEY", "LITELLM_API_BASE"]:
            assert field in content, f"Required field {field} missing from .env.example"

    def test_env_example_covers_key_optional_fields(self):
        """Key optional fields should be documented in .env.example."""
        content = self.ENV_EXAMPLE.read_text()
        for field in [
            "GCP_PROJECT",
            "KPI_DATASET",
            "DATA_DATASET",
            "METADATA_DATASET",
            "LITELLM_MODEL",
            "BQ_MAX_RESULT_ROWS",
            "SEMANTIC_CACHE_THRESHOLD",
        ]:
            assert field in content, f"Optional field {field} missing from .env.example"
