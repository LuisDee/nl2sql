"""Tests for configuration loading via pydantic-settings."""

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
        from pydantic import ValidationError
        import pytest

        with pytest.raises(ValidationError):
            Settings(_env_file=None)

    def test_settings_ignores_extra_env_vars(self, monkeypatch):
        """Settings should not crash on unknown env vars."""
        monkeypatch.setenv("SOME_RANDOM_THING", "foobar")
        s = Settings()  # Should not raise
        assert s.gcp_project == "cloud-data-n-base-d4b3"