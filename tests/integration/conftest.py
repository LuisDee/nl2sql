"""Integration test fixtures — uses real services, not mocks.

All tests in this directory are automatically marked with @pytest.mark.integration.
Run them with: pytest -m integration

The real_settings fixture loads the actual .env file and temporarily overrides
os.environ so the Settings instance gets the correct project/dataset values
(not the mock values from the parent conftest).
"""

import os
from pathlib import Path

import pytest
from dotenv import dotenv_values

_REAL_ENV_FILE = Path(__file__).parent.parent.parent / "nl2sql_agent" / ".env"


def pytest_collection_modifyitems(items):
    """Automatically mark all tests in this directory as integration."""
    for item in items:
        if "/integration/" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(autouse=True)
def set_test_env():
    """Override parent conftest's autouse set_test_env.

    Integration tests use real .env values, not the mock _TEST_ENV.
    This no-op fixture shadows the parent's monkeypatch-based fixture.
    """


@pytest.fixture(scope="session")
def real_settings():
    """Provide a Settings instance loaded from the real .env.

    Temporarily injects the .env values into os.environ so pydantic-settings
    picks them up (env vars take priority over .env file in pydantic-settings).
    """
    if not _REAL_ENV_FILE.exists():
        pytest.skip(f"No .env file at {_REAL_ENV_FILE} — cannot run integration tests")

    # Read .env values and inject into os.environ
    env_values = dotenv_values(str(_REAL_ENV_FILE))
    saved = {}
    for key, val in env_values.items():
        saved[key] = os.environ.get(key)
        os.environ[key] = val

    from nl2sql_agent.config import Settings

    s = Settings(_env_file=str(_REAL_ENV_FILE))

    # Restore original env (so unit tests aren't polluted)
    for key, orig in saved.items():
        if orig is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = orig

    return s


@pytest.fixture(scope="session")
def litellm_base_url(real_settings):
    """Resolve the LiteLLM proxy URL for local testing.

    Translates host.docker.internal -> localhost since integration
    tests run on the host, not inside Docker.
    """
    url = real_settings.litellm_api_base
    return url.replace("host.docker.internal", "localhost")


@pytest.fixture(scope="session")
def bq_client(real_settings):
    """Provide a real BigQuery client. Skips if auth fails."""
    try:
        from nl2sql_agent.clients import LiveBigQueryClient

        client = LiveBigQueryClient(
            project=real_settings.gcp_project,
            location=real_settings.bq_location,
        )
        # Smoke test — check auth works (dry_run_query catches errors internally)
        result = client.dry_run_query("SELECT 1")
        if not result["valid"]:
            error = result.get("error", "unknown error")
            if "invalid_grant" in str(error) or "credentials" in str(error).lower():
                pytest.skip(f"BigQuery auth failed: {error}")
            if "Access Denied" in str(error) or "403" in str(error):
                pytest.skip(f"BigQuery access denied: {error}")
        return client
    except Exception as e:
        pytest.skip(f"BigQuery unavailable: {e}")
