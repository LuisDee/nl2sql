"""Integration tests for LiteLLM proxy connectivity.

These tests verify the exact failure mode that caused the runtime error:
    litellm.BadRequestError: LLM Provider NOT provided.
    You passed model=claude-haiku

The root cause was missing provider prefix (e.g. openai/) on model names.
"""

import pytest
import requests

import litellm


class TestLiteLLMProxy:
    def test_model_name_has_provider_prefix(self, real_settings):
        """Model string must include a provider prefix (e.g. openai/)."""
        assert "/" in real_settings.litellm_model, (
            f"Model '{real_settings.litellm_model}' missing provider prefix. "
            "LiteLLM needs e.g. 'openai/claude-haiku' to resolve the API protocol."
        )

    def test_complex_model_name_has_provider_prefix(self, real_settings):
        """Complex model string must also include a provider prefix."""
        assert "/" in real_settings.litellm_model_complex, (
            f"Complex model '{real_settings.litellm_model_complex}' missing provider prefix."
        )

    def test_litellm_resolves_provider(self, real_settings):
        """litellm.get_llm_provider() must succeed for the configured model."""
        model, provider, _, _ = litellm.get_llm_provider(real_settings.litellm_model)
        assert provider is not None
        assert model is not None

    def test_proxy_reachable(self, litellm_base_url, real_settings):
        """LiteLLM proxy must respond to health check."""
        try:
            resp = requests.get(f"{litellm_base_url}/health", timeout=5)
        except requests.ConnectionError:
            pytest.skip(f"LiteLLM proxy not reachable at {litellm_base_url}")
        assert resp.status_code == 200

    def test_simple_completion(self, litellm_base_url, real_settings):
        """LiteLLM proxy must return a completion for a simple prompt."""
        try:
            requests.get(f"{litellm_base_url}/health", timeout=5)
        except requests.ConnectionError:
            pytest.skip(f"LiteLLM proxy not reachable at {litellm_base_url}")

        response = litellm.completion(
            model=real_settings.litellm_model,
            messages=[{"role": "user", "content": "Say hello in one word."}],
            api_key=real_settings.litellm_api_key,
            base_url=litellm_base_url,
            max_tokens=10,
        )
        assert response.choices[0].message.content is not None
        assert len(response.choices[0].message.content) > 0
