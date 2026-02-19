"""Integration tests for LiteLLM proxy connectivity.

These tests verify the exact failure mode that caused the runtime error:
    litellm.BadRequestError: LLM Provider NOT provided.
    You passed model=claude-haiku

The root cause was missing provider prefix (e.g. openai/) on model names.
"""

import pytest
import requests
import os

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
        # The health check requires auth on this proxy (returns 401 without key)
        # We just want to know we reached it, so 200 or 401 proves connectivity.
        assert resp.status_code in (200, 401)

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
        )
        # Gemini Thinking models return reasoning_content; if max_tokens is too low, content might be None.
        # But we just want to prove we got a valid response structure (auth worked).
        msg = response.choices[0].message
        assert msg.content is not None or hasattr(msg, "reasoning_content")

    def test_explicit_auth_params(self, litellm_base_url, real_settings, monkeypatch):
        """LiteLLM must work when api_key is passed explicitly, with env vars UNSET.

        This verifies our 'clean' approach where we pass params directly to the constructor/call.
        We explicitly UNSET env vars to prove we aren't relying on them.
        """
        try:
            requests.get(f"{litellm_base_url}/health", timeout=5)
        except requests.ConnectionError:
            pytest.skip(f"LiteLLM proxy not reachable at {litellm_base_url}")
        # Ensure NO env vars are helping us
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("LITELLM_API_KEY", raising=False)
        monkeypatch.delenv("LITELLM_API_BASE", raising=False)

        # Call with explicit params
        response = litellm.completion(
            model=real_settings.litellm_model,
            messages=[{"role": "user", "content": "Say hello in one word."}],
            api_key=real_settings.litellm_api_key,  # Explicit pass
            base_url=litellm_base_url,              # Explicit pass
            max_tokens=10,
        )
        msg = response.choices[0].message
        assert msg.content is not None or hasattr(msg, "reasoning_content")