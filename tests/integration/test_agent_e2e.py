"""End-to-end integration tests for the agent.

Tests agent initialization and basic conversation flow using
ADK's InMemoryRunner to verify the full stack works together.
"""

import pytest


class TestAgentInitialisation:
    def test_agent_imports_without_error(self, real_settings):
        """Importing agent.py must not raise — catches import-time init errors."""
        from nl2sql_agent.agent import nl2sql_agent, root_agent

        assert root_agent.name == "mako_assistant"
        assert nl2sql_agent.name == "nl2sql_agent"
        assert len(nl2sql_agent.tools) == 6

    def test_model_has_provider_prefix(self, real_settings):
        """The LiteLlm model instance must use a prefixed model name."""
        from nl2sql_agent.agent import default_model

        assert "/" in default_model.model, (
            f"LiteLlm model '{default_model.model}' missing provider prefix"
        )


class TestAgentConversation:
    @pytest.mark.asyncio
    async def test_greeting_no_sql(self, real_settings, litellm_base_url):
        """A greeting should get a direct response without SQL tool calls."""
        try:
            import requests

            requests.get(f"{litellm_base_url}/health", timeout=5)
        except requests.ConnectionError:
            pytest.skip(f"LiteLLM proxy not reachable at {litellm_base_url}")

        from google.adk.runners import InMemoryRunner
        from google.adk.sessions import InMemorySessionService

        from nl2sql_agent.agent import root_agent

        runner = InMemoryRunner(
            agent=root_agent,
            app_name="integration_test",
            session_service=InMemorySessionService(),
        )

        session = await runner.session_service.create_session(
            app_name="integration_test", user_id="test_user"
        )

        from google.genai.types import Content, Part

        user_msg = Content(
            role="user",
            parts=[Part(text="Hello, how are you?")],
        )

        events = []
        async for event in runner.run_async(
            user_id="test_user",
            session_id=session.id,
            new_message=user_msg,
        ):
            events.append(event)

        # Should have at least one response event
        assert len(events) > 0

        # Should NOT have called any SQL tools for a greeting
        tool_calls = [e for e in events if hasattr(e, "tool_calls") and e.tool_calls]
        sql_tool_names = {"dry_run_sql", "execute_sql"}
        for tc_event in tool_calls:
            for tc in tc_event.tool_calls:
                assert tc.name not in sql_tool_names, (
                    f"Greeting triggered SQL tool: {tc.name}"
                )
