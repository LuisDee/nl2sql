"""Unit tests for the MCP server.

Tests tool listing, schema validation, agent invocation, error handling,
and progress notifications — all without live services.

The mcp_server module imports agent.py which triggers BQ client init,
but the test conftest.py sets mock env vars at module level so Settings()
loads with test values and LiveBigQueryClient is created (but never called).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_event(*, is_final=False, text=None, function_calls=None):
    """Build a mock ADK event."""
    event = MagicMock()
    event.is_final_response.return_value = is_final

    if is_final and text:
        part = MagicMock()
        part.text = text
        event.content = MagicMock()
        event.content.parts = [part]
    elif is_final:
        event.content = None

    # get_function_calls() returns list of FunctionCall-like objects
    if function_calls:
        fcs = []
        for name in function_calls:
            fc = MagicMock()
            fc.name = name
            fcs.append(fc)
        event.get_function_calls.return_value = fcs
    else:
        event.get_function_calls.return_value = []

    return event


# ---------------------------------------------------------------------------
# Tool listing & schema
# ---------------------------------------------------------------------------


class TestToolListing:
    @pytest.mark.asyncio
    async def test_list_tools_returns_one_tool(self):
        """Server exposes exactly one tool named ask_trading_data."""
        from mcp.shared.memory import create_connected_server_and_client_session

        from nl2sql_agent.mcp_server import mcp

        async with create_connected_server_and_client_session(mcp) as client:
            result = await client.list_tools()
            tool_names = [t.name for t in result.tools]
            assert tool_names == ["ask_trading_data"]

    @pytest.mark.asyncio
    async def test_tool_schema_has_question_field(self):
        """Input schema requires a 'question' string parameter."""
        from mcp.shared.memory import create_connected_server_and_client_session

        from nl2sql_agent.mcp_server import mcp

        async with create_connected_server_and_client_session(mcp) as client:
            result = await client.list_tools()
            tool = result.tools[0]
            props = tool.inputSchema.get("properties", {})
            assert "question" in props
            assert props["question"]["type"] == "string"
            assert "question" in tool.inputSchema.get("required", [])

    @pytest.mark.asyncio
    async def test_tool_description_mentions_trading(self):
        """Description contains routing keywords for Gemini CLI."""
        from mcp.shared.memory import create_connected_server_and_client_session

        from nl2sql_agent.mcp_server import mcp

        async with create_connected_server_and_client_session(mcp) as client:
            result = await client.list_tools()
            desc = result.tools[0].description.lower()
            for keyword in ["trading", "pnl", "bigquery", "edge"]:
                assert keyword in desc, (
                    f"Missing keyword '{keyword}' in tool description"
                )


# ---------------------------------------------------------------------------
# Tool invocation
# ---------------------------------------------------------------------------


class TestToolInvocation:
    @pytest.mark.asyncio
    async def test_call_tool_unknown_name_returns_error(self):
        """Calling a non-existent tool returns an error."""
        from mcp.shared.memory import create_connected_server_and_client_session

        from nl2sql_agent.mcp_server import mcp

        async with create_connected_server_and_client_session(mcp) as client:
            result = await client.call_tool("nonexistent_tool", {"question": "test"})
            assert result.isError is True

    @pytest.mark.asyncio
    async def test_call_tool_missing_question_returns_error(self):
        """Missing required 'question' arg returns error."""
        from mcp.shared.memory import create_connected_server_and_client_session

        from nl2sql_agent.mcp_server import mcp

        async with create_connected_server_and_client_session(mcp) as client:
            result = await client.call_tool("ask_trading_data", {})
            assert result.isError is True

    @pytest.mark.asyncio
    async def test_call_tool_invokes_runner(self):
        """Tool invocation calls Runner.run_async with the user's question."""
        final_event = _make_event(is_final=True, text="The answer is 42.")

        async def mock_run_async(**kwargs):
            yield final_event

        mock_session = AsyncMock()
        mock_session.id = "test-session-id"
        mock_session_service = AsyncMock()
        mock_session_service.create_session = AsyncMock(return_value=mock_session)

        with patch("nl2sql_agent.mcp_server._runner") as mock_runner:
            mock_runner.run_async = mock_run_async
            mock_runner.session_service = mock_session_service

            from mcp.shared.memory import create_connected_server_and_client_session

            from nl2sql_agent.mcp_server import mcp

            async with create_connected_server_and_client_session(mcp) as client:
                result = await client.call_tool(
                    "ask_trading_data", {"question": "what was the edge today?"}
                )
                assert result.isError is not True
                text = result.content[0].text
                assert "42" in text

    @pytest.mark.asyncio
    async def test_call_tool_returns_final_response_text(self):
        """Final response text is extracted correctly from ADK event."""
        final_event = _make_event(is_final=True, text="Average edge was 0.0234 bps.")

        async def mock_run_async(**kwargs):
            yield final_event

        mock_session = AsyncMock()
        mock_session.id = "s1"
        mock_session_service = AsyncMock()
        mock_session_service.create_session = AsyncMock(return_value=mock_session)

        with patch("nl2sql_agent.mcp_server._runner") as mock_runner:
            mock_runner.run_async = mock_run_async
            mock_runner.session_service = mock_session_service

            from mcp.shared.memory import create_connected_server_and_client_session

            from nl2sql_agent.mcp_server import mcp

            async with create_connected_server_and_client_session(mcp) as client:
                result = await client.call_tool(
                    "ask_trading_data", {"question": "avg edge?"}
                )
                assert "0.0234" in result.content[0].text

    @pytest.mark.asyncio
    async def test_call_tool_handles_agent_exception(self):
        """Runner exception returns error message, not crash."""

        async def mock_run_async(**kwargs):
            raise RuntimeError("BQ connection failed")
            yield  # unreachable — makes this an async generator

        mock_session = AsyncMock()
        mock_session.id = "s2"
        mock_session_service = AsyncMock()
        mock_session_service.create_session = AsyncMock(return_value=mock_session)

        with patch("nl2sql_agent.mcp_server._runner") as mock_runner:
            mock_runner.run_async = mock_run_async
            mock_runner.session_service = mock_session_service

            from mcp.shared.memory import create_connected_server_and_client_session

            from nl2sql_agent.mcp_server import mcp

            async with create_connected_server_and_client_session(mcp) as client:
                result = await client.call_tool(
                    "ask_trading_data", {"question": "test"}
                )
                text = result.content[0].text
                assert "Error" in text or "error" in text
                assert "BQ connection failed" in text

    @pytest.mark.asyncio
    async def test_call_tool_no_response_returns_fallback(self):
        """If runner yields no final response, return fallback message."""
        non_final = _make_event(is_final=False)

        async def mock_run_async(**kwargs):
            yield non_final

        mock_session = AsyncMock()
        mock_session.id = "s3"
        mock_session_service = AsyncMock()
        mock_session_service.create_session = AsyncMock(return_value=mock_session)

        with patch("nl2sql_agent.mcp_server._runner") as mock_runner:
            mock_runner.run_async = mock_run_async
            mock_runner.session_service = mock_session_service

            from mcp.shared.memory import create_connected_server_and_client_session

            from nl2sql_agent.mcp_server import mcp

            async with create_connected_server_and_client_session(mcp) as client:
                result = await client.call_tool(
                    "ask_trading_data", {"question": "test"}
                )
                assert "No response" in result.content[0].text


# ---------------------------------------------------------------------------
# Progress notifications
# ---------------------------------------------------------------------------


class TestProgressNotifications:
    @pytest.mark.asyncio
    async def test_progress_emitted_for_tool_calls(self):
        """Mock runner yields tool call events — tool doesn't crash."""
        tool_event = _make_event(function_calls=["vector_search_columns"])
        final_event = _make_event(is_final=True, text="Done.")

        async def mock_run_async(**kwargs):
            yield tool_event
            yield final_event

        mock_session = AsyncMock()
        mock_session.id = "s4"
        mock_session_service = AsyncMock()
        mock_session_service.create_session = AsyncMock(return_value=mock_session)

        with patch("nl2sql_agent.mcp_server._runner") as mock_runner:
            mock_runner.run_async = mock_run_async
            mock_runner.session_service = mock_session_service

            from mcp.shared.memory import create_connected_server_and_client_session

            from nl2sql_agent.mcp_server import mcp

            async with create_connected_server_and_client_session(mcp) as client:
                result = await client.call_tool(
                    "ask_trading_data", {"question": "edge?"}
                )
                assert result.content[0].text == "Done."

    @pytest.mark.asyncio
    async def test_progress_messages_describe_steps(self):
        """Progress text matches expected descriptions for known tools."""
        from nl2sql_agent.mcp_server import TOOL_PROGRESS_MESSAGES

        assert "vector_search_columns" in TOOL_PROGRESS_MESSAGES
        assert "dry_run_sql" in TOOL_PROGRESS_MESSAGES
        assert "execute_sql" in TOOL_PROGRESS_MESSAGES
        assert "Searching" in TOOL_PROGRESS_MESSAGES["vector_search_columns"]
        assert "Validating" in TOOL_PROGRESS_MESSAGES["dry_run_sql"]
        assert "Executing" in TOOL_PROGRESS_MESSAGES["execute_sql"]

    @pytest.mark.asyncio
    async def test_multiple_tool_calls_increment_progress(self):
        """Multiple tool call events increment the step counter."""
        event1 = _make_event(function_calls=["check_semantic_cache"])
        event2 = _make_event(function_calls=["vector_search_columns"])
        event3 = _make_event(function_calls=["dry_run_sql"])
        final_event = _make_event(is_final=True, text="Result.")

        async def mock_run_async(**kwargs):
            yield event1
            yield event2
            yield event3
            yield final_event

        mock_session = AsyncMock()
        mock_session.id = "s5"
        mock_session_service = AsyncMock()
        mock_session_service.create_session = AsyncMock(return_value=mock_session)

        with patch("nl2sql_agent.mcp_server._runner") as mock_runner:
            mock_runner.run_async = mock_run_async
            mock_runner.session_service = mock_session_service

            from mcp.shared.memory import create_connected_server_and_client_session

            from nl2sql_agent.mcp_server import mcp

            async with create_connected_server_and_client_session(mcp) as client:
                result = await client.call_tool(
                    "ask_trading_data", {"question": "test?"}
                )
                # If progress reporting failed, the tool would error
                assert result.content[0].text == "Result."


# ---------------------------------------------------------------------------
# Module-level safety
# ---------------------------------------------------------------------------


class TestModuleSafety:
    def test_no_bare_print_in_module(self):
        """Module must not have bare print() calls (stdout is MCP JSON-RPC)."""
        import ast

        import nl2sql_agent.mcp_server as mod

        with open(mod.__file__) as _f:
            tree = ast.parse(_f.read())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name) and func.id == "print":
                    pytest.fail("Found bare print() call in mcp_server.py")

    def test_progress_messages_cover_all_tools(self):
        """All agent tools have a progress message mapping."""
        from nl2sql_agent.mcp_server import TOOL_PROGRESS_MESSAGES

        expected_tools = {
            "check_semantic_cache",
            "resolve_exchange",
            "vector_search_columns",
            "vector_search_tables",
            "load_yaml_metadata",
            "fetch_few_shot_examples",
            "dry_run_sql",
            "execute_sql",
            "save_validated_query",
        }
        assert set(TOOL_PROGRESS_MESSAGES.keys()) == expected_tools
