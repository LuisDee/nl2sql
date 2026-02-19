"""Tests that tools are correctly wired into the agent."""

from unittest.mock import patch, MagicMock


class TestToolWiring:
    @patch("nl2sql_agent.clients.LiveBigQueryClient")
    def test_nl2sql_agent_has_seven_tools(self, mock_client_class):
        mock_client_class.return_value = MagicMock()

        from nl2sql_agent.agent import nl2sql_agent

        assert nl2sql_agent.tools is not None
        assert len(nl2sql_agent.tools) == 7

    @patch("nl2sql_agent.clients.LiveBigQueryClient")
    def test_root_agent_still_has_sub_agents(self, mock_client_class):
        mock_client_class.return_value = MagicMock()

        from nl2sql_agent.agent import root_agent

        assert len(root_agent.sub_agents) == 1
        assert root_agent.sub_agents[0].name == "nl2sql_agent"

    @patch("nl2sql_agent.clients.LiveBigQueryClient")
    def test_root_agent_has_no_tools(self, mock_client_class):
        mock_client_class.return_value = MagicMock()

        from nl2sql_agent.agent import root_agent

        assert root_agent.tools is None or len(root_agent.tools) == 0

    @patch("nl2sql_agent.clients.LiveBigQueryClient")
    def test_all_tools_are_callable(self, mock_client_class):
        mock_client_class.return_value = MagicMock()

        from nl2sql_agent.agent import nl2sql_agent

        for tool in nl2sql_agent.tools:
            # ADK wraps functions as FunctionTool. The underlying func should be callable.
            assert callable(tool) or hasattr(tool, "func")
