"""Tests for ADK agent initialisation and delegation structure."""

from google.adk.agents import LlmAgent


class TestAgentStructure:
    """Test that agents are correctly configured."""

    def test_root_agent_exists_and_is_llm_agent(self):
        """The root_agent must exist and be an LlmAgent instance."""
        from nl2sql_agent.agent import root_agent

        assert isinstance(root_agent, LlmAgent)
        assert root_agent.name == "mako_assistant"

    def test_root_agent_has_sub_agents(self):
        """Root agent must have exactly one sub-agent: nl2sql_agent."""
        from nl2sql_agent.agent import root_agent

        assert len(root_agent.sub_agents) == 1
        assert root_agent.sub_agents[0].name == "nl2sql_agent"

    def test_nl2sql_agent_is_llm_agent(self):
        """The NL2SQL sub-agent must be an LlmAgent instance."""
        from nl2sql_agent.agent import nl2sql_agent

        assert isinstance(nl2sql_agent, LlmAgent)
        assert nl2sql_agent.name == "nl2sql_agent"

    def test_nl2sql_agent_has_description(self):
        """NL2SQL agent description must mention key domains."""
        from nl2sql_agent.agent import nl2sql_agent

        desc = nl2sql_agent.description.lower()
        assert "trading data" in desc
        assert "bigquery" in desc

    def test_nl2sql_agent_has_no_tools_in_track_01(self):
        """In Track 01, nl2sql_agent must have zero tools."""
        from nl2sql_agent.agent import nl2sql_agent

        assert nl2sql_agent.tools is None or len(nl2sql_agent.tools) == 0

    def test_root_agent_instruction_mentions_delegation(self):
        """Root agent instruction must tell it to delegate data questions."""
        from nl2sql_agent.agent import root_agent

        instruction = root_agent.instruction.lower()
        assert "delegate" in instruction or "nl2sql_agent" in instruction

    def test_agent_names_are_valid_python_identifiers(self):
        """All agent names must be valid Python identifiers (ADK requirement)."""
        from nl2sql_agent.agent import root_agent, nl2sql_agent

        assert root_agent.name.isidentifier()
        assert nl2sql_agent.name.isidentifier()

    def test_agent_names_are_not_reserved(self):
        """No agent can be named 'user' — reserved by ADK."""
        from nl2sql_agent.agent import root_agent, nl2sql_agent

        assert root_agent.name != "user"
        assert nl2sql_agent.name != "user"