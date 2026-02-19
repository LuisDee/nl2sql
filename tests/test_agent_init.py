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

    def test_nl2sql_agent_has_six_tools(self):
        """nl2sql_agent must have 6 tools wired in."""
        from nl2sql_agent.agent import nl2sql_agent

        assert nl2sql_agent.tools is not None
        assert len(nl2sql_agent.tools) == 6

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

    def test_nl2sql_agent_has_generate_content_config(self):
        """NL2SQL agent must have temperature=0.1 for deterministic SQL."""
        from nl2sql_agent.agent import nl2sql_agent

        assert nl2sql_agent.generate_content_config is not None
        assert nl2sql_agent.generate_content_config.temperature == 0.1

    def test_nl2sql_agent_has_callbacks(self):
        """NL2SQL agent must have before and after tool callbacks."""
        from nl2sql_agent.agent import nl2sql_agent

        assert nl2sql_agent.before_tool_callback is not None
        assert nl2sql_agent.after_tool_callback is not None

    def test_nl2sql_agent_instruction_is_callable(self):
        """NL2SQL agent instruction must be a callable (dynamic prompt)."""
        from nl2sql_agent.agent import nl2sql_agent

        assert callable(nl2sql_agent.instruction)