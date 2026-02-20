# Track 11: Gemini CLI Integration (MCP Server)

## Goal
Expose the NL2SQL agent as a single MCP tool (`ask_trading_data`) so Gemini CLI can delegate trading data questions to it.

## Requirements
1. Single tool: `ask_trading_data(question: str) -> str`
2. Progress notifications: emit step descriptions as agent tools execute
3. Zero footprint: no changes to trader's GEMINI.md — routing handled by tool description
4. Existing ADK web entry point unaffected
5. All logging to stderr (stdout reserved for MCP JSON-RPC)

## Architecture
```
Gemini CLI → stdio → MCP Server (FastMCP) → InMemoryRunner → root_agent → nl2sql_agent → BigQuery
```

## Files
- `nl2sql_agent/mcp_server.py` — MCP server implementation
- `tests/test_mcp_server.py` — 14 unit tests
- `tests/integration/test_mcp_server.py` — stdio round-trip integration test
- `docs/HOW_IT_ALL_WORKS.md` — Part 9: Gemini CLI Integration
- `pyproject.toml` — `mcp[cli]>=1.20.0` dependency added

## Dependencies
- `mcp[cli]>=1.20.0` (MCP Python SDK with CLI extras)
- Google ADK `InMemoryRunner` for programmatic agent invocation
