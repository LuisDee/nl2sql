"""Integration tests for the MCP server.

Tests the MCP server as a subprocess via stdio transport,
verifying it can list tools and handle requests end-to-end.

Requires: LiteLLM proxy running, GCP ADC configured.
"""

import json
import subprocess
import sys


class TestMCPServerStdio:
    def test_mcp_server_stdio_list_tools(self):
        """Start MCP server as subprocess, send list_tools, verify response."""
        # MCP JSON-RPC initialize request
        init_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1.0.0"},
            },
        }

        # MCP JSON-RPC initialized notification
        initialized_notification = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }

        # MCP JSON-RPC list_tools request
        list_tools_request = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {},
        }

        # Send all messages, newline-delimited
        stdin_data = "\n".join(
            [
                json.dumps(init_request),
                json.dumps(initialized_notification),
                json.dumps(list_tools_request),
                "",  # trailing newline
            ]
        )

        proc = subprocess.run(  # noqa: S603
            [sys.executable, "-m", "nl2sql_agent.mcp_server"],
            input=stdin_data,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(
                subprocess.os.path.dirname(
                    subprocess.os.path.dirname(subprocess.os.path.dirname(__file__))
                )
            ),
        )

        # Server should not crash
        assert proc.returncode == 0 or proc.returncode is None, (
            f"Server crashed with stderr: {proc.stderr[:500]}"
        )

        # Parse stdout for JSON-RPC responses (may be multiple lines)
        responses = []
        for line in proc.stdout.strip().split("\n"):
            line = line.strip()
            if line:
                try:
                    responses.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

        # Find the list_tools response (id=2)
        list_response = None
        for resp in responses:
            if resp.get("id") == 2:
                list_response = resp
                break

        assert list_response is not None, (
            f"No list_tools response found. Got: {responses}"
        )
        assert "result" in list_response
        tools = list_response["result"]["tools"]
        assert len(tools) == 1
        assert tools[0]["name"] == "ask_trading_data"
        assert "question" in tools[0]["inputSchema"]["properties"]
