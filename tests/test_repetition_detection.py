"""Tests for smart loop detection (repetition detector) in callbacks."""

from unittest.mock import MagicMock

from nl2sql_agent.callbacks import _tool_call_hash, before_tool_guard


class TestToolCallHash:
    def test_same_inputs_produce_same_hash(self):
        h1 = _tool_call_hash("dry_run_sql", {"sql_query": "SELECT 1"})
        h2 = _tool_call_hash("dry_run_sql", {"sql_query": "SELECT 1"})
        assert h1 == h2

    def test_different_args_produce_different_hash(self):
        h1 = _tool_call_hash("dry_run_sql", {"sql_query": "SELECT 1"})
        h2 = _tool_call_hash("dry_run_sql", {"sql_query": "SELECT 2"})
        assert h1 != h2

    def test_different_tools_produce_different_hash(self):
        h1 = _tool_call_hash("dry_run_sql", {"sql_query": "SELECT 1"})
        h2 = _tool_call_hash("execute_sql", {"sql_query": "SELECT 1"})
        assert h1 != h2

    def test_non_serializable_args_dont_crash(self):
        """Args with unhashable/non-serializable values handled gracefully."""
        h = _tool_call_hash("some_tool", {"data": object()})
        assert isinstance(h, str)
        assert len(h) == 12


def _make_tool(name="dry_run_sql"):
    tool = MagicMock()
    tool.name = name
    return tool


def _make_context(state=None):
    ctx = MagicMock()
    ctx.state = state if state is not None else {}
    return ctx


class TestRepetitionDetection:
    def test_no_block_on_different_tools(self):
        """10 different tools in sequence — all allowed."""
        ctx = _make_context()
        for i in range(10):
            tool = _make_tool(f"tool_{i}")
            result = before_tool_guard(tool, {"arg": "val"}, ctx)
            assert result is None, f"Blocked on tool_{i}"

    def test_no_block_on_same_tool_different_args(self):
        """dry_run_sql called 5x with different SQL — allowed."""
        ctx = _make_context()
        tool = _make_tool("dry_run_sql")
        for i in range(5):
            result = before_tool_guard(tool, {"sql_query": f"SELECT {i}"}, ctx)
            assert result is None, f"Blocked on call {i}"

    def test_blocks_same_tool_same_args_consecutive(self):
        """dry_run_sql('SELECT broken') called 3x — blocked on 3rd."""
        ctx = _make_context()
        tool = _make_tool("dry_run_sql")
        args = {"sql_query": "SELECT broken"}

        # First 2 calls allowed
        assert before_tool_guard(tool, args, ctx) is None
        assert before_tool_guard(tool, args, ctx) is None

        # 3rd call blocked
        result = before_tool_guard(tool, args, ctx)
        assert result is not None
        assert result["status"] == "error"
        assert result["blocked_by"] == "repetition_detector"
        assert "Loop detected" in result["error_message"]

    def test_resets_on_different_call(self):
        """A, A, B, A, A — not blocked (consecutive count resets)."""
        ctx = _make_context()
        tool_a = _make_tool("tool_a")
        tool_b = _make_tool("tool_b")
        args_a = {"x": "1"}
        args_b = {"x": "2"}

        assert before_tool_guard(tool_a, args_a, ctx) is None  # A
        assert before_tool_guard(tool_a, args_a, ctx) is None  # A
        assert before_tool_guard(tool_b, args_b, ctx) is None  # B (breaks streak)
        assert before_tool_guard(tool_a, args_a, ctx) is None  # A
        assert before_tool_guard(tool_a, args_a, ctx) is None  # A (only 2 consecutive)

    def test_history_stored_in_state(self):
        ctx = _make_context()
        tool = _make_tool("my_tool")

        before_tool_guard(tool, {"arg": "val"}, ctx)
        before_tool_guard(tool, {"arg": "val2"}, ctx)

        history = ctx.state["tool_call_history"]
        assert len(history) == 2
        assert all(isinstance(h, str) for h in history)

    def test_configurable_threshold(self, monkeypatch):
        """max_consecutive_repeats=5 allows 4 repeats."""
        monkeypatch.setattr(
            "nl2sql_agent.callbacks.settings.max_consecutive_repeats", 5
        )
        ctx = _make_context()
        tool = _make_tool("dry_run_sql")
        args = {"sql_query": "SELECT broken"}

        # 4 calls allowed
        for i in range(4):
            result = before_tool_guard(tool, args, ctx)
            assert result is None, f"Blocked early on call {i+1}"

        # 5th call blocked
        result = before_tool_guard(tool, args, ctx)
        assert result is not None
        assert result["blocked_by"] == "repetition_detector"

    def test_check_semantic_cache_resets_history(self):
        """New question (check_semantic_cache) clears history."""
        ctx = _make_context({"tool_call_history": ["abc", "abc"]})
        tool = _make_tool("check_semantic_cache")

        result = before_tool_guard(tool, {"question": "new question"}, ctx)

        assert result is None
        # History should be reset then contain only the new call
        assert len(ctx.state["tool_call_history"]) == 1

    def test_high_safety_net_still_exists(self, monkeypatch):
        """51st tool call blocked regardless (safety net)."""
        monkeypatch.setattr(
            "nl2sql_agent.callbacks.settings.max_tool_calls_per_turn", 50
        )
        monkeypatch.setattr(
            "nl2sql_agent.callbacks.settings.max_consecutive_repeats", 100
        )
        ctx = _make_context({"tool_call_count": 50})  # Already at 50

        tool = _make_tool("some_tool")
        result = before_tool_guard(tool, {"x": "y"}, ctx)

        assert result is not None
        assert result["blocked_by"] == "max_tool_calls"

    def test_non_serializable_args_dont_crash_hashing(self):
        """Args with unhashable values handled gracefully."""
        ctx = _make_context()
        tool = _make_tool("some_tool")

        # object() is not JSON-serializable — should not crash
        result = before_tool_guard(tool, {"data": object()}, ctx)
        assert result is None
