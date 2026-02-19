"""Integration tests for ADK evaluation framework.

These tests require a live LLM (via LiteLLM proxy) and BigQuery access.
Run with: pytest -m integration tests/integration/test_adk_eval.py -v

Note: ADK's AgentEvaluator may not be available in all ADK versions.
These tests will skip gracefully if the evaluator is not importable.
"""

from pathlib import Path

import pytest

EVAL_DIR = Path(__file__).parent.parent.parent / "eval" / "adk"
ROUTING_EVAL_FILE = EVAL_DIR / "routing_eval.test.json"


def test_eval_file_exists():
    """The routing eval test file must exist."""
    assert ROUTING_EVAL_FILE.exists(), f"Missing: {ROUTING_EVAL_FILE}"


def test_eval_file_valid_json():
    """The eval file must be valid JSON with test cases."""
    import json

    with open(ROUTING_EVAL_FILE) as f:
        data = json.load(f)
    assert isinstance(data, list)
    assert len(data) >= 15, f"Expected at least 15 test cases, got {len(data)}"


def test_eval_cases_have_required_fields():
    """Each test case must have name, data with query and expected_tool_use."""
    import json

    with open(ROUTING_EVAL_FILE) as f:
        data = json.load(f)

    for case in data:
        assert "name" in case, f"Test case missing 'name'"
        assert "data" in case, f"Test case '{case.get('name')}' missing 'data'"
        for entry in case["data"]:
            assert "query" in entry, f"Entry missing 'query' in {case['name']}"
            assert "expected_tool_use" in entry, (
                f"Entry missing 'expected_tool_use' in {case['name']}"
            )
            # Each expected tool use must have tool_name
            for tool in entry["expected_tool_use"]:
                assert "tool_name" in tool


@pytest.mark.integration
async def test_adk_eval_tool_trajectory():
    """Run ADK AgentEvaluator on routing test cases.

    This requires a live LLM and BigQuery. Skips if AgentEvaluator
    is not available in the installed ADK version.
    """
    try:
        from google.adk.evaluation import AgentEvaluator
    except ImportError:
        pytest.skip("google.adk.evaluation.AgentEvaluator not available in this ADK version")

    await AgentEvaluator.evaluate(
        agent_module="nl2sql_agent",
        eval_dataset_file_path_or_dir=str(ROUTING_EVAL_FILE),
    )
