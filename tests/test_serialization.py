"""Exhaustive tests for BigQuery JSON serialization sanitizer."""

import json
import math
from datetime import date, datetime, time
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from nl2sql_agent.serialization import sanitize_row, sanitize_rows, sanitize_value


class TestSanitizeValue:
    def test_sanitize_timestamp(self):
        val = pd.Timestamp("2024-01-15 10:30:00+00:00")
        result = sanitize_value(val)
        assert isinstance(result, str)
        assert "2024-01-15" in result
        assert "10:30:00" in result

    def test_sanitize_naive_timestamp(self):
        val = pd.Timestamp("2024-01-15 10:30:00")
        result = sanitize_value(val)
        assert isinstance(result, str)
        assert "2024-01-15" in result

    def test_sanitize_date(self):
        val = date(2024, 1, 15)
        result = sanitize_value(val)
        assert result == "2024-01-15"

    def test_sanitize_time(self):
        val = time(10, 30, 0)
        result = sanitize_value(val)
        assert result == "10:30:00"

    def test_sanitize_decimal(self):
        val = Decimal("123.45")
        result = sanitize_value(val)
        assert result == 123.45
        assert isinstance(result, float)

    def test_sanitize_nat(self):
        result = sanitize_value(pd.NaT)
        assert result is None

    def test_sanitize_bytes(self):
        val = b"hello"
        result = sanitize_value(val)
        assert isinstance(result, str)
        # base64 of "hello" is "aGVsbG8="
        assert result == "aGVsbG8="

    def test_sanitize_numpy_int(self):
        val = np.int64(42)
        result = sanitize_value(val)
        assert result == 42
        assert isinstance(result, (int, np.integer))

    def test_sanitize_numpy_float(self):
        val = np.float64(3.14)
        result = sanitize_value(val)
        assert result == pytest.approx(3.14)

    def test_sanitize_numpy_bool(self):
        val = np.bool_(True)
        result = sanitize_value(val)
        assert result is True or result == True  # noqa: E712 — numpy bool

    def test_sanitize_nan(self):
        result = sanitize_value(float("nan"))
        assert result is None

    def test_sanitize_preserves_safe_types(self):
        assert sanitize_value("hello") == "hello"
        assert sanitize_value(42) == 42
        assert sanitize_value(3.14) == 3.14
        assert sanitize_value(True) is True
        assert sanitize_value(None) is None

    def test_sanitize_nested_dict(self):
        val = {"inner": pd.Timestamp("2024-01-15"), "count": 5}
        result = sanitize_value(val)
        assert isinstance(result, dict)
        assert isinstance(result["inner"], str)
        assert result["count"] == 5

    def test_sanitize_nested_list(self):
        val = [pd.Timestamp("2024-01-15"), Decimal("9.99"), "ok"]
        result = sanitize_value(val)
        assert isinstance(result, list)
        assert isinstance(result[0], str)
        assert isinstance(result[1], float)
        assert result[2] == "ok"

    def test_sanitize_datetime(self):
        val = datetime(2024, 1, 15, 10, 30, 0)
        result = sanitize_value(val)
        assert isinstance(result, str)
        assert "2024-01-15" in result


class TestSanitizeRow:
    def test_full_row_round_trips_through_json_dumps(self):
        """End-to-end: sanitized row passes json.dumps without error."""
        row = {
            "ts": pd.Timestamp("2024-01-15 10:30:00+00:00"),
            "d": date(2024, 1, 15),
            "t": time(10, 30, 0),
            "n": Decimal("123.45"),
            "nat": pd.NaT,
            "b": b"hello",
            "np_int": np.int64(42),
            "np_float": np.float64(3.14),
            "nan": float("nan"),
            "safe_str": "ok",
            "safe_int": 7,
            "safe_none": None,
        }
        sanitized = sanitize_row(row)

        # Must not raise
        result = json.dumps(sanitized)
        assert isinstance(result, str)

        # Round-trip back
        parsed = json.loads(result)
        assert parsed["safe_str"] == "ok"
        assert parsed["safe_int"] == 7
        assert parsed["safe_none"] is None
        assert parsed["nat"] is None
        assert parsed["nan"] is None
        assert "2024-01-15" in parsed["ts"]

    def test_sanitize_row_preserves_keys(self):
        row = {"a": 1, "b": pd.Timestamp("2024-01-01")}
        result = sanitize_row(row)
        assert set(result.keys()) == {"a", "b"}

    def test_sanitize_row_with_nested_dict(self):
        row = {"meta": {"ts": pd.Timestamp("2024-01-01")}, "val": 1}
        result = sanitize_row(row)
        assert isinstance(result["meta"]["ts"], str)

    def test_sanitize_row_with_nested_list(self):
        row = {"tags": [pd.Timestamp("2024-01-01"), "ok"]}
        result = sanitize_row(row)
        assert isinstance(result["tags"][0], str)
        assert result["tags"][1] == "ok"


class TestSanitizeRows:
    def test_sanitize_rows_list(self):
        rows = [
            {"ts": pd.Timestamp("2024-01-15"), "val": 1},
            {"ts": pd.Timestamp("2024-02-20"), "val": 2},
        ]
        result = sanitize_rows(rows)
        assert len(result) == 2
        assert all(isinstance(r["ts"], str) for r in result)

        # All rows must be JSON-serializable
        json.dumps(result)

    def test_sanitize_empty_list(self):
        assert sanitize_rows([]) == []

    def test_sanitize_rows_preserves_count(self):
        rows = [{"a": i} for i in range(10)]
        assert len(sanitize_rows(rows)) == 10
