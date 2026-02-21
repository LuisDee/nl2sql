"""JSON serialization safety for BigQuery result types.

BigQuery rows (via pandas DataFrames or Row objects) contain types that
are not JSON-serializable: pd.Timestamp, datetime.date, Decimal, NaT,
numpy integers, etc.  These crash ADK's state persistence pipeline.

This module provides sanitize_row() / sanitize_rows() to convert all
non-serializable values to JSON-safe equivalents *at the source*, before
they enter ADK's serialization boundaries.

Pattern follows ADK's own built-in BQ tool fix (v1.8.0, issue #1033).
"""

import base64
import json
import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd


def sanitize_value(val: Any) -> Any:
    """Convert a single value to a JSON-safe type."""
    if val is None:
        return None
    # NaT/NA check FIRST — pd.NaT is a datetime instance so must be caught early
    if not isinstance(val, str | bytes):
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
    # pd.Timestamp before datetime (Timestamp is a datetime subclass)
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, time):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, bytes):
        return base64.b64encode(val).decode("ascii")
    if isinstance(val, float) and math.isnan(val):
        return None
    # Numpy scalar types (not JSON-serializable)
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        return float(val)
    if isinstance(val, np.bool_):
        return bool(val)
    # Nested structures
    if isinstance(val, dict):
        return {k: sanitize_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [sanitize_value(v) for v in val]
    # Final check: if json.dumps works, it's safe; otherwise str() fallback
    try:
        json.dumps(val)
        return val
    except (TypeError, ValueError, OverflowError):
        return str(val)


def sanitize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Sanitize all values in a row dict for JSON serialization."""
    return {k: sanitize_value(v) for k, v in row.items()}


def sanitize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sanitize a list of row dicts."""
    return [sanitize_row(r) for r in rows]
