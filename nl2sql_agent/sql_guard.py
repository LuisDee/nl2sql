"""Shared SQL guard: detect DML/DDL in full SQL body.

Used by both callbacks.py (before_tool_guard) and sql_executor.py
to reject non-SELECT queries. Scans the full body, not just the
first keyword, to catch patterns like WITH ... INSERT INTO.
"""

import re

_DML_DDL_KEYWORDS = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "ALTER",
        "TRUNCATE",
        "MERGE",
        "CREATE",
    }
)

# Regex that matches any of the DML/DDL keywords as whole words
_DML_PATTERN = re.compile(
    r"\b(" + "|".join(_DML_DDL_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

# Matches string literals (single-quoted, handles escaped quotes)
_STRING_LITERAL = re.compile(r"'(?:[^'\\]|\\.)*'")


def contains_dml(sql: str) -> tuple[bool, str]:
    """Check if SQL contains DML/DDL keywords anywhere in the body.

    Returns (is_blocked, reason).
    Scans full body for INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/MERGE/CREATE.
    Also rejects multiple statements (semicolons outside string literals).
    """
    if not sql or not sql.strip():
        return False, ""

    # Strip string literals to avoid false positives on keywords inside quotes
    stripped = _STRING_LITERAL.sub("''", sql)

    # Check for multiple statements (semicolons)
    if ";" in stripped:
        return (
            True,
            "Multiple statements detected (semicolon). Only single SELECT queries allowed.",
        )

    # Scan full body for DML/DDL keywords
    match = _DML_PATTERN.search(stripped)
    if match:
        keyword = match.group(1).upper()
        return (
            True,
            f"Blocked: {keyword} queries are not allowed. Only SELECT/WITH queries are permitted.",
        )

    return False, ""
