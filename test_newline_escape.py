def _escape_sql_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")

test_sql = "SELECT\n  *\nFROM table"
escaped = _escape_sql_string(test_sql)
print(f"Original:\n{test_sql}")
print(f"Escaped: {escaped}")
print(f"BigQuery literal: '{escaped}'")
