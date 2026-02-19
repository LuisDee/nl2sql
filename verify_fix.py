import sqlglot

def _escape_sql_string(value: str) -> str:
    # Need to be careful with order: backslashes first!
    return value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")

def test():
    question = "What's the price?"
    sql = "SELECT\n  *\nFROM table"
    
    e_q = _escape_sql_string(question)
    e_s = _escape_sql_string(sql)
    
    query = f"SELECT STRUCT('{e_q}' AS question, '{e_s}' AS sql_query)"
    print(f"Generated SQL:\n{query}")
    
    try:
        sqlglot.transpile(query, read="bigquery")
        print("\nSQL is valid according to sqlglot.")
    except Exception as e:
        print(f"\nSQL Syntax Error: {e}")

test()
