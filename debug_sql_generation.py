import yaml
import glob
import sqlglot
from nl2sql_agent.catalog_loader import load_all_examples, resolve_example_sql

def _escape_sql_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")

def debug():
    settings_project = "cloud-data-n-base-d4b3"
    examples = load_all_examples()
    rows = []
    for ex in examples:
        resolved_sql = resolve_example_sql(ex["sql"], settings_project)
        rows.append(
            {
                "question": ex["question"],
                "sql_query": resolved_sql.strip(),
                "tables_used": ex["tables_used"],
                "dataset": ex["dataset"],
                "complexity": ex.get("complexity", "simple"),
                "routing_signal": ex.get("routing_signal", ""),
                "validated_by": ex.get("validated_by", ""),
            }
        )

    # We only need the first batch to see if it fails
    batch = rows[:500] 
    struct_rows = []
    for r in batch:
        question = _escape_sql_string(r["question"])
        sql_query = _escape_sql_string(r["sql_query"])
        tables_str = ", ".join(f"'{t}'" for t in r["tables_used"])
        routing_signal = _escape_sql_string(r["routing_signal"])

        struct_rows.append(
            f"STRUCT('{question}' AS question, "
            f"'{sql_query}' AS sql_query, "
            f"[{tables_str}] AS tables_used, "
            f"'{r['dataset']}' AS dataset, "
            f"'{r['complexity']}' AS complexity, "
            f"'{routing_signal}' AS routing_signal, "
            f"'{r['validated_by']}' AS validated_by)"
        )

    unnest_list = ",\n            ".join(struct_rows)
    sql = f"""
    SELECT * FROM UNNEST([
    {unnest_list}
    ])
    """
    
    try:
        # Try parsing with sqlglot
        sqlglot.transpile(sql, read="bigquery")
        print("SQL is valid for the first batch.")
    except Exception as e:
        print("SQL Generation Error found!")
        print(f"Error: {e}")
        # Find where it fails
        lines = sql.split('\n')
        for i, line in enumerate(lines):
            print(f"{i+1:3}: {line}")

debug()
