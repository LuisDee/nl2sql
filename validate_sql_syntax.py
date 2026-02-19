import yaml
import glob
import sqlglot
from nl2sql_agent.catalog_loader import resolve_example_sql

def check_sql():
    project_id = "cloud-data-n-base-d4b3"
    example_files = glob.glob("examples/*.yaml")
    for file_path in example_files:
        print(f"Checking SQL syntax in {file_path}...")
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
            examples = data.get('examples', [])
            for i, ex in enumerate(examples):
                sql = ex.get('sql', '')
                question = ex.get('question', '')
                try:
                    resolved = resolve_example_sql(sql, project_id)
                    # Try to parse it as BigQuery SQL
                    sqlglot.transpile(resolved, read="bigquery")
                except Exception as e:
                    print(f"  [!] SQL Syntax Error at index {i} in {file_path}")
                    print(f"      Q: {question}")
                    print(f"      SQL: {sql}")
                    print(f"      Error: {str(e)}")
                    print("-" * 20)

check_sql()
