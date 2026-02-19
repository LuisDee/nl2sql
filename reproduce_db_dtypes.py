import pandas as pd
from google.cloud import bigquery
from nl2sql_agent.config import settings

def reproduce():
    client = bigquery.Client(project=settings.gcp_project)
    # A simple query that returns a timestamp
    sql = "SELECT CURRENT_TIMESTAMP() as ts"
    print(f"Executing query: {sql}")
    try:
        query_job = client.query(sql)
        df = query_job.to_dataframe()
        print("Success!")
        print(df)
    except Exception as e:
        print(f"Caught expected exception: {e}")

if __name__ == "__main__":
    reproduce()
