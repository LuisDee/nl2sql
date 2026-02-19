from nl2sql_agent.tools.sql_executor import execute_sql
from nl2sql_agent.tools._deps import init_bq_service
from nl2sql_agent.clients import LiveBigQueryClient
from nl2sql_agent.config import settings

def verify():
    # Initialize dependencies
    bq_client = LiveBigQueryClient(
        project=settings.gcp_project, location=settings.bq_location
    )
    init_bq_service(bq_client)

    sql = "SELECT CURRENT_TIMESTAMP() as ts"
    print(f"Executing query via execute_sql tool: {sql}")
    
    result = execute_sql(sql)
    
    if result.get("status") == "success":
        print("Success!")
        print(f"Row count: {result.get('row_count')}")
        print(f"Rows: {result.get('rows')}")
        return True
    else:
        print("Failed!")
        print(f"Error message: {result.get('error_message')}")
        return False

if __name__ == "__main__":
    if verify():
        exit(0)
    else:
        exit(1)
