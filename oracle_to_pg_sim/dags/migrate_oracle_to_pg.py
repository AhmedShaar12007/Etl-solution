from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

# Naked connection URL string parameters matching your container network setup
ORACLE_CONN_STRING = "oracle+oracledb://SALES_APP:YourPassword123@oracle_db:1521/XE"
PG_URL = "postgresql://postgres:warehouse_pass@postgres_dw:5432/warehouse"

def run_dlt_pipeline_native():
    # Hidden inside the execution function to prevent background validation thread failures
    import dlt
    from dlt.sources.sql_database import sql_database
    import os
    
    # Enforces writing file cache allocations cleanly inside an unlocked directory
    os.environ["DLT_PROJECT_DIR"] = "/tmp"
    
    print(">>> INITIALIZING NATIVE DLT PIPELINE CORE <<<")
    pipeline = dlt.pipeline(
        pipeline_name="oracle_to_postgres_bulk",
        destination=dlt.destinations.postgres(credentials=PG_URL),
        dataset_name="staging_data"
    )
    
    print(">>> ATTEMPTING SOCKET STREAM CONNECTION TO ORACLE CONTAINER <<<")
    # FIXED: Using lowercase 'sales_data' to match dlt's auto-discovery resources mapping
    source = sql_database(credentials=ORACLE_CONN_STRING).with_resources("sales_data")
    
    print(">>> PIPELINE RUNNING: STREAMING DATA VIA HIGH-SPEED CSV COPY <<<")
    load_info = pipeline.run(source, write_disposition="replace", loader_file_format="csv")
    print(">>> PIPELINE STREAM COMPLETED SUCCESSFULLY <<<")
    print(load_info)

with DAG(
    dag_id='dlt_only_million_row_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Airflow 3.0 Required Syntax
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id='run_dlt_only_load',
        python_callable=run_dlt_pipeline_native
    )
