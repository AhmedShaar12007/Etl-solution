import dlt
from dlt.sources.sql_database import sql_database

ORACLE_CONN_STRING = "oracle+oracledb://SALES_APP:YourPassword123@oracle_db:1521/XE"
PG_URL = "postgresql://postgres:warehouse_pass@postgres_dw:5432/warehouse"

def execute_load():
    print(">>> INITIALIZING DLT PIPELINE CORE ENGINE <<<")
    pipeline = dlt.pipeline(
        pipeline_name="oracle_to_postgres_bulk",
        destination=dlt.destinations.postgres(credentials=PG_URL),
        dataset_name="staging_data"
    )
    source = sql_database(credentials=ORACLE_CONN_STRING).with_resources("sales_data")
    load_info = pipeline.run(source, write_disposition="replace", loader_file_format="parquet")
    print(">>> PIPELINE STREAM COMPLETED SUCCESSFULLY <<<")
    print(load_info)

if __name__ == "__main__":
    execute_load()
