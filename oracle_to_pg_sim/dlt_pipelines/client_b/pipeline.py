import os
import json
import logging
from datetime import datetime, timezone

import dlt
from dlt.sources.sql_database import sql_database
from dlt.sources import incremental as Incremental

log = logging.getLogger(__name__)


def _oracle_credentials() -> dict:
    return {
        "drivername": "oracle+oracledb",
        "username":   os.environ["ORACLE_USER"],
        "password":   os.environ["ORACLE_PASS"],
        "host":       os.environ["ORACLE_HOST"],
        "port":       int(os.environ.get("ORACLE_PORT", 1521)),
        "database":   os.environ["ORACLE_SERVICE"],
    }


def _warehouse_url() -> str:
    return os.environ["WAREHOUSE_PG_URL"]


def build_source():
    orders = sql_database(
        credentials=_oracle_credentials(),
        schema="DEMO_USER",
        table_names=["orders"],
    ).with_resources("orders")

    orders.orders.apply_hints(
        incremental=Incremental(
            cursor_path="updated_at",
            initial_value=datetime(2020, 1, 1, tzinfo=timezone.utc),
        ),
        write_disposition="replace"
    )

    customers = sql_database(
        credentials=_oracle_credentials(),
        schema="DEMO_USER",
        table_names=["customers"],
    ).with_resources("customers")

    customers.customers.apply_hints(
        incremental=Incremental(
            cursor_path="created_at",
            initial_value=datetime(2020, 1, 1, tzinfo=timezone.utc),
        ),
        write_disposition="append"
    )

    return [orders, customers]


def run_pipeline(**context) -> dict:
    run_date = context.get("ds", str(datetime.now().date()))

    pipeline = dlt.pipeline(
        pipeline_name="oracle_to_staging",
        destination=dlt.destinations.postgres(credentials=_warehouse_url()),
        dataset_name="staging",
    )

    load_info = pipeline.run(build_source())
    print(f"\nDLT load complete: {load_info}")

    result = {
        "status":   "success",
        "run_date": run_date,
    }
    return result


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

    result = run_pipeline(ds=str(datetime.now().date()))
    print("\n── DLT result ──────────────────────────────")
    print(json.dumps(result, indent=2))