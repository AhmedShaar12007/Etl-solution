"""
oracle_dlt_dbt_dag.py
─────────────────────
Full pipeline DAG for Airflow 3.x standalone mode.

Flow:
  FileSensor (snapshot flag)
    → DLT incremental load  (Oracle → staging PG on VM)
      → DBT staging views   (stg_orders, stg_customers)
        → DBT mart build    (fct_orders, delta_report — incremental)
          → DBT tests
            → cleanup flag

Requirements before enabling this DAG:
  1. Add fs_default connection:
     airflow connections add fs_default --conn-type fs
  2. Install packages in container:
     docker exec -u airflow airflow_webserver python -m pip install
       dlt[sql_database] oracledb dbt-core dbt-postgres python-dotenv
"""

import os
import sys
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

sys.path.insert(0, "/opt/airflow/dlt_pipelines")
from oracle_to_staging import run_pipeline as _dlt_run

DBT_DIR  = "/opt/airflow/dbt_project"
SNAP_DIR = "/mnt/snapshots"


def task_dlt_load(**context):
    result = _dlt_run(**context)
    context["ti"].xcom_push(key="dlt_result", value=result)
    print(f"[DLT] Result: {result}")
    return result


def task_dbt_run(selector):
    def _inner(**context):
        env = {**os.environ}
        cmd = [
            "dbt", "run",
            "--project-dir",  DBT_DIR,
            "--profiles-dir", DBT_DIR,
            "--select",       selector,
            "--no-version-check",
            "--no-partial-parse",
        ]
        print(f"[DBT] Running: {' '.join(cmd)}")
        proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
        print(proc.stdout)
        if proc.returncode != 0:
            print("[DBT ERROR]", proc.stderr)
            raise RuntimeError(f"dbt run failed for selector: {selector}")
        return {"selector": selector, "returncode": proc.returncode}
    _inner.__name__ = f"dbt_run_{selector}"
    return _inner


def task_dbt_test(**context):
    env = {**os.environ}
    cmd = [
        "dbt", "test",
        "--project-dir",  DBT_DIR,
        "--profiles-dir", DBT_DIR,
        "--no-version-check",
        "--no-partial-parse",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    print(proc.stdout)
    if proc.returncode != 0:
        print("[DBT TEST WARNING]", proc.stderr)
    return {"returncode": proc.returncode}


with DAG(
    dag_id      = "oracle_dlt_dbt_warehouse",
    description = "Oracle -> DLT (delta) -> Staging PG -> DBT -> Warehouse marts",
    start_date  = datetime(2024, 1, 1),
    schedule    = "@daily",
    catchup     = False,
    tags        = ["oracle", "dlt", "dbt", "warehouse", "delta"],
) as dag:

    wait_for_snapshot = FileSensor(
        task_id       = "wait_for_snapshot_flag",
        filepath      = f"{SNAP_DIR}/READY_FLAG_{{{{ ds_nodash }}}}",
        poke_interval = 10,
        timeout       = 43200,    # 12 hours — gives the snapshot plenty of time
        mode          = "poke",
    )

    dlt_load = PythonOperator(
        task_id         = "dlt_incremental_load",
        python_callable = task_dlt_load,
    )

    dbt_staging = PythonOperator(
        task_id         = "dbt_build_staging_views",
        python_callable = task_dbt_run("staging"),
    )

    dbt_marts = PythonOperator(
        task_id         = "dbt_build_marts_incremental",
        python_callable = task_dbt_run("marts"),
    )

    dbt_tests = PythonOperator(
        task_id         = "dbt_run_tests",
        python_callable = task_dbt_test,
    )

    cleanup_flag = BashOperator(
        task_id      = "cleanup_snapshot_flag",
        bash_command = f"rm -f {SNAP_DIR}/READY_FLAG_{{{{ ds_nodash }}}} && echo 'Flag removed'",
    )

    wait_for_snapshot >> dlt_load >> dbt_staging >> dbt_marts >> dbt_tests >> cleanup_flag
