import json
import os
import subprocess
from airflow.decorators import dag
from airflow.providers.standard.operators.python import PythonOperator

PROJECTS_DIR = "/opt/airflow/projects"


def make_dlt_runner(project_path):
    def run_dlt(**context):
        import toml, dlt, sqlalchemy
        from dlt.sources.sql_database import sql_database
        from dlt.sources import incremental as Incremental

        cfg     = toml.load(os.path.join(project_path, ".dlt", "config.toml"))
        secrets = toml.load(os.path.join(project_path, ".dlt", "secrets.toml"))
        oracle  = dict(list(cfg["source"]["oracle"].items()) + list(secrets["source"]["oracle"].items()))
        pg      = dict(list(cfg["destination"]["postgres"].items()) + list(secrets["destination"]["postgres"].items()))
        tables  = cfg.get("tables", {})
        pipe    = cfg["pipeline"]
        sources = []

        for table_name, tcfg in tables.items():
            delta  = tcfg.get("delta", "timestamp")
            cursor = tcfg.get("cursor", "updated_at")
            pk     = tcfg.get("primary_key", "id")
            conn_str = "oracle+oracledb://{}:{}@{}:{}/?service_name={}".format(
                oracle["username"], oracle["password"],
                oracle["host"], oracle["port"], oracle["service"])
            src = sql_database(credentials=conn_str, schema=oracle["schema"], table_names=[table_name])

            if delta == "timestamp":
                src.resources[table_name].apply_hints(
                    incremental=Incremental(cursor_path=cursor, initial_value=None),
                    primary_key=pk)
            elif delta == "hash":
                src.resources[table_name].apply_hints(
                    write_disposition="replace", primary_key=pk)
            elif delta == "auto":
                candidates = tcfg.get("cursor_candidates", ["updated_at", "created_at"])
                engine = sqlalchemy.create_engine(conn_str)
                found = None
                with engine.connect() as conn:
                    rows = conn.execute(sqlalchemy.text(
                        "SELECT LOWER(column_name) FROM all_tab_columns "
                        "WHERE LOWER(owner)=:s AND LOWER(table_name)=:t "
                        "AND data_type LIKE 'TIMESTAMP%'"),
                        {"s": oracle["schema"].lower(), "t": table_name.lower()})
                    existing = {r[0] for r in rows}
                for c in candidates:
                    if c.lower() in existing:
                        found = c
                        break
                if found:
                    print("[{}] auto -> timestamp on {}".format(table_name, found))
                    src.resources[table_name].apply_hints(
                        incremental=Incremental(cursor_path=found, initial_value=None),
                        primary_key=pk)
                else:
                    print("[{}] auto -> full hash".format(table_name))
                    src.resources[table_name].apply_hints(
                        write_disposition="replace", primary_key=pk)
            sources.append(src)

        wh_url = "postgresql://{}:{}@{}:{}/{}".format(
            pg["username"], pg["password"], pg["host"], pg["port"], pg["database"])
        pipeline = dlt.pipeline(
            pipeline_name=pipe["name"],
            destination=dlt.destinations.postgres(credentials=wh_url),
            dataset_name=pipe["dataset_name"])
        print(pipeline.run(sources))
    return run_dlt


def make_dbt_runner(dbt_path):
    def run_dbt(**context):
        result = subprocess.run(
            ["/home/airflow/.local/bin/dbt", "run",
             "--profiles-dir", dbt_path, "--project-dir", dbt_path,
             "--no-partial-parse", "--no-version-check"],
            capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            raise Exception("DBT failed: " + result.stderr)
    return run_dbt


def make_pipeline(config):
    project_name = config["project_name"]
    trigger_cfg  = config.get("trigger", {})
    trigger_type = trigger_cfg.get("trigger_type", "sql")
    dlt_path     = config.get("dlt_pipeline_path", "/opt/airflow/dlt_pipelines/" + project_name)
    dbt_path     = config.get("dbt_project_path",  "/opt/airflow/dbt_projects/"  + project_name)
    cleanup_sql  = trigger_cfg.get("cleanup_sql", "")
    replica_conn = config.get("replica_conn_id", project_name + "_oracle_replica")
    schedule     = trigger_cfg.get("cron_expression", "0 2 * * *") if trigger_type == "schedule" else "*/2 * * * *"

    @dag(dag_id=project_name + "_pipeline",
         schedule=schedule, catchup=False, max_active_runs=1,
         tags=[project_name, "factory"])
    def pipeline():

        if trigger_type == "sql":
            from airflow.providers.common.sql.sensors.sql import SqlSensor
            wait = SqlSensor(
                task_id="wait_for_replica_load",
                conn_id=replica_conn,
                sql=trigger_cfg.get("trigger_sql",
                    "SELECT COUNT(*) FROM load_status WHERE status='COMPLETE' AND processed=0"),
                poke_interval=10, timeout=43200, mode="poke")

        elif trigger_type == "file":
            from airflow.providers.standard.sensors.filesystem import FileSensor
            wait = FileSensor(
                task_id="wait_for_flag",
                filepath=trigger_cfg.get("flag_path", "/mnt/snapshots"),
                fs_conn_id="fs_default",
                poke_interval=10, timeout=43200, mode="poke")

        elif trigger_type == "custom":
            custom_code = trigger_cfg.get("custom_code", "")
            wait = None
            if custom_code:
                local_vars = {}
                exec(custom_code, globals(), local_vars)
                wait = local_vars.get("wait", None)
                if wait is None:
                    print("[WARNING] Custom trigger code did not define a 'wait' variable")
            else:
                print("[WARNING] Custom trigger selected but no code provided")

        else:
            wait = None

        dlt_task = PythonOperator(
            task_id="dlt_load",
            python_callable=make_dlt_runner(dlt_path))

        dbt_task = PythonOperator(
            task_id="dbt_build",
            python_callable=make_dbt_runner(dbt_path))

        if cleanup_sql and trigger_type == "sql":
            from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
            cleanup = SQLExecuteQueryOperator(
                task_id="mark_processed",
                conn_id=replica_conn,
                sql=cleanup_sql)
            if wait:
                wait >> dlt_task >> dbt_task >> cleanup
            else:
                dlt_task >> dbt_task >> cleanup
        else:
            if wait:
                wait >> dlt_task >> dbt_task
            else:
                dlt_task >> dbt_task

    return pipeline()


if os.path.exists(PROJECTS_DIR):
    for fname in os.listdir(PROJECTS_DIR):
        if fname.endswith(".json"):
            with open(os.path.join(PROJECTS_DIR, fname)) as f:
                cfg = json.load(f)
            globals()[cfg["project_name"] + "_pipeline"] = make_pipeline(cfg)