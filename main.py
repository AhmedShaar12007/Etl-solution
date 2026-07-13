"""
Pipeline Control Center — FastAPI Backend
Reads and writes config files for DLT, DBT, and Airflow projects.
"""

import json
import os
import subprocess
import requests
import toml
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List

app = FastAPI(title="Pipeline Control Center")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Paths ─────────────────────────────────────────────────────────────────
BASE         = "C:/workspace/oracle_to_pg_sim"
PROJECTS_DIR = f"{BASE}/projects"
DLT_DIR      = f"{BASE}/dlt_pipelines"
DBT_DIR      = f"{BASE}/dbt_projects"
DAGS_DIR     = f"{BASE}/dags"

AIRFLOW_URL  = "http://localhost:8080/api/v1"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin"

# ── Models ────────────────────────────────────────────────────────────────
class TableConfig(BaseModel):
    name: str
    delta: str = "auto"          # auto | timestamp | hash
    cursor: Optional[str] = None
    pk: str = "id"
    cursor_candidates: List[str] = ["updated_at","modified_date","created_at"]

class TriggerConfig(BaseModel):
    trigger_type: str            # sql | file | schedule | custom
    trigger_sql: Optional[str]  = None
    cleanup_sql: Optional[str]  = None
    flag_path: Optional[str]    = None
    cron_expression: Optional[str] = None
    custom_code: Optional[str]  = None

class ProjectConfig(BaseModel):
    project_name: str
    source_host: str
    source_port: int = 1521
    source_service: str
    source_schema: str
    warehouse_host: str
    warehouse_port: int = 5432
    warehouse_db: str
    replica_conn_id: str
    warehouse_conn_id: str
    trigger: TriggerConfig
    tables: List[TableConfig]
    dlt_pipeline_path: str = ""
    dbt_project_path: str  = ""

class ModelUpdate(BaseModel):
    sql: str

class VarsUpdate(BaseModel):
    vars: dict

# ── Helpers ───────────────────────────────────────────────────────────────
def airflow_headers():
    import base64
    creds = base64.b64encode(f"{AIRFLOW_USER}:{AIRFLOW_PASS}".encode()).decode()
    return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}

def ensure_dirs(project_name: str):
    os.makedirs(PROJECTS_DIR, exist_ok=True)
    os.makedirs(f"{DLT_DIR}/{project_name}/.dlt", exist_ok=True)
    os.makedirs(f"{DBT_DIR}/{project_name}/models/staging", exist_ok=True)
    os.makedirs(f"{DBT_DIR}/{project_name}/models/marts", exist_ok=True)

# ══════════════════════════════════════════════════════════════════════════
# PROJECTS
# ══════════════════════════════════════════════════════════════════════════

@app.get("/api/projects")
def list_projects():
    os.makedirs(PROJECTS_DIR, exist_ok=True)
    projects = []
    for f in os.listdir(PROJECTS_DIR):
        if f.endswith(".json"):
            with open(f"{PROJECTS_DIR}/{f}") as fh:
                data = json.load(fh)
            projects.append({
                "name": data.get("project_name", f.replace(".json","")),
                "trigger": data.get("trigger",{}).get("trigger_type","unknown"),
                "tables": len(data.get("tables", []))
            })
    return projects

@app.get("/api/projects/{name}")
def get_project(name: str):
    path = f"{PROJECTS_DIR}/{name}.json"
    if not os.path.exists(path):
        raise HTTPException(404, f"Project {name} not found")
    with open(path) as f:
        return json.load(f)

@app.post("/api/projects/{name}")
def save_project(name: str, config: ProjectConfig):
    ensure_dirs(name)
    config.dlt_pipeline_path = f"/opt/airflow/dlt_pipelines/{name}"
    config.dbt_project_path  = f"/opt/airflow/dbt_projects/{name}"

    path = f"{PROJECTS_DIR}/{name}.json"
    with open(path, "w") as f:
        json.dump(config.dict(), f, indent=2)

    # Auto-generate DLT config.toml
    _write_dlt_config(name, config)
    # Auto-generate DBT profiles.yml
    _write_dbt_profiles(name, config)
    # Auto-generate DBT project yml
    _write_dbt_project(name, config)
    # Auto-generate sources.yml
    _write_sources_yml(name, config)
    # Auto-generate default staging views
    _write_staging_models(name, config)

    return {"status": "saved", "project": name}


def _write_dbt_project(name: str, config: ProjectConfig):
    path = f"{DBT_DIR}/{name}/dbt_project.yml"
    if os.path.exists(path):
        return  # don't overwrite if already exists
    data = {
        "name": f"{name}_warehouse",
        "version": "1.0.0",
        "profile": f"{name}_warehouse",
        "model-paths": ["models"],
        "models": {
            f"{name}_warehouse": {
                "staging": {"+materialized": "view", "+schema": "staging"},
                "marts":   {"+materialized": "incremental", "+schema": "marts",
                            "+on_schema_change": "sync_all_columns"}
            }
        }
    }
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False)


def _write_sources_yml(name: str, config: ProjectConfig):
    """Auto-generate sources.yml from configured tables."""
    path = f"{DBT_DIR}/{name}/models/staging/sources.yml"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tables = [{"name": t.name} for t in config.tables]
    data = {
        "version": 2,
        "sources": [{
            "name": "staging",
            "schema": "staging",
            "tables": tables
        }]
    }
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False)


def _write_staging_models(name: str, config: ProjectConfig):
    """Auto-generate a default staging view for each table."""
    staging_dir = f"{DBT_DIR}/{name}/models/staging"
    os.makedirs(staging_dir, exist_ok=True)

    for table in config.tables:
        path = f"{staging_dir}/stg_{table.name}.sql"
        if os.path.exists(path):
            continue  # don't overwrite models the user already wrote

        sql = f"""{{{{ config(materialized='view') }}}}

SELECT
    *
FROM {{{{ source('staging', '{table.name}') }}}}
"""
        with open(path, "w") as f:
            f.write(sql)

def _write_dlt_config(name: str, config: ProjectConfig):
    cfg = {
        "source": {
            "oracle": {
                "host": config.source_host,
                "port": config.source_port,
                "service": config.source_service,
                "schema": config.source_schema,
            }
        },
        "destination": {
            "postgres": {
                "host": config.warehouse_host,
                "port": config.warehouse_port,
                "database": config.warehouse_db,
            }
        },
        "pipeline": {
            "name": f"{name}_pipeline",
            "dataset_name": "staging"
        },
        "tables": {
            t.name: {
                "delta": t.delta,
                "cursor": t.cursor or "",
                "primary_key": t.pk,
                "cursor_candidates": t.cursor_candidates,
            }
            for t in config.tables
        }
    }
    with open(f"{DLT_DIR}/{name}/.dlt/config.toml", "w") as f:
        toml.dump(cfg, f)

def _write_dbt_profiles(name: str, config: ProjectConfig):
    profiles = {
        f"{name}_warehouse": {
            "target": "prod",
            "outputs": {
                "prod": {
                    "type": "postgres",
                    "host": config.warehouse_host,
                    "port": config.warehouse_port,
                    "dbname": config.warehouse_db,
                    "user": "dlt_user",
                    "password": "dlt_pass",
                    "schema": "marts",
                    "threads": 4
                }
            }
        }
    }
    with open(f"{DBT_DIR}/{name}/profiles.yml", "w") as f:
        yaml.dump(profiles, f, default_flow_style=False)

# ══════════════════════════════════════════════════════════════════════════
# DLT — TABLE DELTA CONFIG
# ══════════════════════════════════════════════════════════════════════════

@app.get("/api/projects/{name}/dlt")
def get_dlt_config(name: str):
    path = f"{DLT_DIR}/{name}/.dlt/config.toml"
    if not os.path.exists(path):
        return {"tables": {}, "pipeline": {}, "source": {}}
    with open(path) as f:
        return toml.load(f)

@app.post("/api/projects/{name}/dlt/tables")
def save_dlt_tables(name: str, tables: List[TableConfig]):
    path = f"{DLT_DIR}/{name}/.dlt/config.toml"
    if not os.path.exists(path):
        raise HTTPException(404, "DLT config not found — save project first")

    with open(path) as f:
        cfg = toml.load(f)

    cfg["tables"] = {
        t.name: {
            "delta": t.delta,
            "cursor": t.cursor or "",
            "primary_key": t.pk,
            "cursor_candidates": t.cursor_candidates,
        }
        for t in tables
    }

    with open(path, "w") as f:
        toml.dump(cfg, f)

    return {"status": "saved", "tables": len(tables)}

# ══════════════════════════════════════════════════════════════════════════
# DBT — MODELS
# ══════════════════════════════════════════════════════════════════════════

@app.get("/api/projects/{name}/dbt/models")
def list_models(name: str):
    models = []
    for schema in ["staging", "marts"]:
        folder = f"{DBT_DIR}/{name}/models/{schema}"
        if os.path.exists(folder):
            for f in os.listdir(folder):
                if f.endswith(".sql"):
                    models.append({"schema": schema, "name": f.replace(".sql",""), "file": f})
    return models

@app.get("/api/projects/{name}/dbt/models/{schema}/{model}")
def get_model(name: str, schema: str, model: str):
    path = f"{DBT_DIR}/{name}/models/{schema}/{model}.sql"
    if not os.path.exists(path):
        return {"sql": "", "exists": False}
    with open(path) as f:
        return {"sql": f.read(), "exists": True}

@app.post("/api/projects/{name}/dbt/models/{schema}/{model}")
def save_model(name: str, schema: str, model: str, body: ModelUpdate):
    os.makedirs(f"{DBT_DIR}/{name}/models/{schema}", exist_ok=True)
    path = f"{DBT_DIR}/{name}/models/{schema}/{model}.sql"
    with open(path, "w") as f:
        f.write(body.sql)
    return {"status": "saved", "path": path}

@app.get("/api/projects/{name}/dbt/vars")
def get_dbt_vars(name: str):
    path = f"{DBT_DIR}/{name}/dbt_project.yml"
    if not os.path.exists(path):
        return {"vars": {}}
    with open(path) as f:
        data = yaml.safe_load(f)
    return {"vars": data.get("vars", {})}

@app.post("/api/projects/{name}/dbt/vars")
def save_dbt_vars(name: str, body: VarsUpdate):
    path = f"{DBT_DIR}/{name}/dbt_project.yml"
    if not os.path.exists(path):
        # Create basic dbt_project.yml
        data = {
            "name": f"{name}_warehouse",
            "version": "1.0.0",
            "profile": f"{name}_warehouse",
            "model-paths": ["models"],
            "models": {
                f"{name}_warehouse": {
                    "staging": {"+materialized": "view", "+schema": "staging"},
                    "marts":   {"+materialized": "incremental", "+schema": "marts",
                                "+on_schema_change": "sync_all_columns"}
                }
            }
        }
    else:
        with open(path) as f:
            data = yaml.safe_load(f)

    data["vars"] = body.vars

    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False)

    return {"status": "saved", "vars": body.vars}

# ══════════════════════════════════════════════════════════════════════════
# AIRFLOW
# ══════════════════════════════════════════════════════════════════════════

@app.get("/api/airflow/dags")
def list_dags():
    try:
        r = requests.get(f"{AIRFLOW_URL}/dags",
                        headers=airflow_headers(), timeout=5)
        if r.status_code == 200:
            dags = r.json().get("dags", [])
            return [{"dag_id": d["dag_id"],
                     "is_paused": d["is_paused"],
                     "last_parsed_time": d.get("last_parsed_time","")}
                    for d in dags]
        return []
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/airflow/dags/{dag_id}/runs")
def get_dag_runs(dag_id: str):
    try:
        r = requests.get(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns?limit=5&order_by=-execution_date",
            headers=airflow_headers(), timeout=5)
        if r.status_code == 200:
            return r.json().get("dag_runs", [])
        return []
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/airflow/trigger/{dag_id}")
def trigger_dag(dag_id: str):
    try:
        r = requests.post(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns",
            headers=airflow_headers(),
            json={"conf": {}},
            timeout=5)
        if r.status_code in [200, 201]:
            return {"status": "triggered", "dag_id": dag_id}
        return {"status": "error", "detail": r.text}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/airflow/pause/{dag_id}")
def pause_dag(dag_id: str):
    try:
        r = requests.patch(
            f"{AIRFLOW_URL}/dags/{dag_id}",
            headers=airflow_headers(),
            json={"is_paused": True}, timeout=5)
        return {"status": "paused"} if r.status_code == 200 else {"error": r.text}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/airflow/unpause/{dag_id}")
def unpause_dag(dag_id: str):
    try:
        r = requests.patch(
            f"{AIRFLOW_URL}/dags/{dag_id}",
            headers=airflow_headers(),
            json={"is_paused": False}, timeout=5)
        return {"status": "unpaused"} if r.status_code == 200 else {"error": r.text}
    except Exception as e:
        return {"error": str(e)}

# ══════════════════════════════════════════════════════════════════════════
# DEPLOY
# ══════════════════════════════════════════════════════════════════════════

@app.post("/api/deploy/{name}")
def deploy_project(name: str):
    results = {}

    # 1. Write factory.py to dags folder automatically
    factory_dest = f"{BASE}/dags/factory.py"
    if not os.path.exists(factory_dest):
        _write_factory(factory_dest)
        results["factory"] = "created"
    else:
        results["factory"] = "already exists"

    # 2. Copy pipeline.py to client folder if not there
    client_pipeline = f"{DLT_DIR}/{name}/pipeline.py"
    base_pipeline   = f"{BASE}/dlt_pipeline.py"
    os.makedirs(f"{DLT_DIR}/{name}", exist_ok=True)
    if not os.path.exists(client_pipeline) and os.path.exists(base_pipeline):
        import shutil
        shutil.copy(base_pipeline, client_pipeline)
        results["pipeline_py"] = "copied"
    else:
        results["pipeline_py"] = "already exists"

    # 3. Add Airflow connections via REST API
    project_path = f"{PROJECTS_DIR}/{name}.json"
    if os.path.exists(project_path):
        with open(project_path) as f:
            cfg = json.load(f)

        # Add Oracle connection via REST API
        oracle_conn = {
            "connection_id": cfg["replica_conn_id"],
            "conn_type": "oracle",
            "host": cfg["source_host"],
            "port": cfg["source_port"],
            "login": "demo_user",
            "password": "DemoPass123",
            "schema": cfg["source_service"]
        }
        try:
            r = requests.post(
                f"{AIRFLOW_URL}/connections",
                headers=airflow_headers(),
                json=oracle_conn,
                timeout=5
            )
            results["oracle_conn"] = "created" if r.status_code in [200,201] else f"exists or error: {r.status_code}"
        except Exception as e:
            results["oracle_conn"] = f"error: {str(e)}"

        # Add PostgreSQL connection via REST API
        pg_conn = {
            "connection_id": cfg["warehouse_conn_id"],
            "conn_type": "postgres",
            "host": cfg["warehouse_host"],
            "port": cfg["warehouse_port"],
            "login": "dlt_user",
            "password": "dlt_pass",
            "schema": cfg["warehouse_db"]
        }
        try:
            r = requests.post(
                f"{AIRFLOW_URL}/connections",
                headers=airflow_headers(),
                json=pg_conn,
                timeout=5
            )
            results["pg_conn"] = "created" if r.status_code in [200,201] else f"exists or error: {r.status_code}"
        except Exception as e:
            results["pg_conn"] = f"error: {str(e)}"

    # 4. Restart Airflow to pick up new DAG
    restart = subprocess.run(
        "docker restart airflow_webserver",
        shell=True, capture_output=True, text=True
    )
    results["restart"] = restart.stdout.strip()

    return {"status": "deployed", "project": name, "results": results}


def _write_factory(dest_path: str):
    """Write factory.py to the dags folder."""
    content = r'''"""
factory.py — auto-discovers all projects in /opt/airflow/projects/
and generates one DAG per project. Drop a JSON file = new pipeline live.
"""
import json, os, subprocess, sys
from airflow.decorators import dag
from airflow.providers.standard.operators.python import PythonOperator

PROJECTS_DIR = "/opt/airflow/projects"

def make_dlt_runner(project_path):
    def run_dlt(**context):
        import toml
        cfg     = toml.load(os.path.join(project_path, ".dlt", "config.toml"))
        secrets = toml.load(os.path.join(project_path, ".dlt", "secrets.toml"))
        oracle  = {**cfg["source"]["oracle"],        **secrets["source"]["oracle"]}
        pg      = {**cfg["destination"]["postgres"],  **secrets["destination"]["postgres"]}
        tables  = cfg.get("tables", {})
        pipe    = cfg["pipeline"]

        import dlt, sqlalchemy
        from dlt.sources.sql_database import sql_database
        from dlt.sources import incremental as Incremental

        sources = []
        for table_name, tcfg in tables.items():
            delta  = tcfg.get("delta", "timestamp")
            cursor = tcfg.get("cursor", "updated_at")
            pk     = tcfg.get("primary_key", "id")
            src    = sql_database(schema=oracle["schema"], table_names=[table_name])

            if delta == "timestamp":
                src[table_name].apply_hints(
                    incremental=Incremental(cursor_path=cursor, initial_value=None),
                    primary_key=pk)
            elif delta == "hash":
                src[table_name].apply_hints(write_disposition="replace", primary_key=pk)
            elif delta == "auto":
                candidates = tcfg.get("cursor_candidates", ["updated_at","created_at"])
                url = (f"oracle+oracledb://{oracle['username']}:{oracle['password']}"
                       f"@{oracle['host']}:{oracle['port']}/?service_name={oracle['service']}")
                engine = sqlalchemy.create_engine(url)
                found  = None
                with engine.connect() as conn:
                    rows = conn.execute(sqlalchemy.text(
                        "SELECT LOWER(column_name) FROM all_tab_columns "
                        "WHERE LOWER(owner)=:s AND LOWER(table_name)=:t "
                        "AND data_type LIKE 'TIMESTAMP%'"
                    ), {"s": oracle["schema"].lower(), "t": table_name.lower()})
                    existing = {r[0] for r in rows}
                for c in candidates:
                    if c.lower() in existing:
                        found = c
                        break
                if found:
                    print(f"[{table_name}] auto -> timestamp on {found}")
                    src[table_name].apply_hints(
                        incremental=Incremental(cursor_path=found, initial_value=None),
                        primary_key=pk)
                else:
                    print(f"[{table_name}] auto -> full hash")
                    src[table_name].apply_hints(write_disposition="replace", primary_key=pk)

            sources.append(src)

        wh_url = (f"postgresql://{pg['username']}:{pg['password']}"
                  f"@{pg['host']}:{pg['port']}/{pg['database']}")
        pipeline = dlt.pipeline(
            pipeline_name=pipe["name"],
            destination=dlt.destinations.postgres(credentials=wh_url),
            dataset_name=pipe["dataset_name"])
        print(pipeline.run(sources))
    return run_dlt


def make_dbt_runner(dbt_path):
    def run_dbt(**context):
        result = subprocess.run(
            ["dbt","run","--profiles-dir",dbt_path,"--project-dir",dbt_path,
             "--no-partial-parse","--no-version-check"],
            capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"DBT failed:\n{result.stderr}")
    return run_dbt


def make_pipeline(config: dict):
    project_name = config["project_name"]
    trigger_cfg  = config.get("trigger", {})
    trigger_type = trigger_cfg.get("trigger_type", "sql")
    dlt_path = config.get("dlt_pipeline_path", f"/opt/airflow/dlt_pipelines/{project_name}")
    dbt_path = config.get("dbt_project_path",  f"/opt/airflow/dbt_projects/{project_name}")

    @dag(dag_id=f"{project_name}_pipeline", schedule=None,
         catchup=False, tags=[project_name, "factory"])
    def pipeline():
        if trigger_type == "sql":
            from airflow.providers.common.sql.sensors.sql import SqlSensor
            wait = SqlSensor(
                task_id="wait_for_replica_load",
                conn_id=config.get("replica_conn_id", f"{project_name}_oracle_replica"),
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

        dlt_task = PythonOperator(task_id="dlt_load",  python_callable=make_dlt_runner(dlt_path))
        dbt_task = PythonOperator(task_id="dbt_build", python_callable=make_dbt_runner(dbt_path))

        if trigger_type in ["sql","file"]:
            wait >> dlt_task >> dbt_task
        else:
            dlt_task >> dbt_task

    return pipeline()


if os.path.exists(PROJECTS_DIR):
    for fname in os.listdir(PROJECTS_DIR):
        if fname.endswith(".json"):
            with open(os.path.join(PROJECTS_DIR, fname)) as f:
                cfg = json.load(f)
            globals()[f"{cfg['project_name']}_pipeline"] = make_pipeline(cfg)
'''
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "w") as f:
        f.write(content)

@app.post("/api/run_dbt/{name}")
def run_dbt(name: str):
    """Run DBT manually for a project."""
    dbt_path = f"{DBT_DIR}/{name}"
    if not os.path.exists(dbt_path):
        raise HTTPException(404, "DBT project not found")

    result = subprocess.run(
        f'docker exec -u airflow airflow_webserver bash -c '
        f'"cd /opt/airflow/dbt_projects/{name} && '
        f'dbt run --profiles-dir . --no-partial-parse --no-version-check"',
        shell=True, capture_output=True, text=True
    )
    return {
        "returncode": result.returncode,
        "stdout": result.stdout[-3000:],
        "stderr": result.stderr[-1000:]
    }

@app.post("/api/run_dlt/{name}")
def run_dlt_manual(name: str):
    """Run DLT manually for a project."""
    result = subprocess.run(
        f'docker exec -u airflow airflow_webserver python '
        f'/opt/airflow/dlt_pipelines/{name}/pipeline.py',
        shell=True, capture_output=True, text=True
    )
    return {
        "returncode": result.returncode,
        "stdout": result.stdout[-3000:],
        "stderr": result.stderr[-1000:]
    }

# ── Serve static files ────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=5050, reload=True)
