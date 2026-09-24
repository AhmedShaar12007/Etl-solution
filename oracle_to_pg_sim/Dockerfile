FROM apache/airflow:latest

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-oracle \
    dlt[sql_database] \
    oracledb \
    dbt-core \
    dbt-postgres \
    toml \
    pyyaml \
    requests \
    sqlalchemy
