"""Airflow DAG for automated Pagila and Sakila database setup.

This DAG automates the creation of PostgreSQL databases and loads
schema/data from SQL dump files stored in the project's data/ directory.
Designed for SADA task: Airflow + Airbyte + dbt pipeline.
"""

from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Project root path (adjust if your DAGs folder is nested differently)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"


@dag(
    dag_id="setup_pagila_sakila",
    default_args={
        "owner": "z10n",
        "retries": 1,
    },
    start_date=datetime(2026, 8, 1),
    schedule_interval=None,  # Manual trigger for initial deployment
    catchup=False,
    tags=["sada", "database-setup"],
    doc_md=__doc__,
)
def setup_pagila_sakila():
    """Orchestrate Pagila and Sakila database provisioning in PostgreSQL."""

    @task(task_id="create_pagila_database")
    def create_pagila_db():
        """Create the pagila database if it does not exist."""
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            # Check if database exists before creating
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'pagila';")
            if not cur.fetchone():
                cur.execute("CREATE DATABASE pagila;")
                return "pagila database created"
            else:
                return "pagila database already exists"
        conn.close()

    @task(task_id="load_pagila_schema")
    def load_pagila_schema():
        """Load Pagila schema from SQL dump into the pagila database.

        Reads the schema file and executes it against the pagila database.
        Requires a separate connection pointing to the 'pagila' database.
        """
        hook = PostgresHook(postgres_conn_id="postgres_pagila")
        sql_path = DATA_DIR / "pagila-schema.sql"
        sql_content = sql_path.read_text(encoding="utf-8")
        hook.run(sql_content)
        return "pagila schema loaded"

    @task(task_id="load_pagila_data")
    def load_pagila_data():
        """Load Pagila data using psql to handle raw pg_dump format correctly."""
        import subprocess
        import os

        # Path to the SQL dump file inside the container
        sql_path = "/opt/airflow/data/pagila-data.sql"

        # Build psql command with connection parameters for the pagila database
        cmd = [
            "psql",
            "-h", "airflow-postgres-1",
            "-U", "airflow",
            "-d", "pagila",
            "-f", sql_path
        ]

        # Pass password via environment variable to avoid interactive prompt
        env = os.environ.copy()
        env["PGPASSWORD"] = "airflow"

        # Execute psql and capture output for error handling
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)

        # Raise exception if psql exited with non-zero code
        if result.returncode != 0:
            raise Exception(f"psql failed: {result.stderr}")

        return "pagila data loaded successfully via psql"

    @task(task_id="create_sakila_database")
    def create_sakila_db():
        """Create the sakila database if it does not exist."""
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            # Check if database exists before creating
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'sakila';")
            if not cur.fetchone():
                cur.execute("CREATE DATABASE sakila;")
                return "sakila database created"
            else:
                return "sakila database already exists"
        conn.close()

    @task(task_id="load_sakila_schema")
    def load_sakila_schema():
        """Load Sakila schema from SQL dump into the sakila database."""
        hook = PostgresHook(postgres_conn_id="postgres_sakila")
        sql_path = DATA_DIR / "sakila-schema.sql"
        sql_content = sql_path.read_text(encoding="utf-8")
        hook.run(sql_content)
        return "sakila schema loaded"

    @task(task_id="load_sakila_data")
    def load_sakila_data():
        """Load Sakila data using psql to handle COPY format correctly."""
        import subprocess
        import os

        # Path to the SQL dump file inside the container
        sql_path = "/opt/airflow/data/sakila-data.sql"

        # Build psql command with connection parameters for the sakila database
        cmd = [
            "psql",
            "-h", "airflow-postgres-1",
            "-U", "airflow",
            "-d", "sakila",
            "-f", sql_path
        ]

        # Pass password via environment variable to avoid interactive prompt
        env = os.environ.copy()
        env["PGPASSWORD"] = "airflow"

        # Execute psql and capture output for error handling
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)

        # Raise exception if psql exited with non-zero code
        if result.returncode != 0:
            raise Exception(f"psql failed: {result.stderr}")

        return "sakila data loaded successfully via psql"

    # Define task dependencies explicitly
    create_pagila_db() >> load_pagila_schema() >> load_pagila_data()
    create_sakila_db() >> load_sakila_schema() >> load_sakila_data()

# Instantiate the DAG
setup_pagila_sakila()