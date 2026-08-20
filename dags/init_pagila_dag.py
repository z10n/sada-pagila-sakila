"""
DAG: init_pagila_db
Description: Automates Pagila database setup with in-memory SQL syntax adaptation and COPY parsing for Postgres.
"""

import io
import re
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def is_executable_sql(sql_text: str) -> bool:
    """
    Checks whether a given SQL string contains actual executable statements
    and is not comprised solely of whitespace or SQL comments.
    """
    cleaned = re.sub(r'--.*$', '', sql_text, flags=re.MULTILINE)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    return bool(cleaned.strip().strip(';').strip())


@dag(
    dag_id='init_pagila_db',
    default_args=DEFAULT_ARGS,
    description='Automated initialization of Pagila database (Schema + Data)',
    schedule='@once',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['setup', 'pagila', 'postgres'],
)
def init_pagila_db():
    """
    Executes SQL scripts by reading files and performing in-memory engine-specific transformations,
    handling Postgres COPY FROM stdin blocks natively.
    """

    @task
    def execute_sql_in_memory(file_path: str, conn_id: str, is_schema: bool = False) -> None:
        """
        Reads a raw SQL script, adapts syntax in RAM, processes COPY ... FROM stdin blocks
        via psycopg2 copy_expert, filters out incompatible GUC parameters, and executes statements idempotently.

        :param file_path: Absolute path to the raw .sql file
        :param conn_id: Airflow Connection ID for target Postgres database
        :param is_schema: Flag indicating if the script is creating a schema
        """
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        with open(file_path, 'r', encoding='utf-8') as f:
            raw_sql = f.read()

        # Perform in-memory SQL dialect adaptations for PostgreSQL 15
        adapted_sql = (
            raw_sql.replace(' VIRTUAL', ' STORED')
                   .replace('uuidv7()', 'gen_random_uuid()')
        )

        # Wipe schema for idempotency on schema tasks
        if is_schema:
            cursor.execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
            conn.commit()

        def flush_buffer(buf: list) -> list:
            """Executes SQL statements in buffer if non-empty and non-comment."""
            query = "\n".join(buf).strip()
            if is_executable_sql(query):
                cursor.execute(query)
            return []

        # Parse and execute SQL lines, handling COPY ... FROM stdin streams and unsupported parameters
        lines = adapted_sql.splitlines()
        in_copy = False
        copy_cmd = ""
        copy_data = []
        buffer = []

        for line in lines:
            # Ignore unsupported transaction_timeout parameter in Postgres 15
            if "transaction_timeout" in line.lower():
                continue

            if in_copy:
                if line.strip() == r"\.":
                    in_copy = False
                    cursor.copy_expert(copy_cmd, io.StringIO("\n".join(copy_data) + "\n"))
                    copy_cmd = ""
                    copy_data = []
                else:
                    copy_data.append(line)
            else:
                if line.strip().upper().startswith("COPY ") and "FROM STDIN" in line.strip().upper():
                    buffer = flush_buffer(buffer)
                    in_copy = True
                    copy_cmd = line
                else:
                    buffer.append(line)

        flush_buffer(buffer)

        conn.commit()
        cursor.close()
        conn.close()

    create_schema = execute_sql_in_memory.override(task_id='create_pagila_schema')(
        file_path='/opt/airflow/sql/pagila/schema.sql',
        conn_id='postgres_pagila_conn',
        is_schema=True
    )

    insert_data = execute_sql_in_memory.override(task_id='insert_pagila_data')(
        file_path='/opt/airflow/sql/pagila/data.sql',
        conn_id='postgres_pagila_conn',
        is_schema=False
    )

    create_schema >> insert_data


init_pagila_db()