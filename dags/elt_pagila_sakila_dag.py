import json
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- Airbyte Configuration ---
AIRBYTE_HOST = 'http://host.docker.internal:8000'
AIRBYTE_CLIENT_ID = 'a0cd7a8e-e9ef-491d-8bc8-f469652a1810'
AIRBYTE_CLIENT_SECRET = 'xmjVCIEiIpOnXPRJbieFD0yT3I9Xjdzb'

# --- Connection GUIDs from Airbyte UI ---
PAGILA_CONN_ID = 'c00b1195-b621-4ab9-b3ab-9416580dee19'
SAKILA_CONN_ID = '78d337b3-9983-4b83-a789-2fd36edd2725'

@task
def trigger_airbyte_sync(connection_id: str):
    """Fetches OAuth token and triggers Airbyte synchronization using pure Python."""
    # 1. Request access token
    token_url = f"{AIRBYTE_HOST}/api/v1/applications/token"
    token_payload = json.dumps({
        "client_id": AIRBYTE_CLIENT_ID,
        "client_secret": AIRBYTE_CLIENT_SECRET,
        "grant_type": "client_credentials"
    }).encode('utf-8')

    token_req = urllib.request.Request(
        token_url,
        data=token_payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    with urllib.request.urlopen(token_req) as resp:
        token_data = json.loads(resp.read().decode('utf-8'))
        access_token = token_data.get("access_token")

    if not access_token:
        raise ValueError("Failed to retrieve access_token from Airbyte API")

    # 2. Trigger connection sync
    sync_url = f"{AIRBYTE_HOST}/api/v1/connections/sync"
    sync_payload = json.dumps({"connectionId": connection_id}).encode('utf-8')

    sync_req = urllib.request.Request(
        sync_url,
        data=sync_payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(sync_req) as resp:
            print(f"Airbyte sync triggered successfully for connection: {connection_id} (HTTP {resp.status})")
    except urllib.error.HTTPError as e:
        if e.code == 409:
            print(f"Airbyte sync is already in progress for connection: {connection_id} (HTTP 409 Conflict). Skipping trigger.")
        else:
            error_body = e.read().decode('utf-8')
            raise RuntimeError(f"Airbyte API request failed with HTTP {e.code}: {error_body}")


@dag(
    dag_id='elt_pagila_sakila_pipeline',
    default_args=default_args,
    description='Automated ELT pipeline: Airbyte sync -> dbt run -> dbt test',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['elt', 'snowflake', 'dbt', 'airbyte'],
)
def elt_pagila_sakila_pipeline():

    # Trigger Airbyte syncs in parallel
    sync_pagila = trigger_airbyte_sync.override(task_id='airbyte_sync_pagila')(PAGILA_CONN_ID)
    sync_sakila = trigger_airbyte_sync.override(task_id='airbyte_sync_sakila')(SAKILA_CONN_ID)

    # Execute dbt transformation models
    run_dbt_models = BashOperator(
        task_id='dbt_run_transformations',
        bash_command='cd /opt/airflow/dbt_transform && /opt/airflow/dbt_venv/bin/dbt run --profiles-dir /opt/airflow/.dbt'
    )

    # Execute dbt quality tests
    test_dbt_models = BashOperator(
        task_id='dbt_test_quality',
        bash_command='cd /opt/airflow/dbt_transform && /opt/airflow/dbt_venv/bin/dbt test --profiles-dir /opt/airflow/.dbt'
    )

    # Pipeline execution flow
    [sync_pagila, sync_sakila] >> run_dbt_models >> test_dbt_models


elt_pagila_sakila_pipeline()