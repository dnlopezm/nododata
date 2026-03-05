"""Nodo ETL Single Entity Executor DAG.

Supports running a single process, job, or dataset.
Triggered manually via Airflow UI or CLI.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nodo_etl_helpers import (
    execute_hooks,
    get_execution_repo,
    get_metadata_connection,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "nodo-etl",
    "depends_on_past": False,
    "retries": 0,
}


def execute_single_entity(**context):
    """Execute a single process, job, or dataset."""
    params = context.get("params", {})
    entity_type = params.get("entity_type", "process")
    entity_id = params.get("entity_id")
    environment = params.get("environment", "dev")

    if not entity_id:
        raise ValueError("entity_id is required")

    conn = get_metadata_connection()
    repo = get_execution_repo(conn)

    try:
        if entity_type == "process":
            exec_id = repo.start_process_execution(
                process_id=entity_id,
                environment=environment,
                triggered_by="manual",
            )
            logger.info(f"Started process execution id={exec_id}")

            # Import the process executor logic
            from nodo_etl_process_executor import _execute_jobs
            execute_hooks(conn, "process", entity_id, "pre")
            _execute_jobs(conn, repo, exec_id, entity_id)
            repo.complete_process_execution(exec_id)
            execute_hooks(conn, "process", entity_id, "post", status="success")
            logger.info(f"Completed process execution id={exec_id}")

        elif entity_type == "job":
            # Start the job execution directly
            repo.start_job_execution(entity_id)
            execute_hooks(conn, "job", entity_id, "pre")

            from nodo_etl_process_executor import _execute_datasets
            _execute_datasets(conn, repo, entity_id, entity_id)

            retryable = repo.retry_failed_datasets(entity_id)
            while retryable:
                _execute_datasets(conn, repo, entity_id, entity_id)
                retryable = repo.retry_failed_datasets(entity_id)

            repo.complete_job_execution(entity_id)
            execute_hooks(conn, "job", entity_id, "post", status="success")
            logger.info(f"Completed job execution id={entity_id}")

        elif entity_type == "dataset":
            # Start the dataset execution directly
            repo.start_dataset_execution(entity_id)
            execute_hooks(conn, "dataset", entity_id, "pre")

            try:
                logger.info(f"[DUMMY] Processing dataset execution id={entity_id}")
                repo.complete_dataset_execution(
                    dataset_execution_id=entity_id,
                    status="success",
                    rows_read=0,
                    rows_written=0,
                    rows_errored=0,
                )
                execute_hooks(conn, "dataset", entity_id, "post", status="success")
            except Exception as e:
                repo.complete_dataset_execution(
                    dataset_execution_id=entity_id,
                    status="failed",
                    error_message=str(e),
                )
                execute_hooks(conn, "dataset", entity_id, "post", status="failed")
                raise

            logger.info(f"Completed dataset execution id={entity_id}")

        else:
            raise ValueError(f"Unknown entity_type: {entity_type}")

    finally:
        conn.close()


with DAG(
    dag_id="nodo_etl_single_executor",
    default_args=default_args,
    description="Execute a single process, job, or dataset",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=10,
    tags=["nodo-etl", "executor", "manual"],
    params={
        "entity_type": "process",
        "entity_id": None,
        "environment": "dev",
    },
) as dag:

    execute = PythonOperator(
        task_id="execute_single_entity",
        python_callable=execute_single_entity,
    )
