"""Nodo ETL Process Executor DAG.

Receives a process_id and environment, then orchestrates the full
process execution lifecycle: jobs in order, datasets with parallelism,
hooks, and retries.
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
    "retry_delay": timedelta(minutes=1),
}


def execute_process(**context):
    """Execute a full process lifecycle."""
    params = context.get("params", {})
    process_id = params.get("process_id")
    environment = params.get("environment", "dev")
    triggered_by = params.get("triggered_by", "manual")
    parameters = params.get("parameters")

    if not process_id:
        raise ValueError("process_id is required")

    conn = get_metadata_connection()
    repo = get_execution_repo(conn)

    try:
        # Start process execution
        process_execution_id = repo.start_process_execution(
            process_id=process_id,
            environment=environment,
            triggered_by=triggered_by,
            parameters=parameters,
        )
        logger.info(f"Started process execution id={process_execution_id}")

        # Execute pre-hooks for process
        execute_hooks(conn, "process", process_id, "pre")

        # Get and execute jobs
        _execute_jobs(conn, repo, process_execution_id, process_id)

        # Complete process execution
        repo.complete_process_execution(process_execution_id)
        logger.info(f"Completed process execution id={process_execution_id}")

        # Execute post-hooks for process
        # Get the final status
        summary = repo.get_execution_summary(process_id=process_id)
        process_status = "success"
        for row in summary:
            if row.get("process_execution_id") == process_execution_id:
                process_status = row.get("process_status", "success")
                break
        execute_hooks(conn, "process", process_id, "post", status=process_status)

        return process_execution_id
    except Exception as e:
        logger.error(f"Process execution failed: {e}")
        raise
    finally:
        conn.close()


def _execute_jobs(conn, repo, process_execution_id, process_id):
    """Execute all jobs for a process, respecting order and parallelism."""
    while True:
        jobs = repo.get_jobs_to_execute(process_execution_id)
        if not jobs:
            break

        for job in jobs:
            job_execution_id = job["job_execution_id"]
            job_id = job["job_id"]
            job_name = job["job_name"]

            logger.info(f"Starting job '{job_name}' (execution_id={job_execution_id})")
            repo.start_job_execution(job_execution_id)

            # Execute pre-hooks for job
            execute_hooks(conn, "job", job_id, "pre")

            # Execute datasets
            _execute_datasets(conn, repo, job_execution_id, job_id)

            # Handle retries
            retryable = repo.retry_failed_datasets(job_execution_id)
            while retryable:
                logger.info(f"Retrying {len(retryable)} dataset(s) for job '{job_name}'")
                _execute_datasets(conn, repo, job_execution_id, job_id)
                retryable = repo.retry_failed_datasets(job_execution_id)

            # Complete job execution
            repo.complete_job_execution(job_execution_id)
            logger.info(f"Completed job '{job_name}' (execution_id={job_execution_id})")

            # Execute post-hooks for job
            # Determine job status from the completed job
            execute_hooks(conn, "job", job_id, "post", status="success")


def _execute_datasets(conn, repo, job_execution_id, job_id):
    """Execute all pending datasets for a job, respecting order and parallelism."""
    while True:
        datasets = repo.get_datasets_to_execute(job_execution_id)
        if not datasets:
            break

        for ds in datasets:
            dataset_execution_id = ds["dataset_execution_id"]
            dataset_id = ds["dataset_id"]
            dataset_name = ds["dataset_name"]

            logger.info(f"Processing dataset '{dataset_name}' (execution_id={dataset_execution_id})")

            repo.start_dataset_execution(dataset_execution_id)

            # Execute pre-hooks for dataset
            execute_hooks(conn, "dataset", dataset_id, "pre")

            try:
                # Dummy execution for now
                logger.info(f"  [DUMMY] Processing dataset '{dataset_name}'...")
                rows_read = 0
                rows_written = 0

                # Complete dataset execution as success
                repo.complete_dataset_execution(
                    dataset_execution_id=dataset_execution_id,
                    status="success",
                    rows_read=rows_read,
                    rows_written=rows_written,
                    rows_errored=0,
                )
                logger.info(f"  Dataset '{dataset_name}' completed successfully")

                # Post-hooks
                execute_hooks(conn, "dataset", dataset_id, "post", status="success")

            except Exception as e:
                logger.error(f"  Dataset '{dataset_name}' failed: {e}")
                repo.complete_dataset_execution(
                    dataset_execution_id=dataset_execution_id,
                    status="failed",
                    error_message=str(e),
                )
                execute_hooks(conn, "dataset", dataset_id, "post", status="failed")


with DAG(
    dag_id="nodo_etl_process_executor",
    default_args=default_args,
    description="Executes a full ETL process lifecycle",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=5,
    tags=["nodo-etl", "executor"],
    params={
        "process_id": None,
        "environment": "dev",
        "triggered_by": "manual",
        "parameters": None,
    },
) as dag:

    execute = PythonOperator(
        task_id="execute_process",
        python_callable=execute_process,
    )
