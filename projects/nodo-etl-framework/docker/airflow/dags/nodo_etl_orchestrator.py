"""Nodo ETL Orchestrator DAG.

Runs on a short interval to check for scheduled processes
and trigger process execution DAG runs.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from nodo_etl_helpers import get_metadata_connection, get_execution_repo

logger = logging.getLogger(__name__)

default_args = {
    "owner": "nodo-etl",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def check_scheduled_processes(**context):
    """Check for processes with active schedules and trigger executions."""
    conn = get_metadata_connection()
    repo = get_execution_repo(conn)

    try:
        scheduled = repo.get_scheduled_processes()
        logger.info(f"Found {len(scheduled)} scheduled process(es)")

        triggered = []
        for proc in scheduled:
            process_id = proc["process_id"]
            process_name = proc["process_name"]
            schedule_name = proc["schedule_name"]
            cron_expression = proc["cron_expression"]

            logger.info(
                f"Triggering process '{process_name}' (id={process_id}) "
                f"from schedule '{schedule_name}' (cron: {cron_expression})"
            )
            triggered.append({
                "process_id": process_id,
                "process_name": process_name,
            })

        context["ti"].xcom_push(key="triggered_processes", value=triggered)
        return triggered
    finally:
        conn.close()


def trigger_process_execution(process_info, **context):
    """Trigger a process execution DAG run for each scheduled process."""
    conn = get_metadata_connection()
    repo = get_execution_repo(conn)
    environment = context.get("params", {}).get("environment", "dev")

    try:
        process_id = process_info["process_id"]
        exec_id = repo.start_process_execution(
            process_id=process_id,
            environment=environment,
            triggered_by="schedule",
        )
        logger.info(
            f"Started process execution id={exec_id} for "
            f"process '{process_info['process_name']}'"
        )
        return exec_id
    finally:
        conn.close()


with DAG(
    dag_id="nodo_etl_orchestrator",
    default_args=default_args,
    description="Checks for scheduled ETL processes and triggers execution",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nodo-etl", "scheduler"],
) as dag:

    check_schedules = PythonOperator(
        task_id="check_scheduled_processes",
        python_callable=check_scheduled_processes,
    )
