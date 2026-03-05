"""Integration tests: sp_start_job_execution and sp_complete_job_execution.

Requires Docker services. Enable with NODO_ETL_INTEGRATION_TESTS=1.
"""

import pytest

from tests.integration.conftest import skip_integration

from nodo_etl.db.repositories import (
    ConnectionRepository,
    DatasetRepository,
    ExecutionRepository,
    JobRepository,
    ProcessRepository,
)


def _setup_execution(db_connection, suffix: str, *, num_datasets: int = 2):
    """Create process, job, datasets, and start a process execution.

    Returns:
        Tuple of (exec_repo, exec_id, job_exec, dataset_execs).
    """
    conn_repo = ConnectionRepository(db_connection)
    proc_repo = ProcessRepository(db_connection)
    job_repo = JobRepository(db_connection)
    ds_repo = DatasetRepository(db_connection)
    exec_repo = ExecutionRepository(db_connection)

    connection = conn_repo.create({
        "connection_name": f"test_conn_job_{suffix}",
        "connection_type": "postgresql",
        "host": "test",
        "port": 5432,
        "environment": "dev",
    })

    process = proc_repo.create({
        "process_name": f"test_proc_job_{suffix}",
        "execution_order": 1,
        "max_parallelism": 10,
    })

    job = job_repo.create({
        "process_id": process["id"],
        "job_name": f"test_job_{suffix}",
        "execution_order": 1,
        "max_parallelism": 10,
    })

    for i in range(num_datasets):
        ds_repo.create(
            {
                "job_id": job["id"],
                "dataset_name": f"test_ds_job_{suffix}_{i}",
                "source_type": "database",
                "source_connection_id": connection["id"],
                "layer": "bronze",
                "load_strategy": "full",
                "idempotency_strategy": "overwrite",
                "execution_order": 1,
            },
            source_config={
                "source_schema": "test",
                "source_table": f"test_table_job_{suffix}_{i}",
                "target_schema": "bronze",
                "target_table": f"test_table_job_{suffix}_{i}",
            },
        )

    exec_id = exec_repo.start_process_execution(
        process_id=process["id"],
        environment="dev",
        triggered_by="manual",
    )

    job_execs = exec_repo.get_jobs_to_execute(exec_id)
    job_exec = job_execs[0]

    dataset_execs = exec_repo.get_datasets_to_execute(job_exec["job_execution_id"])

    return exec_repo, exec_id, job_exec, dataset_execs


@skip_integration
class TestStartJobExecution:
    """Tests for sp_start_job_execution."""

    def test_start_updates_status_to_running(self, db_connection):
        """Starting a job execution sets status to running and start_time."""
        exec_repo, exec_id, job_exec, _ = _setup_execution(
            db_connection, "start_running",
        )

        exec_repo.start_job_execution(job_exec["job_execution_id"])

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_job_execution "
            f"WHERE id = :id",
            {"id": job_exec["job_execution_id"]},
        )
        assert rows[0]["status"] == "running"
        assert rows[0]["start_time"] is not None

    def test_start_does_not_affect_other_job_executions(self, db_connection):
        """Starting one job execution should not change other job executions."""
        conn_repo = ConnectionRepository(db_connection)
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)
        exec_repo = ExecutionRepository(db_connection)

        connection = conn_repo.create({
            "connection_name": "test_conn_job_no_affect",
            "connection_type": "postgresql",
            "host": "test",
            "port": 5432,
            "environment": "dev",
        })

        process = proc_repo.create({
            "process_name": "test_proc_job_no_affect",
            "execution_order": 1,
            "max_parallelism": 10,
        })

        # Create two jobs at same execution_order so both get returned
        job1 = job_repo.create({
            "process_id": process["id"],
            "job_name": "test_job_no_affect_1",
            "execution_order": 1,
            "max_parallelism": 5,
        })
        job2 = job_repo.create({
            "process_id": process["id"],
            "job_name": "test_job_no_affect_2",
            "execution_order": 1,
            "max_parallelism": 5,
        })

        for j_id, j_name in [(job1["id"], "j1"), (job2["id"], "j2")]:
            ds_repo.create(
                {
                    "job_id": j_id,
                    "dataset_name": f"test_ds_no_affect_{j_name}",
                    "source_type": "database",
                    "source_connection_id": connection["id"],
                    "layer": "bronze",
                    "load_strategy": "full",
                    "idempotency_strategy": "overwrite",
                    "execution_order": 1,
                },
                source_config={
                    "source_schema": "test",
                    "source_table": f"test_no_affect_{j_name}",
                    "target_schema": "bronze",
                    "target_table": f"test_no_affect_{j_name}",
                },
            )

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

        job_execs = exec_repo.get_jobs_to_execute(exec_id)
        assert len(job_execs) == 2

        # Start only the first job
        exec_repo.start_job_execution(job_execs[0]["job_execution_id"])

        # Verify second job is still pending
        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_job_execution "
            f"WHERE id = :id",
            {"id": job_execs[1]["job_execution_id"]},
        )
        assert rows[0]["status"] == "pending"
        assert rows[0]["start_time"] is None


@skip_integration
class TestCompleteJobExecution:
    """Tests for sp_complete_job_execution."""

    def test_complete_all_datasets_success(self, db_connection):
        """When all datasets succeed, job status should be success."""
        exec_repo, exec_id, job_exec, ds_execs = _setup_execution(
            db_connection, "job_all_success", num_datasets=3,
        )

        exec_repo.start_job_execution(job_exec["job_execution_id"])
        for de in ds_execs:
            exec_repo.start_dataset_execution(de["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=de["dataset_execution_id"],
                status="success",
                rows_read=100,
                rows_written=100,
            )
        exec_repo.complete_job_execution(job_exec["job_execution_id"])

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_job_execution "
            f"WHERE id = :id",
            {"id": job_exec["job_execution_id"]},
        )
        assert rows[0]["status"] == "success"

    def test_complete_one_dataset_failed(self, db_connection):
        """When one dataset fails, job status should be failed."""
        exec_repo, exec_id, job_exec, ds_execs = _setup_execution(
            db_connection, "job_one_fail", num_datasets=3,
        )

        exec_repo.start_job_execution(job_exec["job_execution_id"])

        # First two succeed
        for de in ds_execs[:2]:
            exec_repo.start_dataset_execution(de["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=de["dataset_execution_id"],
                status="success",
                rows_read=100,
                rows_written=100,
            )

        # Third fails
        exec_repo.start_dataset_execution(ds_execs[2]["dataset_execution_id"])
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_execs[2]["dataset_execution_id"],
            status="failed",
            error_message="Test failure",
        )

        exec_repo.complete_job_execution(job_exec["job_execution_id"])

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_job_execution "
            f"WHERE id = :id",
            {"id": job_exec["job_execution_id"]},
        )
        assert rows[0]["status"] == "failed"

    def test_complete_sets_end_time(self, db_connection):
        """Completing a job should set end_time."""
        exec_repo, exec_id, job_exec, ds_execs = _setup_execution(
            db_connection, "job_endtime", num_datasets=1,
        )

        exec_repo.start_job_execution(job_exec["job_execution_id"])
        for de in ds_execs:
            exec_repo.start_dataset_execution(de["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=de["dataset_execution_id"],
                status="success",
                rows_read=10,
                rows_written=10,
            )
        exec_repo.complete_job_execution(job_exec["job_execution_id"])

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_job_execution "
            f"WHERE id = :id",
            {"id": job_exec["job_execution_id"]},
        )
        assert rows[0]["end_time"] is not None

    def test_complete_computes_dataset_totals(self, db_connection):
        """Completing a job should compute total/completed/failed datasets."""
        exec_repo, exec_id, job_exec, ds_execs = _setup_execution(
            db_connection, "job_totals", num_datasets=3,
        )

        exec_repo.start_job_execution(job_exec["job_execution_id"])

        # Two succeed, one fails
        for de in ds_execs[:2]:
            exec_repo.start_dataset_execution(de["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=de["dataset_execution_id"],
                status="success",
                rows_read=50,
                rows_written=50,
            )

        exec_repo.start_dataset_execution(ds_execs[2]["dataset_execution_id"])
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_execs[2]["dataset_execution_id"],
            status="failed",
            error_message="Simulated error",
        )

        exec_repo.complete_job_execution(job_exec["job_execution_id"])

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_job_execution "
            f"WHERE id = :id",
            {"id": job_exec["job_execution_id"]},
        )
        assert rows[0]["total_datasets"] == 3
        assert rows[0]["completed_datasets"] == 2
        assert rows[0]["failed_datasets"] == 1
