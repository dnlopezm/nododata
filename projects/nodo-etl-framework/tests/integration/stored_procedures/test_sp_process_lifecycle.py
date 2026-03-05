"""Integration tests: sp_start_process_execution and sp_complete_process_execution.

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


def _create_connection(db_connection, suffix: str) -> dict:
    """Create a unique connection for test isolation."""
    conn_repo = ConnectionRepository(db_connection)
    return conn_repo.create({
        "connection_name": f"test_conn_proc_{suffix}",
        "connection_type": "postgresql",
        "host": "test",
        "port": 5432,
        "environment": "dev",
    })


def _create_process_with_jobs_and_datasets(
    db_connection,
    suffix: str,
    *,
    num_jobs: int = 2,
    datasets_per_job: int = 1,
    disabled_jobs: list[int] | None = None,
    disabled_datasets: list[tuple[int, int]] | None = None,
):
    """Create a full process hierarchy for testing.

    Args:
        suffix: Unique suffix to avoid name collisions.
        num_jobs: Number of jobs to create.
        datasets_per_job: Datasets per job.
        disabled_jobs: List of job indices (0-based) to disable.
        disabled_datasets: List of (job_index, dataset_index) tuples to disable.

    Returns:
        Tuple of (process, jobs, datasets_by_job, connection).
    """
    disabled_jobs = disabled_jobs or []
    disabled_datasets = disabled_datasets or []

    connection = _create_connection(db_connection, suffix)

    proc_repo = ProcessRepository(db_connection)
    job_repo = JobRepository(db_connection)
    ds_repo = DatasetRepository(db_connection)

    process = proc_repo.create({
        "process_name": f"test_proc_{suffix}",
        "execution_order": 1,
        "max_parallelism": 5,
    })

    jobs = []
    datasets_by_job = {}
    for j_idx in range(num_jobs):
        job = job_repo.create({
            "process_id": process["id"],
            "job_name": f"test_job_{suffix}_{j_idx}",
            "execution_order": j_idx + 1,
            "max_parallelism": 5,
            "is_enabled": j_idx not in disabled_jobs,
        })
        jobs.append(job)

        ds_list = []
        for d_idx in range(datasets_per_job):
            ds = ds_repo.create(
                {
                    "job_id": job["id"],
                    "dataset_name": f"test_ds_{suffix}_{j_idx}_{d_idx}",
                    "source_type": "database",
                    "source_connection_id": connection["id"],
                    "layer": "bronze",
                    "load_strategy": "full",
                    "idempotency_strategy": "overwrite",
                    "execution_order": d_idx + 1,
                    "is_enabled": (j_idx, d_idx) not in disabled_datasets,
                },
                source_config={
                    "source_schema": "test",
                    "source_table": f"test_table_{suffix}_{j_idx}_{d_idx}",
                    "target_schema": "bronze",
                    "target_table": f"test_table_{suffix}_{j_idx}_{d_idx}",
                },
            )
            ds_list.append(ds)
        datasets_by_job[job["id"]] = ds_list

    return process, jobs, datasets_by_job, connection


@skip_integration
class TestStartProcessExecution:
    """Tests for sp_start_process_execution."""

    def test_start_creates_process_execution_running(self, db_connection):
        """Starting a process creates a process_execution with status=running."""
        process, jobs, _, _ = _create_process_with_jobs_and_datasets(
            db_connection, "start_running",
        )
        exec_repo = ExecutionRepository(db_connection)

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_process_execution "
            f"WHERE id = :id",
            {"id": exec_id},
        )
        assert len(rows) == 1
        assert rows[0]["status"] == "running"
        assert rows[0]["start_time"] is not None

    def test_start_creates_job_execution_records_for_enabled_jobs(self, db_connection):
        """Starting a process creates job_execution records for enabled jobs."""
        process, jobs, _, _ = _create_process_with_jobs_and_datasets(
            db_connection, "start_jobs", num_jobs=3,
        )
        exec_repo = ExecutionRepository(db_connection)

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

        job_execs = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_job_execution "
            f"WHERE process_execution_id = :exec_id ORDER BY id",
            {"exec_id": exec_id},
        )
        assert len(job_execs) == 3
        for je in job_execs:
            assert je["status"] == "pending"

    def test_start_creates_dataset_execution_records_for_enabled_datasets(
        self, db_connection,
    ):
        """Starting a process creates dataset_execution records for enabled datasets."""
        process, jobs, datasets_by_job, _ = _create_process_with_jobs_and_datasets(
            db_connection, "start_datasets", num_jobs=2, datasets_per_job=2,
        )
        exec_repo = ExecutionRepository(db_connection)

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

        ds_execs = db_connection.execute(
            f"SELECT de.* FROM {db_connection.schema_name}.etl_dataset_execution de "
            f"JOIN {db_connection.schema_name}.etl_job_execution je "
            f"  ON de.job_execution_id = je.id "
            f"WHERE je.process_execution_id = :exec_id",
            {"exec_id": exec_id},
        )
        # 2 jobs x 2 datasets = 4
        assert len(ds_execs) == 4
        for de in ds_execs:
            assert de["status"] == "pending"

    def test_start_skips_disabled_jobs(self, db_connection):
        """Disabled jobs should not get job_execution records."""
        process, jobs, _, _ = _create_process_with_jobs_and_datasets(
            db_connection, "skip_disabled_job",
            num_jobs=3, disabled_jobs=[1],
        )
        exec_repo = ExecutionRepository(db_connection)

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

        job_execs = db_connection.execute(
            f"SELECT je.*, j.job_name "
            f"FROM {db_connection.schema_name}.etl_job_execution je "
            f"JOIN {db_connection.schema_name}.etl_job j ON je.job_id = j.id "
            f"WHERE je.process_execution_id = :exec_id",
            {"exec_id": exec_id},
        )
        # Only 2 out of 3 jobs should have execution records
        assert len(job_execs) == 2
        job_ids_in_exec = {je["job_id"] for je in job_execs}
        disabled_job_id = jobs[1]["id"]
        assert disabled_job_id not in job_ids_in_exec

    def test_start_returns_valid_process_execution_id(self, db_connection):
        """start_process_execution returns a valid positive integer ID."""
        process, _, _, _ = _create_process_with_jobs_and_datasets(
            db_connection, "valid_id",
        )
        exec_repo = ExecutionRepository(db_connection)

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

        assert isinstance(exec_id, int)
        assert exec_id > 0

    def test_start_with_nonexistent_process_raises_error(self, db_connection):
        """Starting execution for a non-existent process should raise an error."""
        exec_repo = ExecutionRepository(db_connection)

        with pytest.raises(Exception):
            exec_repo.start_process_execution(
                process_id=999999,
                environment="dev",
                triggered_by="manual",
            )


@skip_integration
class TestCompleteProcessExecution:
    """Tests for sp_complete_process_execution."""

    def test_complete_all_jobs_success(self, db_connection):
        """When all jobs succeed, process status should be success."""
        process, jobs, datasets_by_job, _ = _create_process_with_jobs_and_datasets(
            db_connection, "complete_success", num_jobs=2, datasets_per_job=1,
        )
        exec_repo = ExecutionRepository(db_connection)

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

        # Execute all jobs and datasets successfully
        job_execs = exec_repo.get_jobs_to_execute(exec_id)
        for je in job_execs:
            exec_repo.start_job_execution(je["job_execution_id"])
            ds_execs = exec_repo.get_datasets_to_execute(je["job_execution_id"])
            for de in ds_execs:
                exec_repo.start_dataset_execution(de["dataset_execution_id"])
                exec_repo.complete_dataset_execution(
                    dataset_execution_id=de["dataset_execution_id"],
                    status="success",
                    rows_read=100,
                    rows_written=100,
                )
            exec_repo.complete_job_execution(je["job_execution_id"])

        # Get remaining pending jobs (second execution_order)
        remaining_jobs = exec_repo.get_jobs_to_execute(exec_id)
        for je in remaining_jobs:
            exec_repo.start_job_execution(je["job_execution_id"])
            ds_execs = exec_repo.get_datasets_to_execute(je["job_execution_id"])
            for de in ds_execs:
                exec_repo.start_dataset_execution(de["dataset_execution_id"])
                exec_repo.complete_dataset_execution(
                    dataset_execution_id=de["dataset_execution_id"],
                    status="success",
                    rows_read=50,
                    rows_written=50,
                )
            exec_repo.complete_job_execution(je["job_execution_id"])

        exec_repo.complete_process_execution(exec_id)

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_process_execution "
            f"WHERE id = :id",
            {"id": exec_id},
        )
        assert rows[0]["status"] == "success"

    def test_complete_one_job_failed(self, db_connection):
        """When one job fails, process status should be failed."""
        process, jobs, datasets_by_job, _ = _create_process_with_jobs_and_datasets(
            db_connection, "complete_failed", num_jobs=2, datasets_per_job=1,
        )
        exec_repo = ExecutionRepository(db_connection)

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

        # Execute first job successfully
        job_execs = exec_repo.get_jobs_to_execute(exec_id)
        je = job_execs[0]
        exec_repo.start_job_execution(je["job_execution_id"])
        ds_execs = exec_repo.get_datasets_to_execute(je["job_execution_id"])
        for de in ds_execs:
            exec_repo.start_dataset_execution(de["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=de["dataset_execution_id"],
                status="success",
                rows_read=100,
                rows_written=100,
            )
        exec_repo.complete_job_execution(je["job_execution_id"])

        # Execute second job with a failed dataset
        remaining_jobs = exec_repo.get_jobs_to_execute(exec_id)
        if remaining_jobs:
            je2 = remaining_jobs[0]
            exec_repo.start_job_execution(je2["job_execution_id"])
            ds_execs2 = exec_repo.get_datasets_to_execute(je2["job_execution_id"])
            for de in ds_execs2:
                exec_repo.start_dataset_execution(de["dataset_execution_id"])
                exec_repo.complete_dataset_execution(
                    dataset_execution_id=de["dataset_execution_id"],
                    status="failed",
                    error_message="Simulated failure",
                )
            exec_repo.complete_job_execution(je2["job_execution_id"])

        exec_repo.complete_process_execution(exec_id)

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_process_execution "
            f"WHERE id = :id",
            {"id": exec_id},
        )
        assert rows[0]["status"] == "failed"

    def test_complete_sets_end_time(self, db_connection):
        """Completing a process should set end_time."""
        process, _, _, _ = _create_process_with_jobs_and_datasets(
            db_connection, "complete_endtime", num_jobs=1, datasets_per_job=1,
        )
        exec_repo = ExecutionRepository(db_connection)

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

        # Execute the single job/dataset
        job_execs = exec_repo.get_jobs_to_execute(exec_id)
        je = job_execs[0]
        exec_repo.start_job_execution(je["job_execution_id"])
        ds_execs = exec_repo.get_datasets_to_execute(je["job_execution_id"])
        for de in ds_execs:
            exec_repo.start_dataset_execution(de["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=de["dataset_execution_id"],
                status="success",
                rows_read=10,
                rows_written=10,
            )
        exec_repo.complete_job_execution(je["job_execution_id"])
        exec_repo.complete_process_execution(exec_id)

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_process_execution "
            f"WHERE id = :id",
            {"id": exec_id},
        )
        assert rows[0]["end_time"] is not None
