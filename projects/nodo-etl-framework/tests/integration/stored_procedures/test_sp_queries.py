"""Integration tests: Query stored procedures.

Tests for sp_get_scheduled_processes, sp_get_jobs_to_execute,
sp_get_datasets_to_execute, and sp_get_execution_summary.

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


@skip_integration
class TestGetScheduledProcesses:
    """Tests for sp_get_scheduled_processes."""

    def test_returns_process_with_active_schedule(self, db_connection):
        """A process with an enabled schedule should be returned."""
        proc_repo = ProcessRepository(db_connection)
        exec_repo = ExecutionRepository(db_connection)

        process = proc_repo.create({
            "process_name": "test_sched_active",
            "execution_order": 1,
        })
        proc_repo.create_schedule({
            "process_id": process["id"],
            "schedule_name": "daily_test",
            "cron_expression": "0 6 * * *",
        })

        scheduled = exec_repo.get_scheduled_processes()
        process_ids = [row["process_id"] for row in scheduled]
        assert process["id"] in process_ids

    def test_skips_disabled_processes(self, db_connection):
        """A disabled process should not appear in scheduled processes."""
        proc_repo = ProcessRepository(db_connection)
        exec_repo = ExecutionRepository(db_connection)

        process = proc_repo.create({
            "process_name": "test_sched_disabled",
            "execution_order": 1,
            "is_enabled": False,
        })
        proc_repo.create_schedule({
            "process_id": process["id"],
            "schedule_name": "daily_disabled",
            "cron_expression": "0 6 * * *",
        })

        scheduled = exec_repo.get_scheduled_processes()
        process_ids = [row["process_id"] for row in scheduled]
        assert process["id"] not in process_ids


@skip_integration
class TestGetJobsToExecute:
    """Tests for sp_get_jobs_to_execute."""

    def _create_process_with_jobs(self, db_connection, suffix, *, num_jobs=3):
        """Helper to create a process with multiple jobs at different execution orders."""
        conn_repo = ConnectionRepository(db_connection)
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)
        exec_repo = ExecutionRepository(db_connection)

        connection = conn_repo.create({
            "connection_name": f"test_conn_qj_{suffix}",
            "connection_type": "postgresql",
            "host": "test",
            "port": 5432,
            "environment": "dev",
        })

        process = proc_repo.create({
            "process_name": f"test_proc_qj_{suffix}",
            "execution_order": 1,
            "max_parallelism": 10,
        })

        jobs = []
        for i in range(num_jobs):
            job = job_repo.create({
                "process_id": process["id"],
                "job_name": f"test_job_qj_{suffix}_{i}",
                "execution_order": i + 1,
                "max_parallelism": 5,
            })
            jobs.append(job)
            ds_repo.create(
                {
                    "job_id": job["id"],
                    "dataset_name": f"test_ds_qj_{suffix}_{i}",
                    "source_type": "database",
                    "source_connection_id": connection["id"],
                    "layer": "bronze",
                    "load_strategy": "full",
                    "idempotency_strategy": "overwrite",
                    "execution_order": 1,
                },
                source_config={
                    "source_schema": "test",
                    "source_table": f"test_qj_{suffix}_{i}",
                    "target_schema": "bronze",
                    "target_table": f"test_qj_{suffix}_{i}",
                },
            )

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

        return exec_repo, exec_id, jobs

    def test_returns_jobs_ordered_by_execution_order(self, db_connection):
        """Jobs should be returned respecting execution_order (lowest first)."""
        exec_repo, exec_id, jobs = self._create_process_with_jobs(
            db_connection, "job_order",
        )

        pending_jobs = exec_repo.get_jobs_to_execute(exec_id)
        # Should only return jobs at the minimum execution_order (order=1)
        assert len(pending_jobs) >= 1
        assert pending_jobs[0]["execution_order"] == 1

    def test_returns_only_pending_jobs(self, db_connection):
        """Only pending jobs should be returned, not running or completed ones."""
        exec_repo, exec_id, jobs = self._create_process_with_jobs(
            db_connection, "job_pending",
        )

        # Get first batch and start them
        first_batch = exec_repo.get_jobs_to_execute(exec_id)
        for je in first_batch:
            exec_repo.start_job_execution(je["job_execution_id"])

        # Now get jobs again -- should not include the running ones
        next_batch = exec_repo.get_jobs_to_execute(exec_id)
        running_ids = {je["job_execution_id"] for je in first_batch}
        for je in next_batch:
            assert je["job_execution_id"] not in running_ids


@skip_integration
class TestGetDatasetsToExecute:
    """Tests for sp_get_datasets_to_execute."""

    def _create_job_with_datasets(
        self,
        db_connection,
        suffix,
        *,
        num_datasets=3,
        max_parallelism=10,
        execution_orders=None,
    ):
        """Helper to create a job with multiple datasets.

        Args:
            execution_orders: List of execution_order values for each dataset.
                              If None, all datasets get order=1.
        """
        conn_repo = ConnectionRepository(db_connection)
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)
        exec_repo = ExecutionRepository(db_connection)

        connection = conn_repo.create({
            "connection_name": f"test_conn_qd_{suffix}",
            "connection_type": "postgresql",
            "host": "test",
            "port": 5432,
            "environment": "dev",
        })

        process = proc_repo.create({
            "process_name": f"test_proc_qd_{suffix}",
            "execution_order": 1,
            "max_parallelism": 10,
        })

        job = job_repo.create({
            "process_id": process["id"],
            "job_name": f"test_job_qd_{suffix}",
            "execution_order": 1,
            "max_parallelism": max_parallelism,
        })

        if execution_orders is None:
            execution_orders = [1] * num_datasets

        datasets = []
        for i in range(num_datasets):
            ds = ds_repo.create(
                {
                    "job_id": job["id"],
                    "dataset_name": f"test_ds_qd_{suffix}_{i}",
                    "source_type": "database",
                    "source_connection_id": connection["id"],
                    "layer": "bronze",
                    "load_strategy": "full",
                    "idempotency_strategy": "overwrite",
                    "execution_order": execution_orders[i],
                },
                source_config={
                    "source_schema": "test",
                    "source_table": f"test_qd_{suffix}_{i}",
                    "target_schema": "bronze",
                    "target_table": f"test_qd_{suffix}_{i}",
                },
            )
            datasets.append(ds)

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

        job_execs = exec_repo.get_jobs_to_execute(exec_id)
        job_exec = job_execs[0]
        exec_repo.start_job_execution(job_exec["job_execution_id"])

        return exec_repo, job_exec["job_execution_id"], datasets

    def test_returns_datasets_ordered_by_execution_order(self, db_connection):
        """Datasets should respect execution_order (lowest pending first)."""
        exec_repo, job_exec_id, datasets = self._create_job_with_datasets(
            db_connection, "ds_order",
            num_datasets=3,
            execution_orders=[1, 2, 3],
        )

        ds_execs = exec_repo.get_datasets_to_execute(job_exec_id)
        # Only datasets at the minimum execution_order should be returned
        assert len(ds_execs) >= 1
        assert ds_execs[0]["execution_order"] == 1

    def test_returns_only_pending_datasets(self, db_connection):
        """Only pending datasets should be returned."""
        exec_repo, job_exec_id, datasets = self._create_job_with_datasets(
            db_connection, "ds_pending",
            num_datasets=3,
            execution_orders=[1, 1, 1],
        )

        # Get first batch and start them
        first_batch = exec_repo.get_datasets_to_execute(job_exec_id)
        for de in first_batch:
            exec_repo.start_dataset_execution(de["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=de["dataset_execution_id"],
                status="success",
                rows_read=10,
                rows_written=10,
            )

        # Now there should be no more pending datasets
        next_batch = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(next_batch) == 0

    def test_respects_max_parallelism(self, db_connection):
        """sp_get_datasets_to_execute should respect max_parallelism limit."""
        exec_repo, job_exec_id, datasets = self._create_job_with_datasets(
            db_connection, "ds_parallel",
            num_datasets=5,
            max_parallelism=2,
            execution_orders=[1, 1, 1, 1, 1],
        )

        ds_execs = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(ds_execs) == 2


@skip_integration
class TestGetExecutionSummary:
    """Tests for sp_get_execution_summary."""

    def _run_full_execution(self, db_connection, suffix):
        """Run a complete execution and return (exec_repo, process_id, exec_id)."""
        conn_repo = ConnectionRepository(db_connection)
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)
        exec_repo = ExecutionRepository(db_connection)

        connection = conn_repo.create({
            "connection_name": f"test_conn_summary_{suffix}",
            "connection_type": "postgresql",
            "host": "test",
            "port": 5432,
            "environment": "dev",
        })

        process = proc_repo.create({
            "process_name": f"test_proc_summary_{suffix}",
            "execution_order": 1,
            "max_parallelism": 10,
        })

        job = job_repo.create({
            "process_id": process["id"],
            "job_name": f"test_job_summary_{suffix}",
            "execution_order": 1,
            "max_parallelism": 10,
        })

        ds_repo.create(
            {
                "job_id": job["id"],
                "dataset_name": f"test_ds_summary_{suffix}",
                "source_type": "database",
                "source_connection_id": connection["id"],
                "layer": "bronze",
                "load_strategy": "full",
                "idempotency_strategy": "overwrite",
                "execution_order": 1,
            },
            source_config={
                "source_schema": "test",
                "source_table": f"test_summary_{suffix}",
                "target_schema": "bronze",
                "target_table": f"test_summary_{suffix}",
            },
        )

        exec_id = exec_repo.start_process_execution(
            process_id=process["id"],
            environment="dev",
            triggered_by="manual",
        )

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

        exec_repo.complete_process_execution(exec_id)

        return exec_repo, process["id"], exec_id

    def test_returns_all_fields_joined(self, db_connection):
        """Execution summary should include process, job, and dataset fields."""
        exec_repo, process_id, exec_id = self._run_full_execution(
            db_connection, "summary_fields",
        )

        summary = exec_repo.get_execution_summary(process_id=process_id)
        assert len(summary) > 0

        row = summary[0]
        # Process-level fields
        assert "process_execution_id" in row
        assert "process_name" in row
        assert "process_status" in row
        assert "process_start_time" in row
        assert "process_end_time" in row
        assert "process_environment" in row

        # Job-level fields
        assert "job_execution_id" in row
        assert "job_name" in row
        assert "job_status" in row

        # Dataset-level fields
        assert "dataset_execution_id" in row
        assert "dataset_name" in row
        assert "dataset_status" in row
        assert "dataset_rows_read" in row
        assert "dataset_rows_written" in row

    def test_filter_by_process_id(self, db_connection):
        """Filtering by process_id should only return that process's executions."""
        exec_repo, process_id, exec_id = self._run_full_execution(
            db_connection, "summary_filter",
        )

        summary = exec_repo.get_execution_summary(process_id=process_id)
        assert len(summary) > 0

        for row in summary:
            # All rows should belong to the same process_execution_id
            assert row["process_execution_id"] == exec_id
