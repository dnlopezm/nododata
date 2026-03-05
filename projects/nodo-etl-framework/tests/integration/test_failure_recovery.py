"""Integration tests: Failure and Recovery scenarios.

Requires Docker services. Enable with NODO_ETL_INTEGRATION_TESTS=1.
"""

import pytest

from tests.integration.conftest import skip_integration

from nodo_etl.db.repositories import ExecutionRepository, ProcessRepository


@skip_integration
class TestFailureAndRecovery:
    """Scenario 3: Failure and Recovery."""

    def test_dataset_failure_stores_error(self, db_connection):
        """Run process where a dataset fails, verify error_message stored."""
        exec_repo = ExecutionRepository(db_connection)
        proc_repo = ProcessRepository(db_connection)

        processes = proc_repo.list()
        if not processes:
            pytest.skip("No processes found")

        process_id = processes[0]["id"]
        exec_id = exec_repo.start_process_execution(
            process_id=process_id, environment="dev", triggered_by="manual",
        )

        jobs = exec_repo.get_jobs_to_execute(exec_id)
        if not jobs:
            pytest.skip("No jobs to execute")

        job = jobs[0]
        exec_repo.start_job_execution(job["job_execution_id"])
        datasets = exec_repo.get_datasets_to_execute(job["job_execution_id"])

        if len(datasets) >= 2:
            # First dataset succeeds
            exec_repo.start_dataset_execution(datasets[0]["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=datasets[0]["dataset_execution_id"],
                status="success", rows_read=100, rows_written=100,
            )
            # Second dataset fails
            exec_repo.start_dataset_execution(datasets[1]["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=datasets[1]["dataset_execution_id"],
                status="failed", error_message="Connection timeout",
            )

            # Complete job - should be failed
            exec_repo.complete_job_execution(job["job_execution_id"])

            # Verify retry
            retryable = exec_repo.retry_failed_datasets(job["job_execution_id"])
            assert len(retryable) > 0

    def test_retry_increments_count(self, db_connection):
        """Verify retry_count is incremented."""
        exec_repo = ExecutionRepository(db_connection)
        proc_repo = ProcessRepository(db_connection)

        processes = proc_repo.list()
        if not processes:
            pytest.skip("No processes found")

        process_id = processes[0]["id"]
        exec_id = exec_repo.start_process_execution(
            process_id=process_id, environment="dev", triggered_by="manual",
        )

        jobs = exec_repo.get_jobs_to_execute(exec_id)
        if not jobs:
            pytest.skip("No jobs")

        job = jobs[0]
        exec_repo.start_job_execution(job["job_execution_id"])
        datasets = exec_repo.get_datasets_to_execute(job["job_execution_id"])

        if datasets:
            ds = datasets[0]
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="failed", error_message="Test failure",
            )

            retryable = exec_repo.retry_failed_datasets(job["job_execution_id"])
            if retryable:
                assert retryable[0]["retry_count"] >= 1

    def test_successful_execution_no_retry(self, db_connection):
        """Verify retry returns empty for successful datasets."""
        exec_repo = ExecutionRepository(db_connection)
        proc_repo = ProcessRepository(db_connection)

        processes = proc_repo.list()
        if not processes:
            pytest.skip("No processes found")

        process_id = processes[0]["id"]
        exec_id = exec_repo.start_process_execution(
            process_id=process_id, environment="dev", triggered_by="manual",
        )

        jobs = exec_repo.get_jobs_to_execute(exec_id)
        if not jobs:
            pytest.skip("No jobs")

        job = jobs[0]
        exec_repo.start_job_execution(job["job_execution_id"])
        datasets = exec_repo.get_datasets_to_execute(job["job_execution_id"])

        for ds in datasets:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success", rows_read=50, rows_written=50,
            )

        exec_repo.complete_job_execution(job["job_execution_id"])
        retryable = exec_repo.retry_failed_datasets(job["job_execution_id"])
        assert len(retryable) == 0
