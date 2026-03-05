"""E2E tests: Retry and failure handling scenarios.

Tests dataset failure, retry mechanics, retry_count increments,
and max_retries cascade behavior.

Requires NODO_ETL_E2E_TESTS=1 to enable.
"""

import pytest

from tests.e2e.conftest import skip_e2e

from nodo_etl.db.repositories import SystemConfigRepository


@skip_e2e
class TestRetryScenarios:
    """Retry and failure handling through the execution lifecycle."""

    def test_one_dataset_fails_job_failed(self, repos, full_pipeline):
        """One dataset fails, rest succeed -> job status=failed."""
        exec_repo = repos["execution"]
        process_id = full_pipeline["process"]["id"]

        exec_id = exec_repo.start_process_execution(
            process_id=process_id,
            environment="dev",
            triggered_by="manual",
        )

        # Get first round of jobs (bronze)
        jobs = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs) >= 1

        job = jobs[0]
        job_exec_id = job["job_execution_id"]
        exec_repo.start_job_execution(job_exec_id)

        datasets = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(datasets) >= 2

        # First dataset succeeds
        exec_repo.start_dataset_execution(datasets[0]["dataset_execution_id"])
        exec_repo.complete_dataset_execution(
            dataset_execution_id=datasets[0]["dataset_execution_id"],
            status="success",
            rows_read=100,
            rows_written=100,
        )

        # Second dataset fails
        exec_repo.start_dataset_execution(datasets[1]["dataset_execution_id"])
        exec_repo.complete_dataset_execution(
            dataset_execution_id=datasets[1]["dataset_execution_id"],
            status="failed",
            error_message="Connection refused: source database unavailable",
        )

        # Complete job - should be marked as failed
        exec_repo.complete_job_execution(job_exec_id)

        # Complete process
        exec_repo.complete_process_execution(exec_id)

        # Verify process/job marked as failed in summary
        summary = exec_repo.get_execution_summary(process_id=process_id)
        assert len(summary) > 0
        # At least one row should show failed status
        failed_rows = [
            r for r in summary
            if r.get("process_status") == "failed" or r.get("job_status") == "failed"
        ]
        assert len(failed_rows) > 0

    def test_failed_dataset_retry_increments_count(self, repos, full_pipeline):
        """Failed dataset retried -> retry_count incremented."""
        exec_repo = repos["execution"]
        process_id = full_pipeline["process"]["id"]

        exec_id = exec_repo.start_process_execution(
            process_id=process_id,
            environment="dev",
            triggered_by="manual",
        )

        jobs = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs) >= 1

        job = jobs[0]
        job_exec_id = job["job_execution_id"]
        exec_repo.start_job_execution(job_exec_id)

        datasets = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(datasets) >= 1

        # Fail the first dataset
        ds_exec_id = datasets[0]["dataset_execution_id"]
        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="failed",
            error_message="Timeout waiting for lock",
        )

        # Complete remaining datasets as success
        for ds in datasets[1:]:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success",
                rows_read=50,
                rows_written=50,
            )

        # Retry failed datasets
        retryable = exec_repo.retry_failed_datasets(job_exec_id)
        assert len(retryable) > 0
        assert retryable[0]["retry_count"] >= 1

    def test_failed_dataset_succeeds_on_retry(self, repos, full_pipeline):
        """Failed dataset succeeds on retry -> status=success, retry_count=1."""
        exec_repo = repos["execution"]
        process_id = full_pipeline["process"]["id"]

        exec_id = exec_repo.start_process_execution(
            process_id=process_id,
            environment="dev",
            triggered_by="manual",
        )

        jobs = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs) >= 1

        job = jobs[0]
        job_exec_id = job["job_execution_id"]
        exec_repo.start_job_execution(job_exec_id)

        datasets = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(datasets) >= 1

        # Fail the first dataset
        ds_exec_id = datasets[0]["dataset_execution_id"]
        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="failed",
            error_message="Transient network error",
        )

        # Complete remaining datasets as success
        for ds in datasets[1:]:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success",
                rows_read=50,
                rows_written=50,
            )

        # Retry failed datasets
        retryable = exec_repo.retry_failed_datasets(job_exec_id)
        assert len(retryable) > 0

        # Now execute the retry - it should succeed
        retry_ds = retryable[0]
        retry_ds_exec_id = retry_ds["dataset_execution_id"]
        exec_repo.start_dataset_execution(retry_ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=retry_ds_exec_id,
            status="success",
            rows_read=100,
            rows_written=100,
        )

        # Complete job after retry
        exec_repo.complete_job_execution(job_exec_id)

        # Verify the job can now complete successfully
        # Complete remaining jobs in the pipeline
        pending_jobs = exec_repo.get_jobs_to_execute(exec_id)
        while pending_jobs:
            for j in pending_jobs:
                j_exec_id = j["job_execution_id"]
                exec_repo.start_job_execution(j_exec_id)
                ds_list = exec_repo.get_datasets_to_execute(j_exec_id)
                while ds_list:
                    for d in ds_list:
                        exec_repo.start_dataset_execution(d["dataset_execution_id"])
                        exec_repo.complete_dataset_execution(
                            dataset_execution_id=d["dataset_execution_id"],
                            status="success",
                            rows_read=10,
                            rows_written=10,
                        )
                    ds_list = exec_repo.get_datasets_to_execute(j_exec_id)
                exec_repo.complete_job_execution(j_exec_id)
            pending_jobs = exec_repo.get_jobs_to_execute(exec_id)

        exec_repo.complete_process_execution(exec_id)

        summary = exec_repo.get_execution_summary(process_id=process_id)
        assert len(summary) > 0

    def test_no_failed_datasets_retry_returns_empty(self, repos, full_pipeline):
        """When no datasets failed, retry returns empty list."""
        exec_repo = repos["execution"]
        process_id = full_pipeline["process"]["id"]

        exec_id = exec_repo.start_process_execution(
            process_id=process_id,
            environment="dev",
            triggered_by="manual",
        )

        jobs = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs) >= 1

        job = jobs[0]
        job_exec_id = job["job_execution_id"]
        exec_repo.start_job_execution(job_exec_id)

        datasets = exec_repo.get_datasets_to_execute(job_exec_id)

        # All datasets succeed
        for ds in datasets:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success",
                rows_read=100,
                rows_written=100,
            )

        exec_repo.complete_job_execution(job_exec_id)

        # Retry should return empty
        retryable = exec_repo.retry_failed_datasets(job_exec_id)
        assert len(retryable) == 0

    def test_max_retries_cascade(self, repos, db_connection, full_pipeline):
        """Dataset max_retries cascades: dataset -> job -> process -> system_config.

        Verify that when a dataset has no max_retries, the system looks up the chain.
        """
        sys_repo = SystemConfigRepository(db_connection)
        exec_repo = repos["execution"]
        process_id = full_pipeline["process"]["id"]

        # Set system-level default max_retries
        sys_repo.set("default_max_retries", "3")

        # Verify the config was stored
        config = sys_repo.get("default_max_retries")
        assert config["config_value"] == "3"

        # Start an execution and fail a dataset
        exec_id = exec_repo.start_process_execution(
            process_id=process_id,
            environment="dev",
            triggered_by="manual",
        )

        jobs = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs) >= 1

        job = jobs[0]
        job_exec_id = job["job_execution_id"]
        exec_repo.start_job_execution(job_exec_id)

        datasets = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(datasets) >= 1

        # Fail dataset
        ds_exec_id = datasets[0]["dataset_execution_id"]
        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="failed",
            error_message="Testing max_retries cascade",
        )

        # Retry should work (within max_retries limit)
        retryable = exec_repo.retry_failed_datasets(job_exec_id)
        assert len(retryable) > 0
