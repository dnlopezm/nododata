"""Integration tests: sp_retry_failed_datasets.

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
    SystemConfigRepository,
)


def _setup_retry_scenario(
    db_connection,
    suffix: str,
    *,
    num_datasets: int = 2,
    max_retries: int | None = 3,
    dataset_max_retries: int | None = None,
):
    """Create process/job/datasets and start execution for retry testing.

    Args:
        max_retries: Process-level max_retries.
        dataset_max_retries: Dataset-level max_retries (overrides process level).

    Returns:
        Tuple of (exec_repo, job_exec_id, dataset_exec_ids, db_connection).
    """
    conn_repo = ConnectionRepository(db_connection)
    proc_repo = ProcessRepository(db_connection)
    job_repo = JobRepository(db_connection)
    ds_repo = DatasetRepository(db_connection)
    exec_repo = ExecutionRepository(db_connection)

    connection = conn_repo.create({
        "connection_name": f"test_conn_retry_{suffix}",
        "connection_type": "postgresql",
        "host": "test",
        "port": 5432,
        "environment": "dev",
    })

    process_data = {
        "process_name": f"test_proc_retry_{suffix}",
        "execution_order": 1,
        "max_parallelism": 10,
    }
    if max_retries is not None:
        process_data["max_retries"] = max_retries

    process = proc_repo.create(process_data)

    job = job_repo.create({
        "process_id": process["id"],
        "job_name": f"test_job_retry_{suffix}",
        "execution_order": 1,
        "max_parallelism": 10,
    })

    for i in range(num_datasets):
        ds_data = {
            "job_id": job["id"],
            "dataset_name": f"test_ds_retry_{suffix}_{i}",
            "source_type": "database",
            "source_connection_id": connection["id"],
            "layer": "bronze",
            "load_strategy": "full",
            "idempotency_strategy": "overwrite",
            "execution_order": 1,
        }
        if dataset_max_retries is not None:
            ds_data["max_retries"] = dataset_max_retries

        ds_repo.create(
            ds_data,
            source_config={
                "source_schema": "test",
                "source_table": f"test_retry_{suffix}_{i}",
                "target_schema": "bronze",
                "target_table": f"test_retry_{suffix}_{i}",
            },
        )

    exec_id = exec_repo.start_process_execution(
        process_id=process["id"],
        environment="dev",
        triggered_by="manual",
    )

    job_execs = exec_repo.get_jobs_to_execute(exec_id)
    job_exec = job_execs[0]
    exec_repo.start_job_execution(job_exec["job_execution_id"])

    ds_execs = exec_repo.get_datasets_to_execute(job_exec["job_execution_id"])
    ds_exec_ids = [de["dataset_execution_id"] for de in ds_execs]

    return exec_repo, job_exec["job_execution_id"], ds_exec_ids


@skip_integration
class TestRetryFailedDatasets:
    """Tests for sp_retry_failed_datasets."""

    def test_finds_failed_datasets_under_max_retries(self, db_connection):
        """Failed datasets with retry_count < max_retries should be found."""
        exec_repo, job_exec_id, ds_exec_ids = _setup_retry_scenario(
            db_connection, "retry_find", num_datasets=2, max_retries=3,
        )

        # Fail both datasets
        for ds_id in ds_exec_ids:
            exec_repo.start_dataset_execution(ds_id)
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds_id,
                status="failed",
                error_message="Test error",
            )

        retryable = exec_repo.retry_failed_datasets(job_exec_id)
        assert len(retryable) == 2
        for r in retryable:
            assert r["retry_count"] >= 1

    def test_resets_status_to_pending_and_increments_retry_count(self, db_connection):
        """Retry should reset status to pending and increment retry_count."""
        exec_repo, job_exec_id, ds_exec_ids = _setup_retry_scenario(
            db_connection, "retry_reset", num_datasets=1, max_retries=3,
        )

        ds_exec_id = ds_exec_ids[0]

        # Fail the dataset
        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="failed",
            error_message="First failure",
        )

        # Retry
        exec_repo.retry_failed_datasets(job_exec_id)

        # Verify the dataset execution was reset
        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_dataset_execution "
            f"WHERE id = :id",
            {"id": ds_exec_id},
        )
        row = rows[0]
        assert row["status"] == "pending"
        assert row["retry_count"] == 1
        # Error message should be cleared
        assert row["error_message"] is None

    def test_skips_datasets_at_max_retries(self, db_connection):
        """Datasets that have reached max_retries should not be retried."""
        exec_repo, job_exec_id, ds_exec_ids = _setup_retry_scenario(
            db_connection, "retry_max", num_datasets=1, dataset_max_retries=1,
        )

        ds_exec_id = ds_exec_ids[0]

        # First failure
        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="failed",
            error_message="First failure",
        )

        # First retry (retry_count goes from 0 to 1, which equals max_retries=1)
        retryable_first = exec_repo.retry_failed_datasets(job_exec_id)
        assert len(retryable_first) == 1

        # Fail again after retry
        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="failed",
            error_message="Second failure",
        )

        # Second retry attempt -- should find nothing since retry_count=1 >= max_retries=1
        retryable_second = exec_repo.retry_failed_datasets(job_exec_id)
        assert len(retryable_second) == 0

    def test_no_failed_datasets_returns_empty(self, db_connection):
        """When there are no failed datasets, retry should return empty list."""
        exec_repo, job_exec_id, ds_exec_ids = _setup_retry_scenario(
            db_connection, "retry_none_failed", num_datasets=2, max_retries=3,
        )

        # Complete all datasets successfully
        for ds_id in ds_exec_ids:
            exec_repo.start_dataset_execution(ds_id)
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds_id,
                status="success",
                rows_read=100,
                rows_written=100,
            )

        retryable = exec_repo.retry_failed_datasets(job_exec_id)
        assert len(retryable) == 0

    def test_does_not_affect_successful_datasets(self, db_connection):
        """Retry should only affect failed datasets, not successful ones."""
        exec_repo, job_exec_id, ds_exec_ids = _setup_retry_scenario(
            db_connection, "retry_no_affect", num_datasets=2, max_retries=3,
        )

        # First dataset succeeds, second fails
        exec_repo.start_dataset_execution(ds_exec_ids[0])
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_ids[0],
            status="success",
            rows_read=100,
            rows_written=100,
        )

        exec_repo.start_dataset_execution(ds_exec_ids[1])
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_ids[1],
            status="failed",
            error_message="Test error",
        )

        retryable = exec_repo.retry_failed_datasets(job_exec_id)

        # Only the failed dataset should be retried
        assert len(retryable) == 1
        assert retryable[0]["dataset_execution_id"] == ds_exec_ids[1]

        # Verify successful dataset is untouched
        success_row = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_dataset_execution "
            f"WHERE id = :id",
            {"id": ds_exec_ids[0]},
        )
        assert success_row[0]["status"] == "success"
        assert success_row[0]["retry_count"] == 0
