"""Integration tests: sp_start_dataset_execution and sp_complete_dataset_execution.

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


def _setup_dataset_execution(
    db_connection,
    suffix: str,
    *,
    load_strategy: str = "full",
    watermark_column: str | None = None,
):
    """Create process/job/dataset and start execution, returning dataset_execution info.

    Returns:
        Tuple of (exec_repo, dataset_exec_id, dataset_id, exec_id, job_exec_id).
    """
    conn_repo = ConnectionRepository(db_connection)
    proc_repo = ProcessRepository(db_connection)
    job_repo = JobRepository(db_connection)
    ds_repo = DatasetRepository(db_connection)
    exec_repo = ExecutionRepository(db_connection)

    connection = conn_repo.create({
        "connection_name": f"test_conn_ds_{suffix}",
        "connection_type": "postgresql",
        "host": "test",
        "port": 5432,
        "environment": "dev",
    })

    process = proc_repo.create({
        "process_name": f"test_proc_ds_{suffix}",
        "execution_order": 1,
        "max_parallelism": 10,
    })

    job = job_repo.create({
        "process_id": process["id"],
        "job_name": f"test_job_ds_{suffix}",
        "execution_order": 1,
        "max_parallelism": 10,
    })

    source_config = {
        "source_schema": "test",
        "source_table": f"test_table_ds_{suffix}",
        "target_schema": "bronze",
        "target_table": f"test_table_ds_{suffix}",
    }
    if watermark_column:
        source_config["watermark_column"] = watermark_column

    dataset = ds_repo.create(
        {
            "job_id": job["id"],
            "dataset_name": f"test_ds_{suffix}",
            "source_type": "database",
            "source_connection_id": connection["id"],
            "layer": "bronze",
            "load_strategy": load_strategy,
            "idempotency_strategy": "overwrite",
            "execution_order": 1,
        },
        source_config=source_config,
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
    ds_exec = ds_execs[0]

    return (
        exec_repo,
        ds_exec["dataset_execution_id"],
        dataset["id"],
        exec_id,
        job_exec["job_execution_id"],
    )


@skip_integration
class TestStartDatasetExecution:
    """Tests for sp_start_dataset_execution."""

    def test_start_updates_status_to_running(self, db_connection):
        """Starting a dataset execution sets status to running."""
        exec_repo, ds_exec_id, _, _, _ = _setup_dataset_execution(
            db_connection, "ds_start_running",
        )

        exec_repo.start_dataset_execution(ds_exec_id)

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_dataset_execution "
            f"WHERE id = :id",
            {"id": ds_exec_id},
        )
        assert rows[0]["status"] == "running"
        assert rows[0]["start_time"] is not None


@skip_integration
class TestCompleteDatasetExecution:
    """Tests for sp_complete_dataset_execution."""

    def test_complete_success_updates_all_fields(self, db_connection):
        """Completing with success updates status, end_time, and row counts."""
        exec_repo, ds_exec_id, _, _, _ = _setup_dataset_execution(
            db_connection, "ds_complete_success",
        )

        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="success",
            rows_read=1000,
            rows_written=950,
            rows_errored=50,
        )

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_dataset_execution "
            f"WHERE id = :id",
            {"id": ds_exec_id},
        )
        row = rows[0]
        assert row["status"] == "success"
        assert row["end_time"] is not None
        assert row["rows_read"] == 1000
        assert row["rows_written"] == 950
        assert row["rows_errored"] == 50

    def test_complete_failed_stores_error_message(self, db_connection):
        """Completing with failed status stores the error_message."""
        exec_repo, ds_exec_id, _, _, _ = _setup_dataset_execution(
            db_connection, "ds_complete_failed",
        )

        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="failed",
            error_message="Connection timeout after 30s",
        )

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_dataset_execution "
            f"WHERE id = :id",
            {"id": ds_exec_id},
        )
        row = rows[0]
        assert row["status"] == "failed"
        assert row["error_message"] == "Connection timeout after 30s"

    def test_complete_row_counts(self, db_connection):
        """Verify rows_read, rows_written, rows_errored are stored correctly."""
        exec_repo, ds_exec_id, _, _, _ = _setup_dataset_execution(
            db_connection, "ds_row_counts",
        )

        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="success",
            rows_read=1000,
            rows_written=950,
            rows_errored=50,
        )

        rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_dataset_execution "
            f"WHERE id = :id",
            {"id": ds_exec_id},
        )
        row = rows[0]
        assert row["rows_read"] == 1000
        assert row["rows_written"] == 950
        assert row["rows_errored"] == 50

    def test_complete_success_incremental_updates_watermark(self, db_connection):
        """Success + incremental load + watermark_column should update etl_watermark."""
        exec_repo, ds_exec_id, dataset_id, _, _ = _setup_dataset_execution(
            db_connection,
            "ds_watermark_incr",
            load_strategy="incremental",
            watermark_column="updated_at",
        )

        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="success",
            rows_read=500,
            rows_written=500,
        )

        watermark_rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_watermark "
            f"WHERE dataset_id = :dataset_id",
            {"dataset_id": dataset_id},
        )
        assert len(watermark_rows) == 1
        wm = watermark_rows[0]
        assert wm["watermark_column"] == "updated_at"
        assert wm["last_watermark_value"] is not None
        assert wm["last_successful_execution_id"] == ds_exec_id

    def test_complete_success_full_load_does_not_update_watermark(self, db_connection):
        """Success + full load should NOT create/update etl_watermark."""
        exec_repo, ds_exec_id, dataset_id, _, _ = _setup_dataset_execution(
            db_connection,
            "ds_watermark_full",
            load_strategy="full",
        )

        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="success",
            rows_read=200,
            rows_written=200,
        )

        watermark_rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_watermark "
            f"WHERE dataset_id = :dataset_id",
            {"dataset_id": dataset_id},
        )
        assert len(watermark_rows) == 0

    def test_complete_failed_does_not_update_watermark(self, db_connection):
        """Failed status should NOT create/update etl_watermark even for incremental."""
        exec_repo, ds_exec_id, dataset_id, _, _ = _setup_dataset_execution(
            db_connection,
            "ds_watermark_fail",
            load_strategy="incremental",
            watermark_column="updated_at",
        )

        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="failed",
            error_message="Data quality check failed",
        )

        watermark_rows = db_connection.execute(
            f"SELECT * FROM {db_connection.schema_name}.etl_watermark "
            f"WHERE dataset_id = :dataset_id",
            {"dataset_id": dataset_id},
        )
        assert len(watermark_rows) == 0
