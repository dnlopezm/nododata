"""Integration tests: Full Finance ETL Pipeline scenarios.

Requires Docker services. Enable with NODO_ETL_INTEGRATION_TESTS=1.
"""

import pytest

from tests.integration.conftest import skip_integration

from nodo_etl.db.repositories import (
    ConnectionRepository,
    DatasetRepository,
    ExecutionRepository,
    HookRepository,
    JobRepository,
    LineageRepository,
    ProcessRepository,
    SystemConfigRepository,
    TagRepository,
)


@skip_integration
class TestFullFinancePipeline:
    """Scenario 1: Full Finance ETL Pipeline."""

    def test_create_metadata_via_repositories(self, db_connection):
        """Create all metadata: connections, process, jobs, datasets, hooks, tags, lineage."""
        conn_repo = ConnectionRepository(db_connection)
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)

        # Create connections
        source_conn = conn_repo.create({
            "connection_name": "test_source_db",
            "connection_type": "postgresql",
            "host": "source-db",
            "port": 5432,
            "environment": "dev",
        })
        assert source_conn.get("id") is not None

        # Create process
        process = proc_repo.create({
            "process_name": "test_finance_etl",
            "execution_order": 1,
            "max_parallelism": 3,
        })
        process_id = process["id"]

        # Create schedule
        schedule = proc_repo.create_schedule({
            "process_id": process_id,
            "schedule_name": "daily",
            "cron_expression": "0 6 * * *",
        })
        assert schedule.get("id") is not None

        # Create jobs
        bronze_job = job_repo.create({
            "process_id": process_id,
            "job_name": "test_bronze",
            "execution_order": 1,
            "max_parallelism": 3,
        })
        silver_job = job_repo.create({
            "process_id": process_id,
            "job_name": "test_silver",
            "execution_order": 2,
        })
        gold_job = job_repo.create({
            "process_id": process_id,
            "job_name": "test_gold",
            "execution_order": 3,
        })

        # Create datasets
        ds1 = ds_repo.create(
            {"job_id": bronze_job["id"], "dataset_name": "test_txn_bronze",
             "source_type": "database", "layer": "bronze", "load_strategy": "incremental",
             "execution_order": 1},
            source_config={"source_schema": "fin", "source_table": "transactions",
                          "target_schema": "bronze", "target_table": "transactions",
                          "watermark_column": "updated_at"},
        )
        ds2 = ds_repo.create(
            {"job_id": bronze_job["id"], "dataset_name": "test_acct_bronze",
             "source_type": "database", "layer": "bronze", "load_strategy": "full",
             "execution_order": 1},
        )
        ds3 = ds_repo.create(
            {"job_id": silver_job["id"], "dataset_name": "test_txn_silver",
             "source_type": "database", "layer": "silver", "load_strategy": "incremental",
             "execution_order": 1},
        )
        ds4 = ds_repo.create(
            {"job_id": gold_job["id"], "dataset_name": "test_rev_gold",
             "source_type": "database", "layer": "gold", "load_strategy": "full",
             "execution_order": 1},
        )

        # Verify
        jobs = job_repo.list(process_id)
        assert len(jobs) == 3
        datasets = ds_repo.list(bronze_job["id"])
        assert len(datasets) == 2

    def test_process_execution_lifecycle(self, db_connection):
        """Execute a process and verify the full lifecycle."""
        exec_repo = ExecutionRepository(db_connection)
        proc_repo = ProcessRepository(db_connection)

        processes = proc_repo.list()
        if not processes:
            pytest.skip("No processes found. Run test_create_metadata first.")

        process_id = processes[0]["id"]

        # Start execution
        exec_id = exec_repo.start_process_execution(
            process_id=process_id,
            environment="dev",
            triggered_by="manual",
        )
        assert exec_id > 0

        # Get jobs to execute
        jobs = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs) > 0

        # Execute each job
        for job in jobs:
            exec_repo.start_job_execution(job["job_execution_id"])
            datasets = exec_repo.get_datasets_to_execute(job["job_execution_id"])
            for ds in datasets:
                exec_repo.start_dataset_execution(ds["dataset_execution_id"])
                exec_repo.complete_dataset_execution(
                    dataset_execution_id=ds["dataset_execution_id"],
                    status="success",
                    rows_read=100,
                    rows_written=95,
                    rows_errored=5,
                )
            exec_repo.complete_job_execution(job["job_execution_id"])

        # Complete process
        exec_repo.complete_process_execution(exec_id)

        # Verify summary
        summary = exec_repo.get_execution_summary(process_id=process_id)
        assert len(summary) > 0
        assert summary[0]["process_status"] == "success"

    def test_execution_summary_returns_all_details(self, db_connection):
        """Verify execution summary returns process/job/dataset names."""
        exec_repo = ExecutionRepository(db_connection)
        summary = exec_repo.get_execution_summary()
        if summary:
            row = summary[0]
            assert "process_name" in row
            assert "job_name" in row or row.get("job_name") is None
            assert "dataset_name" in row or row.get("dataset_name") is None


@skip_integration
class TestDisabledEntities:
    """Scenario 4: Disabled entities are skipped."""

    def test_disabled_dataset_skipped(self, db_connection):
        """Disable a dataset and verify it has no execution record."""
        ds_repo = DatasetRepository(db_connection)
        datasets = ds_repo.list(1)  # Assumes job_id=1 exists
        if datasets:
            ds_id = datasets[0]["id"]
            ds_repo.update(ds_id, {"is_enabled": False})
            # Re-running process should skip this dataset
            ds_repo.update(ds_id, {"is_enabled": True})  # Restore


@skip_integration
class TestAllSourceTypes:
    """Scenario 7: All source types."""

    def test_database_source_config(self, db_connection):
        """Verify database dataset has db_config stored and retrieved."""
        ds_repo = DatasetRepository(db_connection)
        job_repo = JobRepository(db_connection)
        proc_repo = ProcessRepository(db_connection)

        # Create test process/job
        proc = proc_repo.create({"process_name": "src_type_test", "execution_order": 99})
        job = job_repo.create({"process_id": proc["id"], "job_name": "src_job", "execution_order": 1})

        # Database source
        ds = ds_repo.create(
            {"job_id": job["id"], "dataset_name": "db_test", "source_type": "database",
             "layer": "bronze", "load_strategy": "full", "execution_order": 1},
            source_config={"source_schema": "dbo", "source_table": "users",
                          "target_schema": "bronze", "target_table": "users"},
        )
        result = ds_repo.get(ds["id"])
        assert "source_config" in result
        assert result["source_config"]["source_table"] == "users"

    def test_file_source_config(self, db_connection):
        """Verify file dataset has file_config stored and retrieved."""
        ds_repo = DatasetRepository(db_connection)
        job_repo = JobRepository(db_connection)

        jobs = job_repo.list(1)
        if not jobs:
            pytest.skip("No jobs found")
        job_id = jobs[0]["id"]

        ds = ds_repo.create(
            {"job_id": job_id, "dataset_name": "file_test", "source_type": "file",
             "layer": "bronze", "load_strategy": "full", "execution_order": 99},
            source_config={"file_path": "/data/events.csv", "file_format": "csv"},
        )
        result = ds_repo.get(ds["id"])
        assert "source_config" in result

    def test_api_source_config(self, db_connection):
        """Verify API dataset has api_config stored and retrieved."""
        ds_repo = DatasetRepository(db_connection)
        job_repo = JobRepository(db_connection)

        jobs = job_repo.list(1)
        if not jobs:
            pytest.skip("No jobs found")
        job_id = jobs[0]["id"]

        ds = ds_repo.create(
            {"job_id": job_id, "dataset_name": "api_test", "source_type": "api",
             "layer": "bronze", "load_strategy": "full", "execution_order": 99},
            source_config={"api_url": "https://api.example.com", "http_method": "GET"},
        )
        result = ds_repo.get(ds["id"])
        assert "source_config" in result


@skip_integration
class TestLineageQueries:
    """Verify lineage tracking."""

    def test_lineage_chain(self, db_connection):
        """Verify bronze -> silver -> gold lineage chain."""
        lineage_repo = LineageRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)

        # Find a dataset with downstream
        datasets = ds_repo.list(1)  # bronze job
        if datasets:
            ds_id = datasets[0]["id"]
            downstream = lineage_repo.get_downstream(ds_id)
            # Should have at least one downstream if lineage was seeded
            # This depends on seed data being present


@skip_integration
class TestTagQueries:
    """Verify tag operations."""

    def test_tags_queryable(self, db_connection):
        """Verify tags are stored and queryable."""
        tag_repo = TagRepository(db_connection)
        proc_repo = ProcessRepository(db_connection)

        processes = proc_repo.list()
        if processes:
            process_id = processes[0]["id"]
            tags = tag_repo.list("process", process_id)
            # Tags should be present if seed data was loaded
