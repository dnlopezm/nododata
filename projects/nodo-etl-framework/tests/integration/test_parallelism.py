"""Integration tests: Parallelism control scenarios.

Requires Docker services. Enable with NODO_ETL_INTEGRATION_TESTS=1.
"""

import pytest

from tests.integration.conftest import skip_integration

from nodo_etl.db.repositories import (
    DatasetRepository,
    ExecutionRepository,
    JobRepository,
    ProcessRepository,
)


@skip_integration
class TestParallelismControl:
    """Scenario: Parallelism control via max_parallelism and execution_order."""

    def test_job_parallelism_limits_datasets(self, db_connection):
        """Job max_parallelism=2 with 5 datasets should return only 2 at a time."""
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)
        exec_repo = ExecutionRepository(db_connection)

        # Create process with constrained parallelism
        proc = proc_repo.create({
            "process_name": "parallel_test",
            "execution_order": 99,
            "max_parallelism": 1,
        })
        job = job_repo.create({
            "process_id": proc["id"],
            "job_name": "parallel_job",
            "execution_order": 1,
            "max_parallelism": 2,
        })

        # Create 5 datasets all at order=1
        for i in range(5):
            ds_repo.create({
                "job_id": job["id"],
                "dataset_name": f"parallel_ds_{i}",
                "source_type": "database",
                "layer": "bronze",
                "load_strategy": "full",
                "execution_order": 1,
            })

        # Start execution
        exec_id = exec_repo.start_process_execution(
            process_id=proc["id"], environment="dev", triggered_by="manual",
        )

        jobs = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs) == 1

        exec_repo.start_job_execution(jobs[0]["job_execution_id"])
        datasets = exec_repo.get_datasets_to_execute(jobs[0]["job_execution_id"])
        # Should return only 2 (max_parallelism)
        assert len(datasets) == 2

    def test_execution_order_respected(self, db_connection):
        """Datasets at order=2 should not be returned until order=1 is done."""
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)
        exec_repo = ExecutionRepository(db_connection)

        proc = proc_repo.create({
            "process_name": "order_test",
            "execution_order": 99,
        })
        job = job_repo.create({
            "process_id": proc["id"],
            "job_name": "order_job",
            "execution_order": 1,
            "max_parallelism": 10,
        })

        # Create datasets at different orders
        ds_repo.create({
            "job_id": job["id"], "dataset_name": "order1_ds",
            "source_type": "database", "layer": "bronze",
            "load_strategy": "full", "execution_order": 1,
        })
        ds_repo.create({
            "job_id": job["id"], "dataset_name": "order2_ds",
            "source_type": "database", "layer": "bronze",
            "load_strategy": "full", "execution_order": 2,
        })

        exec_id = exec_repo.start_process_execution(
            process_id=proc["id"], environment="dev", triggered_by="manual",
        )
        jobs = exec_repo.get_jobs_to_execute(exec_id)
        exec_repo.start_job_execution(jobs[0]["job_execution_id"])

        # Should only get order=1 datasets first
        datasets = exec_repo.get_datasets_to_execute(jobs[0]["job_execution_id"])
        assert len(datasets) == 1
        assert datasets[0]["dataset_name"] == "order1_ds"
