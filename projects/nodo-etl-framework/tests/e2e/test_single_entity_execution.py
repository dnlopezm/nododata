"""E2E tests: Running individual entities.

Tests single-process execution, disabled datasets, and disabled jobs.

Requires NODO_ETL_E2E_TESTS=1 to enable.
"""

import pytest

from tests.e2e.conftest import skip_e2e


@skip_e2e
class TestSingleEntityExecution:
    """Running individual entities with enable/disable behavior."""

    def test_run_single_process_full_lifecycle(self, repos, unique_id, sample_connection):
        """Run a single process through the full lifecycle."""
        proc = repos["process"].create({
            "process_name": f"e2e_single_proc_{unique_id}",
            "execution_order": 1,
        })
        job = repos["job"].create({
            "process_id": proc["id"],
            "job_name": f"e2e_single_j_{unique_id}",
            "execution_order": 1,
        })
        repos["dataset"].create(
            {"job_id": job["id"], "dataset_name": f"e2e_single_d_{unique_id}",
             "source_type": "database", "source_connection_id": sample_connection["id"],
             "layer": "bronze", "load_strategy": "full",
             "idempotency_strategy": "overwrite", "execution_order": 1},
            source_config={"source_schema": "public", "source_table": "items"},
        )

        exec_repo = repos["execution"]

        # Start process
        exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="dev",
            triggered_by="manual",
        )
        assert exec_id > 0

        # Execute jobs
        jobs = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs) == 1

        job_exec_id = jobs[0]["job_execution_id"]
        exec_repo.start_job_execution(job_exec_id)

        datasets = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(datasets) == 1

        ds_exec_id = datasets[0]["dataset_execution_id"]
        exec_repo.start_dataset_execution(ds_exec_id)
        exec_repo.complete_dataset_execution(
            dataset_execution_id=ds_exec_id,
            status="success",
            rows_read=500,
            rows_written=500,
        )

        exec_repo.complete_job_execution(job_exec_id)
        exec_repo.complete_process_execution(exec_id)

        summary = exec_repo.get_execution_summary(process_id=proc["id"])
        assert len(summary) > 0
        assert summary[0]["process_status"] == "success"

    def test_disabled_dataset_skipped(self, repos, unique_id, sample_connection):
        """Disabled dataset should not appear in datasets to execute."""
        proc = repos["process"].create({
            "process_name": f"e2e_dis_ds_{unique_id}",
            "execution_order": 1,
        })
        job = repos["job"].create({
            "process_id": proc["id"],
            "job_name": f"e2e_dis_ds_j_{unique_id}",
            "execution_order": 1,
        })

        # Create an enabled dataset
        enabled_ds = repos["dataset"].create(
            {"job_id": job["id"], "dataset_name": f"e2e_enabled_{unique_id}",
             "source_type": "database", "source_connection_id": sample_connection["id"],
             "layer": "bronze", "load_strategy": "full",
             "idempotency_strategy": "overwrite", "execution_order": 1},
            source_config={"source_schema": "public", "source_table": "active"},
        )

        # Create a disabled dataset
        disabled_ds = repos["dataset"].create(
            {"job_id": job["id"], "dataset_name": f"e2e_disabled_{unique_id}",
             "source_type": "database", "source_connection_id": sample_connection["id"],
             "layer": "bronze", "load_strategy": "full",
             "idempotency_strategy": "overwrite", "execution_order": 1,
             "is_enabled": False},
            source_config={"source_schema": "public", "source_table": "inactive"},
        )

        exec_repo = repos["execution"]

        exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="dev",
            triggered_by="manual",
        )

        jobs = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs) == 1

        job_exec_id = jobs[0]["job_execution_id"]
        exec_repo.start_job_execution(job_exec_id)

        datasets = exec_repo.get_datasets_to_execute(job_exec_id)

        # Only the enabled dataset should be returned
        ds_names = [ds.get("dataset_name", "") for ds in datasets]
        assert f"e2e_enabled_{unique_id}" in ds_names
        assert f"e2e_disabled_{unique_id}" not in ds_names

        # Complete enabled dataset
        for ds in datasets:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success",
                rows_read=10,
                rows_written=10,
            )

        exec_repo.complete_job_execution(job_exec_id)
        exec_repo.complete_process_execution(exec_id)

    def test_disabled_job_skipped(self, repos, unique_id, sample_connection):
        """Disabled job should not appear in jobs to execute."""
        proc = repos["process"].create({
            "process_name": f"e2e_dis_job_{unique_id}",
            "execution_order": 1,
        })

        # Create an enabled job
        enabled_job = repos["job"].create({
            "process_id": proc["id"],
            "job_name": f"e2e_en_job_{unique_id}",
            "execution_order": 1,
        })
        repos["dataset"].create(
            {"job_id": enabled_job["id"], "dataset_name": f"e2e_en_j_ds_{unique_id}",
             "source_type": "database", "source_connection_id": sample_connection["id"],
             "layer": "bronze", "load_strategy": "full",
             "idempotency_strategy": "overwrite", "execution_order": 1},
            source_config={"source_schema": "public", "source_table": "data"},
        )

        # Create a disabled job
        disabled_job = repos["job"].create({
            "process_id": proc["id"],
            "job_name": f"e2e_dis_j_{unique_id}",
            "execution_order": 2,
            "is_enabled": False,
        })
        repos["dataset"].create(
            {"job_id": disabled_job["id"], "dataset_name": f"e2e_dis_j_ds_{unique_id}",
             "source_type": "database", "source_connection_id": sample_connection["id"],
             "layer": "bronze", "load_strategy": "full",
             "idempotency_strategy": "overwrite", "execution_order": 1},
            source_config={"source_schema": "public", "source_table": "skip_me"},
        )

        exec_repo = repos["execution"]

        exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="dev",
            triggered_by="manual",
        )

        # Get all jobs across all rounds
        all_job_names = []
        pending_jobs = exec_repo.get_jobs_to_execute(exec_id)
        while pending_jobs:
            for job in pending_jobs:
                all_job_names.append(job.get("job_name", ""))
                job_exec_id = job["job_execution_id"]
                exec_repo.start_job_execution(job_exec_id)
                datasets = exec_repo.get_datasets_to_execute(job_exec_id)
                for ds in datasets:
                    exec_repo.start_dataset_execution(ds["dataset_execution_id"])
                    exec_repo.complete_dataset_execution(
                        dataset_execution_id=ds["dataset_execution_id"],
                        status="success",
                        rows_read=10,
                        rows_written=10,
                    )
                exec_repo.complete_job_execution(job_exec_id)
            pending_jobs = exec_repo.get_jobs_to_execute(exec_id)

        exec_repo.complete_process_execution(exec_id)

        # Disabled job should not appear
        assert f"e2e_en_job_{unique_id}" in all_job_names
        assert f"e2e_dis_j_{unique_id}" not in all_job_names
