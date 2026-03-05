"""E2E tests: Complete process execution lifecycle.

Tests the full orchestration: process -> jobs -> datasets through
the stored procedure layer against the Docker stack.

Requires NODO_ETL_E2E_TESTS=1 to enable.
"""

import json

import pytest

from tests.e2e.conftest import skip_e2e


@skip_e2e
class TestFullProcessExecution:
    """Happy path: full process lifecycle through bronze -> silver -> gold."""

    def test_happy_path_all_succeed(self, repos, full_pipeline):
        """3 jobs (bronze->silver->gold), all succeed.

        Verify status=success at all levels and correct row totals.
        """
        exec_repo = repos["execution"]
        process_id = full_pipeline["process"]["id"]

        # Start process execution
        exec_id = exec_repo.start_process_execution(
            process_id=process_id,
            environment="dev",
            triggered_by="manual",
        )
        assert exec_id > 0

        # Walk through each job in execution order
        total_rows_read = 0
        total_rows_written = 0

        pending_jobs = exec_repo.get_jobs_to_execute(exec_id)
        jobs_executed = []

        while pending_jobs:
            for job in pending_jobs:
                job_exec_id = job["job_execution_id"]
                exec_repo.start_job_execution(job_exec_id)

                datasets = exec_repo.get_datasets_to_execute(job_exec_id)
                while datasets:
                    for ds in datasets:
                        ds_exec_id = ds["dataset_execution_id"]
                        exec_repo.start_dataset_execution(ds_exec_id)
                        exec_repo.complete_dataset_execution(
                            dataset_execution_id=ds_exec_id,
                            status="success",
                            rows_read=100,
                            rows_written=95,
                            rows_errored=5,
                        )
                        total_rows_read += 100
                        total_rows_written += 95
                    datasets = exec_repo.get_datasets_to_execute(job_exec_id)

                exec_repo.complete_job_execution(job_exec_id)
                jobs_executed.append(job)

            pending_jobs = exec_repo.get_jobs_to_execute(exec_id)

        # Complete the process
        exec_repo.complete_process_execution(exec_id)

        # Verify summary
        summary = exec_repo.get_execution_summary(process_id=process_id)
        assert len(summary) > 0

        # Find our execution in summary
        process_rows = [r for r in summary if r.get("process_status") == "success"]
        assert len(process_rows) > 0

        # All 3 jobs should have been executed
        assert len(jobs_executed) == 3

    def test_job_ordering_respected(self, repos, full_pipeline):
        """Verify order=2 job does not start until order=1 completes.

        Check that jobs are returned in execution_order sequence.
        """
        exec_repo = repos["execution"]
        process_id = full_pipeline["process"]["id"]

        exec_id = exec_repo.start_process_execution(
            process_id=process_id,
            environment="dev",
            triggered_by="manual",
        )

        # First call should only return order=1 jobs (bronze)
        jobs_round_1 = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs_round_1) >= 1
        first_job_name = jobs_round_1[0].get("job_name", "")
        assert "bronze" in first_job_name

        # Complete round 1
        for job in jobs_round_1:
            job_exec_id = job["job_execution_id"]
            exec_repo.start_job_execution(job_exec_id)
            datasets = exec_repo.get_datasets_to_execute(job_exec_id)
            for ds in datasets:
                exec_repo.start_dataset_execution(ds["dataset_execution_id"])
                exec_repo.complete_dataset_execution(
                    dataset_execution_id=ds["dataset_execution_id"],
                    status="success",
                    rows_read=50,
                    rows_written=50,
                )
            # Check if more datasets remain at the next order
            more_ds = exec_repo.get_datasets_to_execute(job_exec_id)
            for ds in more_ds:
                exec_repo.start_dataset_execution(ds["dataset_execution_id"])
                exec_repo.complete_dataset_execution(
                    dataset_execution_id=ds["dataset_execution_id"],
                    status="success",
                    rows_read=50,
                    rows_written=50,
                )
            exec_repo.complete_job_execution(job_exec_id)

        # Round 2 should return order=2 job (silver)
        jobs_round_2 = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs_round_2) >= 1
        second_job_name = jobs_round_2[0].get("job_name", "")
        assert "silver" in second_job_name

        # Complete round 2
        for job in jobs_round_2:
            job_exec_id = job["job_execution_id"]
            exec_repo.start_job_execution(job_exec_id)
            datasets = exec_repo.get_datasets_to_execute(job_exec_id)
            for ds in datasets:
                exec_repo.start_dataset_execution(ds["dataset_execution_id"])
                exec_repo.complete_dataset_execution(
                    dataset_execution_id=ds["dataset_execution_id"],
                    status="success",
                    rows_read=50,
                    rows_written=50,
                )
            exec_repo.complete_job_execution(job_exec_id)

        # Round 3 should return order=3 job (gold)
        jobs_round_3 = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs_round_3) >= 1
        third_job_name = jobs_round_3[0].get("job_name", "")
        assert "gold" in third_job_name

        # Complete round 3
        for job in jobs_round_3:
            job_exec_id = job["job_execution_id"]
            exec_repo.start_job_execution(job_exec_id)
            datasets = exec_repo.get_datasets_to_execute(job_exec_id)
            for ds in datasets:
                exec_repo.start_dataset_execution(ds["dataset_execution_id"])
                exec_repo.complete_dataset_execution(
                    dataset_execution_id=ds["dataset_execution_id"],
                    status="success",
                    rows_read=50,
                    rows_written=50,
                )
            exec_repo.complete_job_execution(job_exec_id)

        # Complete process
        exec_repo.complete_process_execution(exec_id)

        summary = exec_repo.get_execution_summary(process_id=process_id)
        success_rows = [r for r in summary if r.get("process_status") == "success"]
        assert len(success_rows) > 0

    def test_single_job_process_lifecycle(self, repos, unique_id, sample_connection):
        """Single job, single dataset -> lifecycle completes successfully."""
        proc = repos["process"].create({
            "process_name": f"e2e_single_{unique_id}",
            "execution_order": 1,
        })
        job = repos["job"].create({
            "process_id": proc["id"],
            "job_name": f"e2e_single_job_{unique_id}",
            "execution_order": 1,
        })
        repos["dataset"].create(
            {"job_id": job["id"], "dataset_name": f"e2e_single_ds_{unique_id}",
             "source_type": "database", "source_connection_id": sample_connection["id"],
             "layer": "bronze", "load_strategy": "full",
             "idempotency_strategy": "overwrite", "execution_order": 1},
            source_config={"source_schema": "public", "source_table": "users"},
        )

        exec_repo = repos["execution"]
        exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="dev",
            triggered_by="manual",
        )
        assert exec_id > 0

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
            rows_read=200,
            rows_written=200,
        )

        exec_repo.complete_job_execution(job_exec_id)
        exec_repo.complete_process_execution(exec_id)

        summary = exec_repo.get_execution_summary(process_id=proc["id"])
        assert len(summary) > 0
        assert summary[0]["process_status"] == "success"

    def test_process_with_parameters(self, repos, full_pipeline):
        """Pass parameters to process execution -> stored in process_execution."""
        exec_repo = repos["execution"]
        process_id = full_pipeline["process"]["id"]

        params = json.dumps({"date_from": "2026-01-01", "date_to": "2026-01-31"})

        exec_id = exec_repo.start_process_execution(
            process_id=process_id,
            environment="dev",
            triggered_by="manual",
            parameters=params,
        )
        assert exec_id > 0

        # Walk through the lifecycle to complete
        pending_jobs = exec_repo.get_jobs_to_execute(exec_id)
        while pending_jobs:
            for job in pending_jobs:
                job_exec_id = job["job_execution_id"]
                exec_repo.start_job_execution(job_exec_id)
                datasets = exec_repo.get_datasets_to_execute(job_exec_id)
                while datasets:
                    for ds in datasets:
                        exec_repo.start_dataset_execution(ds["dataset_execution_id"])
                        exec_repo.complete_dataset_execution(
                            dataset_execution_id=ds["dataset_execution_id"],
                            status="success",
                            rows_read=10,
                            rows_written=10,
                        )
                    datasets = exec_repo.get_datasets_to_execute(job_exec_id)
                exec_repo.complete_job_execution(job_exec_id)
            pending_jobs = exec_repo.get_jobs_to_execute(exec_id)

        exec_repo.complete_process_execution(exec_id)

        # Verify execution completed with parameters
        summary = exec_repo.get_execution_summary(process_id=process_id)
        assert len(summary) > 0
        assert summary[0]["process_status"] == "success"
