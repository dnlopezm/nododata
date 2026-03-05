"""E2E tests: Parallelism control.

Tests max_parallelism constraints at job and process levels,
and execution_order sequencing for datasets.

Requires NODO_ETL_E2E_TESTS=1 to enable.
"""

import pytest

from tests.e2e.conftest import skip_e2e


@skip_e2e
class TestParallelism:
    """Parallelism control via max_parallelism and execution_order."""

    def test_job_max_parallelism_limits_datasets(
        self, repos, unique_id, sample_connection,
    ):
        """Job max_parallelism=2 with 5 datasets -> only 2 returned at a time."""
        proc = repos["process"].create({
            "process_name": f"e2e_par_{unique_id}",
            "execution_order": 1,
            "max_parallelism": 1,
        })
        job = repos["job"].create({
            "process_id": proc["id"],
            "job_name": f"e2e_par_job_{unique_id}",
            "execution_order": 1,
            "max_parallelism": 2,
        })

        # Create 5 datasets all at execution_order=1
        for i in range(5):
            repos["dataset"].create(
                {"job_id": job["id"],
                 "dataset_name": f"e2e_par_ds_{i}_{unique_id}",
                 "source_type": "database",
                 "source_connection_id": sample_connection["id"],
                 "layer": "bronze",
                 "load_strategy": "full",
                 "idempotency_strategy": "overwrite",
                 "execution_order": 1},
                source_config={
                    "source_schema": "public",
                    "source_table": f"table_{i}",
                },
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

        # First batch: should return exactly 2 (max_parallelism)
        batch_1 = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(batch_1) == 2

        # Complete batch 1
        for ds in batch_1:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success",
                rows_read=10,
                rows_written=10,
            )

        # Second batch: should return 2 more
        batch_2 = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(batch_2) == 2

        # Complete batch 2
        for ds in batch_2:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success",
                rows_read=10,
                rows_written=10,
            )

        # Third batch: should return the last 1
        batch_3 = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(batch_3) == 1

        # Complete batch 3
        for ds in batch_3:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success",
                rows_read=10,
                rows_written=10,
            )

        # No more datasets
        batch_4 = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(batch_4) == 0

        exec_repo.complete_job_execution(job_exec_id)
        exec_repo.complete_process_execution(exec_id)

    def test_datasets_at_different_orders(
        self, repos, unique_id, sample_connection,
    ):
        """Datasets at order=1 must complete before order=2 datasets are returned."""
        proc = repos["process"].create({
            "process_name": f"e2e_ord_{unique_id}",
            "execution_order": 1,
        })
        job = repos["job"].create({
            "process_id": proc["id"],
            "job_name": f"e2e_ord_job_{unique_id}",
            "execution_order": 1,
            "max_parallelism": 10,
        })

        # Order 1 datasets
        repos["dataset"].create(
            {"job_id": job["id"],
             "dataset_name": f"e2e_ord1_a_{unique_id}",
             "source_type": "database",
             "source_connection_id": sample_connection["id"],
             "layer": "bronze",
             "load_strategy": "full",
             "idempotency_strategy": "overwrite",
             "execution_order": 1},
            source_config={"source_schema": "public", "source_table": "order1_a"},
        )
        repos["dataset"].create(
            {"job_id": job["id"],
             "dataset_name": f"e2e_ord1_b_{unique_id}",
             "source_type": "database",
             "source_connection_id": sample_connection["id"],
             "layer": "bronze",
             "load_strategy": "full",
             "idempotency_strategy": "overwrite",
             "execution_order": 1},
            source_config={"source_schema": "public", "source_table": "order1_b"},
        )

        # Order 2 datasets
        repos["dataset"].create(
            {"job_id": job["id"],
             "dataset_name": f"e2e_ord2_a_{unique_id}",
             "source_type": "database",
             "source_connection_id": sample_connection["id"],
             "layer": "silver",
             "load_strategy": "incremental",
             "idempotency_strategy": "upsert",
             "execution_order": 2},
            source_config={"source_schema": "silver", "source_table": "order2_a",
                          "watermark_column": "updated_at"},
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

        # First call should only return order=1 datasets
        datasets_round_1 = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(datasets_round_1) == 2
        for ds in datasets_round_1:
            assert "ord1" in ds.get("dataset_name", "")

        # Complete order=1 datasets
        for ds in datasets_round_1:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success",
                rows_read=20,
                rows_written=20,
            )

        # Now order=2 datasets should be returned
        datasets_round_2 = exec_repo.get_datasets_to_execute(job_exec_id)
        assert len(datasets_round_2) == 1
        assert "ord2" in datasets_round_2[0].get("dataset_name", "")

        # Complete order=2
        exec_repo.start_dataset_execution(datasets_round_2[0]["dataset_execution_id"])
        exec_repo.complete_dataset_execution(
            dataset_execution_id=datasets_round_2[0]["dataset_execution_id"],
            status="success",
            rows_read=15,
            rows_written=15,
        )

        exec_repo.complete_job_execution(job_exec_id)
        exec_repo.complete_process_execution(exec_id)

    def test_process_max_parallelism_limits_jobs(
        self, repos, unique_id, sample_connection,
    ):
        """Process max_parallelism limits how many jobs run concurrently.

        With max_parallelism=1 and 3 jobs at the same execution_order,
        only 1 job should be returned at a time.
        """
        proc = repos["process"].create({
            "process_name": f"e2e_ppar_{unique_id}",
            "execution_order": 1,
            "max_parallelism": 1,
        })

        # Create 3 jobs all at the same execution_order
        job_ids = []
        for i in range(3):
            job = repos["job"].create({
                "process_id": proc["id"],
                "job_name": f"e2e_ppar_j{i}_{unique_id}",
                "execution_order": 1,
            })
            job_ids.append(job["id"])
            repos["dataset"].create(
                {"job_id": job["id"],
                 "dataset_name": f"e2e_ppar_ds{i}_{unique_id}",
                 "source_type": "database",
                 "source_connection_id": sample_connection["id"],
                 "layer": "bronze",
                 "load_strategy": "full",
                 "idempotency_strategy": "overwrite",
                 "execution_order": 1},
                source_config={
                    "source_schema": "public",
                    "source_table": f"proc_par_{i}",
                },
            )

        exec_repo = repos["execution"]
        exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="dev",
            triggered_by="manual",
        )

        # With process max_parallelism=1, should get at most 1 job
        jobs_batch_1 = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs_batch_1) == 1

        # Complete the first job
        j_exec_id = jobs_batch_1[0]["job_execution_id"]
        exec_repo.start_job_execution(j_exec_id)
        datasets = exec_repo.get_datasets_to_execute(j_exec_id)
        for ds in datasets:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success",
                rows_read=5,
                rows_written=5,
            )
        exec_repo.complete_job_execution(j_exec_id)

        # Next batch should return 1 more job
        jobs_batch_2 = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs_batch_2) == 1

        # Complete second job
        j_exec_id = jobs_batch_2[0]["job_execution_id"]
        exec_repo.start_job_execution(j_exec_id)
        datasets = exec_repo.get_datasets_to_execute(j_exec_id)
        for ds in datasets:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success",
                rows_read=5,
                rows_written=5,
            )
        exec_repo.complete_job_execution(j_exec_id)

        # Third batch
        jobs_batch_3 = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs_batch_3) == 1

        j_exec_id = jobs_batch_3[0]["job_execution_id"]
        exec_repo.start_job_execution(j_exec_id)
        datasets = exec_repo.get_datasets_to_execute(j_exec_id)
        for ds in datasets:
            exec_repo.start_dataset_execution(ds["dataset_execution_id"])
            exec_repo.complete_dataset_execution(
                dataset_execution_id=ds["dataset_execution_id"],
                status="success",
                rows_read=5,
                rows_written=5,
            )
        exec_repo.complete_job_execution(j_exec_id)

        # No more jobs
        jobs_batch_4 = exec_repo.get_jobs_to_execute(exec_id)
        assert len(jobs_batch_4) == 0

        exec_repo.complete_process_execution(exec_id)

        summary = exec_repo.get_execution_summary(process_id=proc["id"])
        assert len(summary) > 0
        assert summary[0]["process_status"] == "success"
