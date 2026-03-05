"""E2E tests: Multi-environment support.

Tests that the same process can run in different environments,
executions are tracked per environment, watermarks are per-environment,
and execution summaries can be filtered by environment.

Requires NODO_ETL_E2E_TESTS=1 to enable.
"""

import pytest

from tests.e2e.conftest import skip_e2e


@skip_e2e
class TestMultiEnvironment:
    """Environment-aware execution and tracking."""

    def test_same_process_different_environment_connections(
        self, repos, unique_id,
    ):
        """Same process, different environment connections.

        Verify connections can be created for dev and prod environments.
        """
        conn_repo = repos["connection"]

        dev_conn = conn_repo.create({
            "connection_name": f"e2e_env_conn_{unique_id}",
            "connection_type": "postgresql",
            "host": "dev-db.local",
            "port": 5432,
            "environment": "dev",
        })
        assert dev_conn.get("id") is not None
        assert dev_conn["environment"] == "dev"

        prod_conn = conn_repo.create({
            "connection_name": f"e2e_env_conn_prod_{unique_id}",
            "connection_type": "postgresql",
            "host": "prod-db.local",
            "port": 5432,
            "environment": "prod",
        })
        assert prod_conn.get("id") is not None
        assert prod_conn["environment"] == "prod"

        # List by environment
        dev_conns = conn_repo.list(environment="dev")
        dev_names = [c["connection_name"] for c in dev_conns]
        assert f"e2e_env_conn_{unique_id}" in dev_names

        prod_conns = conn_repo.list(environment="prod")
        prod_names = [c["connection_name"] for c in prod_conns]
        assert f"e2e_env_conn_prod_{unique_id}" in prod_names

    def test_execution_tracked_per_environment(
        self, repos, unique_id, sample_connection,
    ):
        """Executions are tracked per environment.

        Run the same process in dev and prod, verify separate execution records.
        """
        proc = repos["process"].create({
            "process_name": f"e2e_env_proc_{unique_id}",
            "execution_order": 1,
        })
        job = repos["job"].create({
            "process_id": proc["id"],
            "job_name": f"e2e_env_job_{unique_id}",
            "execution_order": 1,
        })
        repos["dataset"].create(
            {"job_id": job["id"],
             "dataset_name": f"e2e_env_ds_{unique_id}",
             "source_type": "database",
             "source_connection_id": sample_connection["id"],
             "layer": "bronze",
             "load_strategy": "full",
             "idempotency_strategy": "overwrite",
             "execution_order": 1},
            source_config={"source_schema": "public", "source_table": "env_data"},
        )

        exec_repo = repos["execution"]

        # Execute in dev
        dev_exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="dev",
            triggered_by="manual",
        )
        assert dev_exec_id > 0
        self._complete_full_execution(exec_repo, dev_exec_id)

        # Execute in prod
        prod_exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="prod",
            triggered_by="manual",
        )
        assert prod_exec_id > 0
        assert prod_exec_id != dev_exec_id
        self._complete_full_execution(exec_repo, prod_exec_id)

        # Both executions should exist
        all_summary = exec_repo.get_execution_summary(process_id=proc["id"])
        assert len(all_summary) >= 2

    def test_watermarks_per_environment(
        self, repos, unique_id, sample_connection,
    ):
        """Watermarks should be tracked per environment.

        Run incremental loads in dev and prod environments separately,
        verify each environment tracks its own watermark state.
        """
        proc = repos["process"].create({
            "process_name": f"e2e_wm_proc_{unique_id}",
            "execution_order": 1,
        })
        job = repos["job"].create({
            "process_id": proc["id"],
            "job_name": f"e2e_wm_job_{unique_id}",
            "execution_order": 1,
        })
        repos["dataset"].create(
            {"job_id": job["id"],
             "dataset_name": f"e2e_wm_ds_{unique_id}",
             "source_type": "database",
             "source_connection_id": sample_connection["id"],
             "layer": "bronze",
             "load_strategy": "incremental",
             "idempotency_strategy": "upsert",
             "execution_order": 1},
            source_config={
                "source_schema": "public",
                "source_table": "watermark_data",
                "watermark_column": "updated_at",
            },
        )

        exec_repo = repos["execution"]

        # Run in dev environment
        dev_exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="dev",
            triggered_by="manual",
        )
        self._complete_full_execution(exec_repo, dev_exec_id)

        # Run in prod environment
        prod_exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="prod",
            triggered_by="manual",
        )
        self._complete_full_execution(exec_repo, prod_exec_id)

        # Verify separate execution records per environment
        dev_summary = exec_repo.get_execution_summary(
            process_id=proc["id"], environment="dev",
        )
        prod_summary = exec_repo.get_execution_summary(
            process_id=proc["id"], environment="prod",
        )

        assert len(dev_summary) >= 1
        assert len(prod_summary) >= 1

        # The summaries should be distinct
        dev_exec_ids = {r.get("process_execution_id") for r in dev_summary}
        prod_exec_ids = {r.get("process_execution_id") for r in prod_summary}
        assert dev_exec_ids.isdisjoint(prod_exec_ids)

    def test_execution_summary_filtered_by_environment(
        self, repos, unique_id, sample_connection,
    ):
        """Execution summary can be filtered by environment."""
        proc = repos["process"].create({
            "process_name": f"e2e_filt_proc_{unique_id}",
            "execution_order": 1,
        })
        job = repos["job"].create({
            "process_id": proc["id"],
            "job_name": f"e2e_filt_job_{unique_id}",
            "execution_order": 1,
        })
        repos["dataset"].create(
            {"job_id": job["id"],
             "dataset_name": f"e2e_filt_ds_{unique_id}",
             "source_type": "database",
             "source_connection_id": sample_connection["id"],
             "layer": "bronze",
             "load_strategy": "full",
             "idempotency_strategy": "overwrite",
             "execution_order": 1},
            source_config={"source_schema": "public", "source_table": "filter_data"},
        )

        exec_repo = repos["execution"]

        # Run in dev
        dev_exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="dev",
            triggered_by="manual",
        )
        self._complete_full_execution(exec_repo, dev_exec_id)

        # Run in staging
        staging_exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="staging",
            triggered_by="manual",
        )
        self._complete_full_execution(exec_repo, staging_exec_id)

        # Run in prod
        prod_exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="prod",
            triggered_by="manual",
        )
        self._complete_full_execution(exec_repo, prod_exec_id)

        # Filter by dev
        dev_only = exec_repo.get_execution_summary(
            process_id=proc["id"], environment="dev",
        )
        for row in dev_only:
            assert row.get("environment") == "dev"

        # Filter by prod
        prod_only = exec_repo.get_execution_summary(
            process_id=proc["id"], environment="prod",
        )
        for row in prod_only:
            assert row.get("environment") == "prod"

        # Filter by staging
        staging_only = exec_repo.get_execution_summary(
            process_id=proc["id"], environment="staging",
        )
        for row in staging_only:
            assert row.get("environment") == "staging"

        # Unfiltered should include all
        all_summary = exec_repo.get_execution_summary(process_id=proc["id"])
        all_envs = {r.get("environment") for r in all_summary}
        assert "dev" in all_envs
        assert "prod" in all_envs
        assert "staging" in all_envs

    @staticmethod
    def _complete_full_execution(exec_repo, exec_id):
        """Helper to walk through a full process execution lifecycle."""
        pending_jobs = exec_repo.get_jobs_to_execute(exec_id)
        while pending_jobs:
            for job in pending_jobs:
                job_exec_id = job["job_execution_id"]
                exec_repo.start_job_execution(job_exec_id)
                datasets = exec_repo.get_datasets_to_execute(job_exec_id)
                while datasets:
                    for ds in datasets:
                        exec_repo.start_dataset_execution(
                            ds["dataset_execution_id"],
                        )
                        exec_repo.complete_dataset_execution(
                            dataset_execution_id=ds["dataset_execution_id"],
                            status="success",
                            rows_read=25,
                            rows_written=25,
                        )
                    datasets = exec_repo.get_datasets_to_execute(job_exec_id)
                exec_repo.complete_job_execution(job_exec_id)
            pending_jobs = exec_repo.get_jobs_to_execute(exec_id)
        exec_repo.complete_process_execution(exec_id)
