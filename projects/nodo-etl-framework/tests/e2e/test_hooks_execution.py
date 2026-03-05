"""E2E tests: Hook metadata and querying.

Tests hook creation, listing, ordering, filtering by on_status,
and disabled hook exclusion. These tests verify the metadata/SP-level
behavior -- actual hook execution (HTTP, SQL, commands) is tested
in the Airflow DAG files.

Requires NODO_ETL_E2E_TESTS=1 to enable.
"""

import pytest

from tests.e2e.conftest import skip_e2e


@skip_e2e
class TestHooksExecution:
    """Hook metadata storage and querying."""

    def test_create_and_list_hooks(self, repos, unique_id, sample_process):
        """Create hooks and verify they can be listed."""
        hook_repo = repos["hook"]
        process_id = sample_process["id"]

        hook1 = hook_repo.create({
            "entity_type": "process",
            "entity_id": process_id,
            "hook_type": "pre",
            "action_type": "sql",
            "action_config": '{"query": "SELECT 1"}',
            "execution_order": 1,
            "hook_name": f"e2e_hook1_{unique_id}",
        })
        assert hook1.get("id") is not None

        hook2 = hook_repo.create({
            "entity_type": "process",
            "entity_id": process_id,
            "hook_type": "post",
            "action_type": "api",
            "action_config": '{"url": "https://notify.example.com"}',
            "execution_order": 2,
            "hook_name": f"e2e_hook2_{unique_id}",
        })
        assert hook2.get("id") is not None

        hooks = hook_repo.list(entity_type="process", entity_id=process_id)
        hook_names = [h.get("hook_name", "") for h in hooks]
        assert f"e2e_hook1_{unique_id}" in hook_names
        assert f"e2e_hook2_{unique_id}" in hook_names

    def test_pre_hook_on_job(self, repos, unique_id, full_pipeline):
        """Pre-hook on a job: verify hook exists for that entity."""
        hook_repo = repos["hook"]
        job_id = full_pipeline["jobs"][0]["id"]

        hook = hook_repo.create({
            "entity_type": "job",
            "entity_id": job_id,
            "hook_type": "pre",
            "action_type": "command",
            "action_config": '{"command": "echo starting"}',
            "execution_order": 1,
            "hook_name": f"e2e_pre_job_{unique_id}",
        })
        assert hook.get("id") is not None

        hooks = hook_repo.list(entity_type="job", entity_id=job_id)
        assert len(hooks) >= 1
        pre_hooks = [h for h in hooks if h.get("hook_type") == "pre"]
        assert len(pre_hooks) >= 1

        hook_names = [h.get("hook_name", "") for h in pre_hooks]
        assert f"e2e_pre_job_{unique_id}" in hook_names

    def test_post_hook_on_status_filtering(self, repos, unique_id, sample_process):
        """Post-hooks with on_status='success' only listed when queried.

        Verify hooks with different on_status values are stored correctly.
        """
        hook_repo = repos["hook"]
        process_id = sample_process["id"]

        # Hook that fires on success
        hook_repo.create({
            "entity_type": "process",
            "entity_id": process_id,
            "hook_type": "post",
            "action_type": "api",
            "action_config": '{"url": "https://success.example.com"}',
            "execution_order": 1,
            "on_status": "success",
            "hook_name": f"e2e_on_success_{unique_id}",
        })

        # Hook that fires on failure
        hook_repo.create({
            "entity_type": "process",
            "entity_id": process_id,
            "hook_type": "post",
            "action_type": "api",
            "action_config": '{"url": "https://failure.example.com"}',
            "execution_order": 2,
            "on_status": "failed",
            "hook_name": f"e2e_on_failed_{unique_id}",
        })

        # Hook that fires on any status
        hook_repo.create({
            "entity_type": "process",
            "entity_id": process_id,
            "hook_type": "post",
            "action_type": "sql",
            "action_config": '{"query": "INSERT INTO audit_log VALUES (1)"}',
            "execution_order": 3,
            "on_status": "any",
            "hook_name": f"e2e_on_any_{unique_id}",
        })

        # List all hooks for this process
        all_hooks = hook_repo.list(entity_type="process", entity_id=process_id)
        our_hooks = [
            h for h in all_hooks
            if unique_id in h.get("hook_name", "")
        ]

        # All 3 should be listed
        assert len(our_hooks) == 3

        # Verify on_status values are correctly stored
        on_status_values = {h["hook_name"]: h.get("on_status") for h in our_hooks}
        assert on_status_values[f"e2e_on_success_{unique_id}"] == "success"
        assert on_status_values[f"e2e_on_failed_{unique_id}"] == "failed"
        assert on_status_values[f"e2e_on_any_{unique_id}"] == "any"

    def test_multiple_hooks_execution_order(self, repos, unique_id, full_pipeline):
        """Multiple hooks with execution_order: verify ordering is preserved."""
        hook_repo = repos["hook"]
        job_id = full_pipeline["jobs"][1]["id"]  # silver job

        # Create hooks in reverse order
        hook_repo.create({
            "entity_type": "job",
            "entity_id": job_id,
            "hook_type": "pre",
            "action_type": "sql",
            "action_config": '{"query": "SELECT 3"}',
            "execution_order": 3,
            "hook_name": f"e2e_order3_{unique_id}",
        })
        hook_repo.create({
            "entity_type": "job",
            "entity_id": job_id,
            "hook_type": "pre",
            "action_type": "sql",
            "action_config": '{"query": "SELECT 1"}',
            "execution_order": 1,
            "hook_name": f"e2e_order1_{unique_id}",
        })
        hook_repo.create({
            "entity_type": "job",
            "entity_id": job_id,
            "hook_type": "pre",
            "action_type": "sql",
            "action_config": '{"query": "SELECT 2"}',
            "execution_order": 2,
            "hook_name": f"e2e_order2_{unique_id}",
        })

        hooks = hook_repo.list(entity_type="job", entity_id=job_id)
        our_hooks = [
            h for h in hooks
            if unique_id in h.get("hook_name", "")
        ]

        # Should be ordered by execution_order
        assert len(our_hooks) == 3
        orders = [h["execution_order"] for h in our_hooks]
        assert orders == sorted(orders)

        # Verify names match expected order
        assert our_hooks[0]["hook_name"] == f"e2e_order1_{unique_id}"
        assert our_hooks[1]["hook_name"] == f"e2e_order2_{unique_id}"
        assert our_hooks[2]["hook_name"] == f"e2e_order3_{unique_id}"

    def test_disabled_hook_not_in_default_list(self, repos, unique_id, sample_process):
        """Disabled hook should not be returned in the default list."""
        hook_repo = repos["hook"]
        process_id = sample_process["id"]

        # Create an enabled hook
        hook_repo.create({
            "entity_type": "process",
            "entity_id": process_id,
            "hook_type": "pre",
            "action_type": "sql",
            "action_config": '{"query": "SELECT 1"}',
            "execution_order": 1,
            "hook_name": f"e2e_enabled_hook_{unique_id}",
            "is_enabled": True,
        })

        # Create a disabled hook
        hook_repo.create({
            "entity_type": "process",
            "entity_id": process_id,
            "hook_type": "pre",
            "action_type": "sql",
            "action_config": '{"query": "SELECT 0"}',
            "execution_order": 2,
            "hook_name": f"e2e_disabled_hook_{unique_id}",
            "is_enabled": False,
        })

        # Default list (include_disabled=False)
        hooks = hook_repo.list(entity_type="process", entity_id=process_id)
        hook_names = [h.get("hook_name", "") for h in hooks]
        assert f"e2e_enabled_hook_{unique_id}" in hook_names
        assert f"e2e_disabled_hook_{unique_id}" not in hook_names

        # List with include_disabled=True
        all_hooks = hook_repo.list(
            entity_type="process",
            entity_id=process_id,
            include_disabled=True,
        )
        all_hook_names = [h.get("hook_name", "") for h in all_hooks]
        assert f"e2e_enabled_hook_{unique_id}" in all_hook_names
        assert f"e2e_disabled_hook_{unique_id}" in all_hook_names
