"""Common repository integration tests — run against both PostgreSQL and SQL Server.

These tests exercise the full CRUD lifecycle for all repository classes
using the parametrized ``db_connection`` fixture from conftest.py.
"""

import uuid

import pytest

from tests.integration.conftest import skip_integration

from nodo_etl.db.repositories import (
    ConnectionRepository,
    DatasetRepository,
    DuplicateError,
    ExecutionRepository,
    HookRepository,
    JobRepository,
    LineageRepository,
    NotFoundError,
    ProcessRepository,
    RepositoryError,
    SystemConfigRepository,
    TagRepository,
)


def _uid() -> str:
    """Return a short unique suffix for test data."""
    return uuid.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------

def _create_process(repo: ProcessRepository, **overrides) -> dict:
    """Create a process with sensible defaults."""
    data = {
        "process_name": f"proc_{_uid()}",
        "process_type": "etl",
        "execution_order": 1,
    }
    data.update(overrides)
    return repo.create(data)


def _create_job(repo: JobRepository, process_id: int, **overrides) -> dict:
    data = {
        "process_id": process_id,
        "job_name": f"job_{_uid()}",
        "job_type": "extract",
        "execution_order": 1,
    }
    data.update(overrides)
    return repo.create(data)


def _create_connection(repo: ConnectionRepository, **overrides) -> dict:
    data = {
        "connection_name": f"test_conn_{_uid()}",
        "connection_type": "postgresql",
        "host": "test",
        "port": 5432,
        "environment": "dev",
    }
    data.update(overrides)
    return repo.create(data)


def _create_dataset(
    ds_repo: DatasetRepository,
    job_id: int,
    connection_id: int,
    **overrides,
) -> dict:
    data = {
        "job_id": job_id,
        "dataset_name": f"ds_{_uid()}",
        "source_type": "database",
        "layer": "bronze",
        "load_strategy": "full",
        "idempotency_strategy": "overwrite",
        "execution_order": 1,
        "connection_id": connection_id,
    }
    data.update(overrides)
    source_config = overrides.pop("source_config", {
        "source_schema": "test",
        "source_table": "tbl",
    })
    return ds_repo.create(data, source_config=source_config)


# ============================================================================
# ProcessRepository
# ============================================================================

@skip_integration
class TestProcessRepository:

    def test_create_process_all_fields(self, db_connection):
        repo = ProcessRepository(db_connection)
        result = _create_process(repo, description="full process")
        assert result["id"] is not None
        assert result["id"] > 0
        assert result["process_name"].startswith("proc_")
        assert result["description"] == "full process"

    def test_create_process_defaults(self, db_connection):
        repo = ProcessRepository(db_connection)
        result = _create_process(repo)
        assert result["is_enabled"] is True or result["is_enabled"] == 1
        assert result["is_deleted"] is False or result["is_deleted"] == 0

    def test_get_process_by_id(self, db_connection):
        repo = ProcessRepository(db_connection)
        created = _create_process(repo)
        fetched = repo.get(created["id"])
        assert fetched["id"] == created["id"]
        assert fetched["process_name"] == created["process_name"]

    def test_get_process_not_found(self, db_connection):
        repo = ProcessRepository(db_connection)
        with pytest.raises(NotFoundError):
            repo.get(999_999_999)

    def test_list_processes_returns_non_deleted(self, db_connection):
        repo = ProcessRepository(db_connection)
        p = _create_process(repo)
        results = repo.list()
        ids = [r["id"] for r in results]
        assert p["id"] in ids

    def test_list_processes_filter_is_enabled(self, db_connection):
        repo = ProcessRepository(db_connection)
        enabled = _create_process(repo, is_enabled=True)
        disabled = _create_process(repo, is_enabled=False)
        enabled_list = repo.list(is_enabled=True)
        disabled_list = repo.list(is_enabled=False)
        enabled_ids = [r["id"] for r in enabled_list]
        disabled_ids = [r["id"] for r in disabled_list]
        assert enabled["id"] in enabled_ids
        assert disabled["id"] in disabled_ids
        assert disabled["id"] not in enabled_ids

    def test_update_process(self, db_connection):
        repo = ProcessRepository(db_connection)
        created = _create_process(repo)
        updated = repo.update(created["id"], {"description": "updated"})
        assert updated["description"] == "updated"
        assert updated["updated_at"] is not None

    def test_delete_process_soft(self, db_connection):
        repo = ProcessRepository(db_connection)
        created = _create_process(repo)
        repo.delete(created["id"])
        with pytest.raises(NotFoundError):
            repo.get(created["id"])

    def test_delete_process_cascades(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)
        conn_repo = ConnectionRepository(db_connection)

        proc = _create_process(proc_repo)
        job = _create_job(job_repo, proc["id"])
        conn = _create_connection(conn_repo)
        _create_dataset(ds_repo, job["id"], conn["id"])

        proc_repo.delete(proc["id"])

        with pytest.raises(NotFoundError):
            job_repo.get(job["id"])

        assert ds_repo.list(job["id"]) == []


# ============================================================================
# JobRepository
# ============================================================================

@skip_integration
class TestJobRepository:

    def test_create_job(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        proc = _create_process(proc_repo)
        job = _create_job(job_repo, proc["id"])
        assert job["id"] > 0
        assert job["process_id"] == proc["id"]

    def test_get_job_by_id(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        proc = _create_process(proc_repo)
        job = _create_job(job_repo, proc["id"])
        fetched = job_repo.get(job["id"])
        assert fetched["id"] == job["id"]

    def test_list_jobs_ordered_by_execution_order(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        proc = _create_process(proc_repo)
        j2 = _create_job(job_repo, proc["id"], execution_order=2)
        j1 = _create_job(job_repo, proc["id"], execution_order=1)
        jobs = job_repo.list(proc["id"])
        ids = [j["id"] for j in jobs]
        assert ids.index(j1["id"]) < ids.index(j2["id"])

    def test_list_jobs_filters_soft_deleted(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        proc = _create_process(proc_repo)
        job = _create_job(job_repo, proc["id"])
        job_repo.delete(job["id"])
        jobs = job_repo.list(proc["id"])
        ids = [j["id"] for j in jobs]
        assert job["id"] not in ids

    def test_update_job(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        proc = _create_process(proc_repo)
        job = _create_job(job_repo, proc["id"])
        updated = job_repo.update(job["id"], {"job_name": "renamed_job"})
        assert updated["job_name"] == "renamed_job"

    def test_delete_job_cascades_to_datasets(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)
        conn_repo = ConnectionRepository(db_connection)

        proc = _create_process(proc_repo)
        job = _create_job(job_repo, proc["id"])
        conn = _create_connection(conn_repo)
        _create_dataset(ds_repo, job["id"], conn["id"])

        job_repo.delete(job["id"])
        assert ds_repo.list(job["id"]) == []


# ============================================================================
# DatasetRepository
# ============================================================================

@skip_integration
class TestDatasetRepository:

    def _setup(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        conn_repo = ConnectionRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)
        proc = _create_process(proc_repo)
        job = _create_job(job_repo, proc["id"])
        conn = _create_connection(conn_repo)
        return ds_repo, job, conn

    def test_create_dataset_db_source(self, db_connection):
        ds_repo, job, conn = self._setup(db_connection)
        ds = _create_dataset(ds_repo, job["id"], conn["id"])
        assert ds["id"] > 0
        assert ds["source_type"] == "database"

    def test_create_dataset_file_source(self, db_connection):
        ds_repo, job, conn = self._setup(db_connection)
        data = {
            "job_id": job["id"],
            "dataset_name": f"ds_file_{_uid()}",
            "source_type": "file",
            "layer": "bronze",
            "load_strategy": "full",
            "idempotency_strategy": "overwrite",
            "execution_order": 1,
            "connection_id": conn["id"],
        }
        source_config = {
            "file_path": "/data/input.csv",
            "file_format": "csv",
        }
        ds = ds_repo.create(data, source_config=source_config)
        assert ds["id"] > 0
        assert ds["source_type"] == "file"

    def test_create_dataset_api_source(self, db_connection):
        ds_repo, job, conn = self._setup(db_connection)
        data = {
            "job_id": job["id"],
            "dataset_name": f"ds_api_{_uid()}",
            "source_type": "api",
            "layer": "bronze",
            "load_strategy": "full",
            "idempotency_strategy": "overwrite",
            "execution_order": 1,
            "connection_id": conn["id"],
        }
        source_config = {
            "endpoint_url": "https://api.example.com/data",
            "http_method": "GET",
        }
        ds = ds_repo.create(data, source_config=source_config)
        assert ds["id"] > 0
        assert ds["source_type"] == "api"

    def test_get_dataset_with_source_config(self, db_connection):
        ds_repo, job, conn = self._setup(db_connection)
        ds = _create_dataset(ds_repo, job["id"], conn["id"])
        fetched = ds_repo.get(ds["id"])
        assert fetched["id"] == ds["id"]
        assert "source_config" in fetched
        assert fetched["source_config"]["source_schema"] == "test"

    def test_list_datasets_by_job_id(self, db_connection):
        ds_repo, job, conn = self._setup(db_connection)
        d2 = _create_dataset(ds_repo, job["id"], conn["id"], execution_order=2)
        d1 = _create_dataset(ds_repo, job["id"], conn["id"], execution_order=1)
        datasets = ds_repo.list(job["id"])
        ids = [d["id"] for d in datasets]
        assert ids.index(d1["id"]) < ids.index(d2["id"])

    def test_update_dataset(self, db_connection):
        ds_repo, job, conn = self._setup(db_connection)
        ds = _create_dataset(ds_repo, job["id"], conn["id"])
        updated = ds_repo.update(ds["id"], {"layer": "silver"})
        assert updated["layer"] == "silver"

    def test_delete_dataset_soft(self, db_connection):
        ds_repo, job, conn = self._setup(db_connection)
        ds = _create_dataset(ds_repo, job["id"], conn["id"])
        ds_repo.delete(ds["id"])
        with pytest.raises(NotFoundError):
            ds_repo.get(ds["id"])


# ============================================================================
# ConnectionRepository
# ============================================================================

@skip_integration
class TestConnectionRepository:

    def test_create_connection(self, db_connection):
        repo = ConnectionRepository(db_connection)
        conn = _create_connection(repo)
        assert conn["id"] > 0
        assert conn["connection_type"] == "postgresql"

    def test_get_connection(self, db_connection):
        repo = ConnectionRepository(db_connection)
        conn = _create_connection(repo)
        fetched = repo.get(conn["id"])
        assert fetched["id"] == conn["id"]
        assert fetched["connection_name"] == conn["connection_name"]

    def test_list_filtered_by_type(self, db_connection):
        repo = ConnectionRepository(db_connection)
        pg = _create_connection(repo, connection_type="postgresql")
        ss = _create_connection(repo, connection_type="sqlserver")
        pg_list = repo.list(connection_type="postgresql")
        pg_ids = [c["id"] for c in pg_list]
        assert pg["id"] in pg_ids
        assert ss["id"] not in pg_ids

    def test_list_filtered_by_environment(self, db_connection):
        repo = ConnectionRepository(db_connection)
        dev = _create_connection(repo, environment="dev")
        prod = _create_connection(repo, environment="prod")
        dev_list = repo.list(environment="dev")
        dev_ids = [c["id"] for c in dev_list]
        assert dev["id"] in dev_ids
        assert prod["id"] not in dev_ids

    def test_update_connection(self, db_connection):
        repo = ConnectionRepository(db_connection)
        conn = _create_connection(repo)
        updated = repo.update(conn["id"], {"host": "new_host"})
        assert updated["host"] == "new_host"

    def test_delete_connection_soft(self, db_connection):
        repo = ConnectionRepository(db_connection)
        conn = _create_connection(repo)
        repo.delete(conn["id"])
        with pytest.raises(NotFoundError):
            repo.get(conn["id"])


# ============================================================================
# HookRepository
# ============================================================================

@skip_integration
class TestHookRepository:

    def test_create_hook_for_process(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        hook_repo = HookRepository(db_connection)
        proc = _create_process(proc_repo)
        hook = hook_repo.create({
            "entity_type": "process",
            "entity_id": proc["id"],
            "hook_type": "pre",
            "action_type": "sql",
            "execution_order": 1,
        })
        assert hook["id"] > 0

    def test_list_hooks_by_entity_ordered(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        hook_repo = HookRepository(db_connection)
        proc = _create_process(proc_repo)
        h2 = hook_repo.create({
            "entity_type": "process",
            "entity_id": proc["id"],
            "hook_type": "pre",
            "action_type": "sql",
            "execution_order": 2,
        })
        h1 = hook_repo.create({
            "entity_type": "process",
            "entity_id": proc["id"],
            "hook_type": "pre",
            "action_type": "sql",
            "execution_order": 1,
        })
        hooks = hook_repo.list("process", proc["id"])
        ids = [h["id"] for h in hooks]
        assert ids.index(h1["id"]) < ids.index(h2["id"])

    def test_list_hooks_filters_disabled(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        hook_repo = HookRepository(db_connection)
        proc = _create_process(proc_repo)
        enabled = hook_repo.create({
            "entity_type": "process",
            "entity_id": proc["id"],
            "hook_type": "pre",
            "action_type": "sql",
            "execution_order": 1,
            "is_enabled": True,
        })
        disabled = hook_repo.create({
            "entity_type": "process",
            "entity_id": proc["id"],
            "hook_type": "pre",
            "action_type": "sql",
            "execution_order": 2,
            "is_enabled": False,
        })
        hooks = hook_repo.list("process", proc["id"])
        ids = [h["id"] for h in hooks]
        assert enabled["id"] in ids
        assert disabled["id"] not in ids

    def test_update_hook(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        hook_repo = HookRepository(db_connection)
        proc = _create_process(proc_repo)
        hook = hook_repo.create({
            "entity_type": "process",
            "entity_id": proc["id"],
            "hook_type": "pre",
            "action_type": "sql",
            "execution_order": 1,
        })
        updated = hook_repo.update(hook["id"], {"action_type": "http"})
        assert updated["action_type"] == "http"

    def test_delete_hook_soft(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        hook_repo = HookRepository(db_connection)
        proc = _create_process(proc_repo)
        hook = hook_repo.create({
            "entity_type": "process",
            "entity_id": proc["id"],
            "hook_type": "pre",
            "action_type": "sql",
            "execution_order": 1,
        })
        hook_repo.delete(hook["id"])
        hooks = hook_repo.list("process", proc["id"])
        ids = [h["id"] for h in hooks]
        assert hook["id"] not in ids


# ============================================================================
# TagRepository
# ============================================================================

@skip_integration
class TestTagRepository:

    def test_add_tag(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        tag_repo = TagRepository(db_connection)
        proc = _create_process(proc_repo)
        tag = tag_repo.add({
            "entity_type": "process",
            "entity_id": proc["id"],
            "tag_key": "team",
            "tag_value": "data-eng",
        })
        assert tag["id"] > 0

    def test_list_tags_by_entity(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        tag_repo = TagRepository(db_connection)
        proc = _create_process(proc_repo)
        tag_repo.add({
            "entity_type": "process",
            "entity_id": proc["id"],
            "tag_key": "env",
            "tag_value": "dev",
        })
        tag_repo.add({
            "entity_type": "process",
            "entity_id": proc["id"],
            "tag_key": "team",
            "tag_value": "data-eng",
        })
        tags = tag_repo.list("process", proc["id"])
        assert len(tags) >= 2
        keys = [t["tag_key"] for t in tags]
        assert "env" in keys
        assert "team" in keys

    def test_remove_tag(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        tag_repo = TagRepository(db_connection)
        proc = _create_process(proc_repo)
        tag = tag_repo.add({
            "entity_type": "process",
            "entity_id": proc["id"],
            "tag_key": "temp",
            "tag_value": "yes",
        })
        tag_repo.remove(tag["id"])
        tags = tag_repo.list("process", proc["id"])
        ids = [t["id"] for t in tags]
        assert tag["id"] not in ids


# ============================================================================
# LineageRepository
# ============================================================================

@skip_integration
class TestLineageRepository:

    def _make_dataset(self, db_connection, **overrides):
        proc_repo = ProcessRepository(db_connection)
        job_repo = JobRepository(db_connection)
        conn_repo = ConnectionRepository(db_connection)
        ds_repo = DatasetRepository(db_connection)
        proc = _create_process(proc_repo)
        job = _create_job(job_repo, proc["id"])
        conn = _create_connection(conn_repo)
        return _create_dataset(ds_repo, job["id"], conn["id"], **overrides)

    def test_add_lineage(self, db_connection):
        lineage_repo = LineageRepository(db_connection)
        src = self._make_dataset(db_connection)
        tgt = self._make_dataset(db_connection)
        link = lineage_repo.add({
            "source_dataset_id": src["id"],
            "target_dataset_id": tgt["id"],
            "lineage_type": "direct",
        })
        assert link["id"] > 0

    def test_get_upstream(self, db_connection):
        lineage_repo = LineageRepository(db_connection)
        src = self._make_dataset(db_connection)
        tgt = self._make_dataset(db_connection)
        lineage_repo.add({
            "source_dataset_id": src["id"],
            "target_dataset_id": tgt["id"],
            "lineage_type": "direct",
        })
        upstream = lineage_repo.get_upstream(tgt["id"])
        src_ids = [u["source_dataset_id"] for u in upstream]
        assert src["id"] in src_ids

    def test_get_downstream(self, db_connection):
        lineage_repo = LineageRepository(db_connection)
        src = self._make_dataset(db_connection)
        tgt = self._make_dataset(db_connection)
        lineage_repo.add({
            "source_dataset_id": src["id"],
            "target_dataset_id": tgt["id"],
            "lineage_type": "direct",
        })
        downstream = lineage_repo.get_downstream(src["id"])
        tgt_ids = [d["target_dataset_id"] for d in downstream]
        assert tgt["id"] in tgt_ids


# ============================================================================
# SystemConfigRepository
# ============================================================================

@skip_integration
class TestSystemConfigRepository:

    def test_get_config_by_key(self, db_connection):
        repo = SystemConfigRepository(db_connection)
        key = f"cfg_{_uid()}"
        repo.set(key, "val1")
        result = repo.get(key)
        assert result["config_value"] == "val1"

    def test_get_config_not_found(self, db_connection):
        repo = SystemConfigRepository(db_connection)
        with pytest.raises(NotFoundError):
            repo.get(f"nonexistent_{_uid()}")

    def test_set_config_update(self, db_connection):
        repo = SystemConfigRepository(db_connection)
        key = f"cfg_{_uid()}"
        repo.set(key, "original")
        repo.set(key, "updated")
        result = repo.get(key)
        assert result["config_value"] == "updated"

    def test_set_config_new(self, db_connection):
        repo = SystemConfigRepository(db_connection)
        key = f"cfg_{_uid()}"
        result = repo.set(key, "brand_new")
        assert result["config_value"] == "brand_new"

    def test_list_all(self, db_connection):
        repo = SystemConfigRepository(db_connection)
        key = f"cfg_{_uid()}"
        repo.set(key, "listed")
        entries = repo.list()
        assert len(entries) > 0
        keys = [e["config_key"] for e in entries]
        assert key in keys


# ============================================================================
# ExecutionRepository
# ============================================================================

@skip_integration
class TestExecutionRepository:

    def test_start_process_execution(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        exec_repo = ExecutionRepository(db_connection)
        proc = _create_process(proc_repo)
        exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="dev",
            triggered_by="test",
        )
        assert exec_id > 0

    def test_complete_process_execution(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        exec_repo = ExecutionRepository(db_connection)
        proc = _create_process(proc_repo)
        exec_id = exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="dev",
            triggered_by="test",
        )
        # Should not raise
        exec_repo.complete_process_execution(exec_id)

    def test_get_execution_summary(self, db_connection):
        proc_repo = ProcessRepository(db_connection)
        exec_repo = ExecutionRepository(db_connection)
        proc = _create_process(proc_repo)
        exec_repo.start_process_execution(
            process_id=proc["id"],
            environment="dev",
            triggered_by="test",
        )
        results = exec_repo.get_execution_summary(process_id=proc["id"])
        assert isinstance(results, list)
