"""PostgreSQL-specific repository integration tests.

Validates JSONB fields, RETURNING clause behaviour, and other
PostgreSQL-specific features that are not covered by the common tests.
"""

import json
import os
import uuid

import pytest

from tests.integration.conftest import skip_integration

from nodo_etl.db.repositories import (
    ConnectionRepository,
    HookRepository,
    ProcessRepository,
)


def _uid() -> str:
    return uuid.uuid4().hex[:8]


_skip_not_pg = pytest.mark.skipif(
    os.environ.get("NODO_ETL_INTEGRATION_TESTS", "0") != "1",
    reason="Integration tests disabled. Set NODO_ETL_INTEGRATION_TESTS=1 to enable.",
)


@pytest.fixture
def pg_connection(monkeypatch):
    """Create a PostgreSQL-only database connection."""
    from nodo_etl.config.settings import Settings
    from nodo_etl.db.connection import MetadataDBConnection

    monkeypatch.setenv("NODO_ETL_DB_TYPE", "postgresql")
    monkeypatch.setenv("NODO_ETL_PG_HOST", "localhost")
    monkeypatch.setenv("NODO_ETL_PG_PORT", "5433")
    monkeypatch.setenv("NODO_ETL_PG_DB", "nodo_etl_db")
    monkeypatch.setenv("NODO_ETL_PG_USER", "nodo_etl")
    monkeypatch.setenv("NODO_ETL_PG_PASSWORD", "nodo_etl_password")

    settings = Settings(_env_file=None)
    conn = MetadataDBConnection(settings)
    yield conn
    conn.close()


@skip_integration
class TestPostgreSQLJsonb:
    """Verify that JSONB columns work correctly in PostgreSQL."""

    def test_connection_additional_params_jsonb(self, pg_connection):
        """Connection additional_params should round-trip as JSONB."""
        repo = ConnectionRepository(pg_connection)
        extra = {"ssl": True, "timeout": 30, "tags": ["a", "b"]}
        conn = repo.create({
            "connection_name": f"test_conn_{_uid()}",
            "connection_type": "postgresql",
            "host": "test",
            "port": 5432,
            "environment": "dev",
            "additional_params": json.dumps(extra),
        })
        assert conn["id"] > 0
        fetched = repo.get(conn["id"])
        # Depending on driver, value may come back as str or dict
        raw = fetched["additional_params"]
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        assert parsed["ssl"] is True
        assert parsed["timeout"] == 30
        assert parsed["tags"] == ["a", "b"]

    def test_hook_action_config_jsonb(self, pg_connection):
        """Hook action_config should round-trip as JSONB."""
        proc_repo = ProcessRepository(pg_connection)
        hook_repo = HookRepository(pg_connection)
        proc = proc_repo.create({
            "process_name": f"proc_{_uid()}",
            "process_type": "etl",
            "execution_order": 1,
        })
        config = {"sql": "SELECT 1", "retries": 3}
        hook = hook_repo.create({
            "entity_type": "process",
            "entity_id": proc["id"],
            "hook_type": "pre",
            "action_type": "sql",
            "execution_order": 1,
            "action_config": json.dumps(config),
        })
        assert hook["id"] > 0
        fetched = hook_repo.list("process", proc["id"], include_disabled=True)
        match = [h for h in fetched if h["id"] == hook["id"]][0]
        raw = match["action_config"]
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        assert parsed["retries"] == 3

    def test_returning_clause_returns_all_columns(self, pg_connection):
        """INSERT ... RETURNING * should return the full row including id."""
        repo = ProcessRepository(pg_connection)
        result = repo.create({
            "process_name": f"proc_{_uid()}",
            "process_type": "etl",
            "execution_order": 1,
        })
        # The RETURNING clause should populate all columns
        assert "id" in result
        assert "created_at" in result
        assert "updated_at" in result
        assert "is_enabled" in result

    def test_jsonb_null_handling(self, pg_connection):
        """JSONB columns should handle None gracefully."""
        repo = ConnectionRepository(pg_connection)
        conn = repo.create({
            "connection_name": f"test_conn_{_uid()}",
            "connection_type": "postgresql",
            "host": "test",
            "port": 5432,
            "environment": "dev",
        })
        fetched = repo.get(conn["id"])
        # additional_params should be None/null when not provided
        assert fetched.get("additional_params") is None
