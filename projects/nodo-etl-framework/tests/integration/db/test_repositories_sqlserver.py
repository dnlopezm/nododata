"""SQL Server-specific repository integration tests.

Validates NVARCHAR(MAX) JSON fields, OUTPUT INSERTED behaviour, and other
SQL Server-specific features that are not covered by the common tests.
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


@pytest.fixture
def ss_connection(monkeypatch):
    """Create a SQL Server-only database connection."""
    from nodo_etl.config.settings import Settings
    from nodo_etl.db.connection import MetadataDBConnection

    monkeypatch.setenv("NODO_ETL_DB_TYPE", "sqlserver")
    monkeypatch.setenv("NODO_ETL_SQLSERVER_HOST", "localhost")
    monkeypatch.setenv("NODO_ETL_SQLSERVER_PORT", "1433")
    monkeypatch.setenv("NODO_ETL_SQLSERVER_DB", "nodo_etl_db")
    monkeypatch.setenv("NODO_ETL_SQLSERVER_USER", "sa")
    monkeypatch.setenv("NODO_ETL_SQLSERVER_PASSWORD", "NodoEtl@2024!")

    settings = Settings(_env_file=None)
    conn = MetadataDBConnection(settings)
    yield conn
    conn.close()


@skip_integration
class TestSQLServerNvarchar:
    """Verify that NVARCHAR(MAX) JSON columns work correctly in SQL Server."""

    def test_connection_additional_params_nvarchar(self, ss_connection):
        """Connection additional_params stored as NVARCHAR(MAX) should round-trip."""
        repo = ConnectionRepository(ss_connection)
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
        raw = fetched["additional_params"]
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        assert parsed["ssl"] is True
        assert parsed["timeout"] == 30
        assert parsed["tags"] == ["a", "b"]

    def test_hook_action_config_nvarchar(self, ss_connection):
        """Hook action_config stored as NVARCHAR(MAX) should round-trip."""
        proc_repo = ProcessRepository(ss_connection)
        hook_repo = HookRepository(ss_connection)
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

    def test_output_inserted_returns_all_columns(self, ss_connection):
        """INSERT ... OUTPUT INSERTED.* should return the full row."""
        repo = ProcessRepository(ss_connection)
        result = repo.create({
            "process_name": f"proc_{_uid()}",
            "process_type": "etl",
            "execution_order": 1,
        })
        # OUTPUT INSERTED.* should populate all columns
        assert "id" in result
        assert "created_at" in result
        assert "updated_at" in result
        assert "is_enabled" in result

    def test_nvarchar_null_handling(self, ss_connection):
        """NVARCHAR(MAX) JSON columns should handle None gracefully."""
        repo = ConnectionRepository(ss_connection)
        conn = repo.create({
            "connection_name": f"test_conn_{_uid()}",
            "connection_type": "postgresql",
            "host": "test",
            "port": 5432,
            "environment": "dev",
        })
        fetched = repo.get(conn["id"])
        assert fetched.get("additional_params") is None

    def test_large_json_payload(self, ss_connection):
        """NVARCHAR(MAX) should handle large JSON payloads without truncation."""
        repo = ConnectionRepository(ss_connection)
        # Build a payload that exceeds typical VARCHAR limits
        large = {"keys": [f"item_{i}" for i in range(500)]}
        conn = repo.create({
            "connection_name": f"test_conn_{_uid()}",
            "connection_type": "postgresql",
            "host": "test",
            "port": 5432,
            "environment": "dev",
            "additional_params": json.dumps(large),
        })
        fetched = repo.get(conn["id"])
        raw = fetched["additional_params"]
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        assert len(parsed["keys"]) == 500
