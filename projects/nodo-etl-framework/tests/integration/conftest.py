"""Integration test configuration.

These tests require Docker services to be running:
    docker compose -f docker/docker-compose.yml up -d postgresql sqlserver

Set NODO_ETL_INTEGRATION_TESTS=1 to enable.
"""

import os

import pytest

# Skip integration tests unless explicitly enabled
INTEGRATION_ENABLED = os.environ.get("NODO_ETL_INTEGRATION_TESTS", "0") == "1"

skip_integration = pytest.mark.skipif(
    not INTEGRATION_ENABLED,
    reason="Integration tests disabled. Set NODO_ETL_INTEGRATION_TESTS=1 to enable.",
)


@pytest.fixture(params=["postgresql", "sqlserver"])
def db_type(request):
    """Parametrize tests to run against both database engines."""
    return request.param


@pytest.fixture
def db_connection(db_type, monkeypatch):
    """Create a database connection for the given db_type."""
    from nodo_etl.config.settings import Settings
    from nodo_etl.db.connection import MetadataDBConnection

    monkeypatch.setenv("NODO_ETL_DB_TYPE", db_type)
    if db_type == "postgresql":
        monkeypatch.setenv("NODO_ETL_PG_HOST", "localhost")
        monkeypatch.setenv("NODO_ETL_PG_PORT", "5433")
        monkeypatch.setenv("NODO_ETL_PG_DB", "nodo_etl_db")
        monkeypatch.setenv("NODO_ETL_PG_USER", "nodo_etl")
        monkeypatch.setenv("NODO_ETL_PG_PASSWORD", "nodo_etl_password")
    else:
        monkeypatch.setenv("NODO_ETL_SQLSERVER_HOST", "localhost")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_PORT", "1433")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_DB", "nodo_etl_db")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_USER", "sa")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_PASSWORD", "NodoEtl@2024!")

    settings = Settings(_env_file=None)
    conn = MetadataDBConnection(settings)
    yield conn
    conn.close()
