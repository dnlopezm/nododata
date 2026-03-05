"""Docker validation tests.

Verify Docker services are reachable and properly configured.
Requires Docker services. Enable with NODO_ETL_INTEGRATION_TESTS=1.
"""
import socket

import pytest

from tests.integration.conftest import skip_integration


def _port_is_open(host: str, port: int, timeout: float = 2.0) -> bool:
    """Check if a port is open on the given host."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, TimeoutError, OSError):
        return False


@skip_integration
class TestDockerServices:
    """Verify Docker services are running and reachable."""

    def test_postgresql_reachable(self):
        """PostgreSQL container is reachable on port 5433."""
        assert _port_is_open("localhost", 5433), "PostgreSQL not reachable on port 5433"

    def test_sqlserver_reachable(self):
        """SQL Server container is reachable on port 1433."""
        assert _port_is_open("localhost", 1433), "SQL Server not reachable on port 1433"

    def test_airflow_webserver_reachable(self):
        """Airflow webserver is reachable on port 8080."""
        assert _port_is_open("localhost", 8080), "Airflow webserver not reachable on port 8080"


@skip_integration
class TestPostgreSQLSchema:
    """Verify PostgreSQL has the nodo_etl schema."""

    def test_schema_exists(self, db_connection):
        """The nodo_etl schema exists in PostgreSQL."""
        # db_connection is parametrized; filter for postgresql only
        if hasattr(db_connection, 'dialect') and db_connection.dialect.dialect_type.value != "postgresql":
            pytest.skip("PostgreSQL only")
        rows = db_connection.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :name",
            {"name": "nodo_etl"},
        )
        assert len(rows) == 1

    def test_tables_exist(self, db_connection):
        """All 17 ETL tables exist in PostgreSQL."""
        if hasattr(db_connection, 'dialect') and db_connection.dialect.dialect_type.value != "postgresql":
            pytest.skip("PostgreSQL only")
        expected_tables = [
            "etl_process", "etl_schedule", "etl_connection", "etl_job",
            "etl_dataset", "etl_dataset_db_config", "etl_dataset_file_config",
            "etl_dataset_api_config", "etl_dataset_stream_config",
            "etl_process_execution", "etl_job_execution", "etl_dataset_execution",
            "etl_watermark", "etl_hook", "etl_tag", "etl_dataset_lineage",
            "etl_system_config",
        ]
        rows = db_connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = :schema",
            {"schema": "nodo_etl"},
        )
        actual = {r["table_name"] for r in rows}
        for table in expected_tables:
            assert table in actual, f"Table {table} not found"


@skip_integration
class TestSQLServerSchema:
    """Verify SQL Server has the nodo_etl schema."""

    def test_schema_exists(self, db_connection):
        """The nodo_etl schema exists in SQL Server."""
        if hasattr(db_connection, 'dialect') and db_connection.dialect.dialect_type.value != "sqlserver":
            pytest.skip("SQL Server only")
        rows = db_connection.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :name",
            {"name": "nodo_etl"},
        )
        assert len(rows) == 1

    def test_tables_exist(self, db_connection):
        """All 17 ETL tables exist in SQL Server."""
        if hasattr(db_connection, 'dialect') and db_connection.dialect.dialect_type.value != "sqlserver":
            pytest.skip("SQL Server only")
        expected_tables = [
            "etl_process", "etl_schedule", "etl_connection", "etl_job",
            "etl_dataset", "etl_dataset_db_config", "etl_dataset_file_config",
            "etl_dataset_api_config", "etl_dataset_stream_config",
            "etl_process_execution", "etl_job_execution", "etl_dataset_execution",
            "etl_watermark", "etl_hook", "etl_tag", "etl_dataset_lineage",
            "etl_system_config",
        ]
        rows = db_connection.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :schema",
            {"schema": "nodo_etl"},
        )
        actual = {r["TABLE_NAME"] for r in rows}
        for table in expected_tables:
            assert table in actual, f"Table {table} not found"
