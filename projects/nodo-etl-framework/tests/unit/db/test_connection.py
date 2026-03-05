"""Tests for the MetadataDBConnection (mocked)."""

from unittest.mock import MagicMock, patch

import pytest

from nodo_etl.config.settings import Settings
from nodo_etl.core.enums import DatabaseType
from nodo_etl.db.connection import MetadataDBConnection
from nodo_etl.db.dialect import PostgreSQLDialect, SQLServerDialect


@pytest.fixture
def pg_settings(monkeypatch):
    """PostgreSQL settings."""
    monkeypatch.setenv("NODO_ETL_DB_TYPE", "postgresql")
    monkeypatch.setenv("NODO_ETL_PG_HOST", "pg-host")
    monkeypatch.setenv("NODO_ETL_PG_PORT", "5432")
    monkeypatch.setenv("NODO_ETL_PG_DB", "test_db")
    monkeypatch.setenv("NODO_ETL_PG_USER", "test_user")
    monkeypatch.setenv("NODO_ETL_PG_PASSWORD", "test_pass")
    return Settings()


@pytest.fixture
def ss_settings(monkeypatch):
    """SQL Server settings."""
    monkeypatch.setenv("NODO_ETL_DB_TYPE", "sqlserver")
    monkeypatch.setenv("NODO_ETL_SQLSERVER_HOST", "ss-host")
    monkeypatch.setenv("NODO_ETL_SQLSERVER_PORT", "1433")
    monkeypatch.setenv("NODO_ETL_SQLSERVER_DB", "test_db")
    monkeypatch.setenv("NODO_ETL_SQLSERVER_USER", "sa")
    monkeypatch.setenv("NODO_ETL_SQLSERVER_PASSWORD", "test_pass")
    return Settings()


class TestMetadataDBConnection:
    """Tests for MetadataDBConnection."""

    def test_pg_connection_dialect(self, pg_settings):
        """Test PostgreSQL connection uses PostgreSQL dialect."""
        conn = MetadataDBConnection(pg_settings)
        assert isinstance(conn.dialect, PostgreSQLDialect)

    def test_ss_connection_dialect(self, ss_settings):
        """Test SQL Server connection uses SQL Server dialect."""
        conn = MetadataDBConnection(ss_settings)
        assert isinstance(conn.dialect, SQLServerDialect)

    def test_schema_name(self, pg_settings):
        """Test schema_name comes from settings."""
        conn = MetadataDBConnection(pg_settings)
        assert conn.schema_name == "nodo_etl"

    def test_custom_pool_size(self, pg_settings):
        """Test custom pool size is accepted."""
        conn = MetadataDBConnection(pg_settings, pool_size=10)
        assert conn._pool_size == 10

    def test_default_pool_size(self, pg_settings):
        """Test default pool size is 5."""
        conn = MetadataDBConnection(pg_settings)
        assert conn._pool_size == 5

    def test_repr_pg(self, pg_settings):
        """Test repr shows connection info without password."""
        conn = MetadataDBConnection(pg_settings)
        repr_str = repr(conn)
        assert "postgresql" in repr_str
        assert "pg-host" in repr_str
        assert "test_pass" not in repr_str

    def test_repr_ss(self, ss_settings):
        """Test repr shows SQL Server connection info."""
        conn = MetadataDBConnection(ss_settings)
        repr_str = repr(conn)
        assert "sqlserver" in repr_str
        assert "ss-host" in repr_str

    def test_close_disposes_engine(self, pg_settings):
        """Test close disposes the engine."""
        conn = MetadataDBConnection(pg_settings)
        # Force engine creation by accessing it
        mock_engine = MagicMock()
        conn._engine = mock_engine
        conn._session_factory = MagicMock()

        conn.close()

        mock_engine.dispose.assert_called_once()
        assert conn._engine is None
        assert conn._session_factory is None

    def test_close_without_engine(self, pg_settings):
        """Test close is safe when engine is None."""
        conn = MetadataDBConnection(pg_settings)
        conn.close()  # Should not raise

    @patch("nodo_etl.db.connection.create_engine")
    def test_engine_created_with_pool_settings(self, mock_create_engine, pg_settings):
        """Test engine is created with pool_pre_ping and pool_size."""
        conn = MetadataDBConnection(pg_settings, pool_size=10)
        _ = conn.engine
        mock_create_engine.assert_called_once()
        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs["pool_size"] == 10
        assert call_kwargs["pool_pre_ping"] is True

    @patch("nodo_etl.db.connection.create_engine")
    def test_engine_is_cached(self, mock_create_engine, pg_settings):
        """Test engine is only created once (cached)."""
        conn = MetadataDBConnection(pg_settings)
        _ = conn.engine
        _ = conn.engine
        mock_create_engine.assert_called_once()

    def test_execute_procedure_pg_format(self, pg_settings):
        """Test PostgreSQL procedure call uses SELECT * FROM syntax."""
        conn = MetadataDBConnection(pg_settings)
        mock_engine = MagicMock()
        mock_connect = MagicMock()
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.keys.return_value = ["id"]
        mock_result.fetchall.return_value = [(1,)]
        mock_connect.__enter__ = MagicMock(return_value=mock_connect)
        mock_connect.__exit__ = MagicMock(return_value=False)
        mock_connect.execute.return_value = mock_result
        mock_engine.connect.return_value = mock_connect
        conn._engine = mock_engine

        result = conn.execute_procedure(
            "sp_test", {"param1": "val1"}
        )

        call_args = mock_connect.execute.call_args
        sql_text = str(call_args[0][0].text)
        assert "SELECT * FROM" in sql_text
        assert "sp_test" in sql_text

    def test_execute_procedure_ss_format(self, ss_settings):
        """Test SQL Server procedure call uses EXEC syntax."""
        conn = MetadataDBConnection(ss_settings)
        mock_engine = MagicMock()
        mock_connect = MagicMock()
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.keys.return_value = ["id"]
        mock_result.fetchall.return_value = [(1,)]
        mock_connect.__enter__ = MagicMock(return_value=mock_connect)
        mock_connect.__exit__ = MagicMock(return_value=False)
        mock_connect.execute.return_value = mock_result
        mock_engine.connect.return_value = mock_connect
        conn._engine = mock_engine

        result = conn.execute_procedure(
            "sp_test", {"param1": "val1"}
        )

        call_args = mock_connect.execute.call_args
        sql_text = str(call_args[0][0].text)
        assert "EXEC" in sql_text
        assert "sp_test" in sql_text
