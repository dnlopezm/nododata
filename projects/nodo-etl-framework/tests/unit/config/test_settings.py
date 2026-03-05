"""Tests for settings loading and validation."""

import os
import tempfile

import pytest
from pydantic import ValidationError

from nodo_etl.config.settings import Settings, load_settings
from nodo_etl.core.enums import DatabaseType, Environment


class TestSettingsDefaults:
    def test_default_db_type(self, monkeypatch):
        monkeypatch.delenv("NODO_ETL_DB_TYPE", raising=False)
        s = Settings(_env_file=None)
        assert s.db_type == DatabaseType.POSTGRESQL

    def test_default_schema(self, monkeypatch):
        monkeypatch.delenv("NODO_ETL_SCHEMA", raising=False)
        s = Settings(_env_file=None)
        assert s.schema_name == "nodo_etl"

    def test_default_environment(self, monkeypatch):
        monkeypatch.delenv("NODO_ETL_ENVIRONMENT", raising=False)
        s = Settings(_env_file=None)
        assert s.environment == Environment.DEV

    def test_default_pg_port(self, monkeypatch):
        monkeypatch.delenv("NODO_ETL_PG_PORT", raising=False)
        s = Settings(_env_file=None)
        assert s.pg_port == 5433

    def test_default_sqlserver_port(self, monkeypatch):
        monkeypatch.delenv("NODO_ETL_SQLSERVER_PORT", raising=False)
        s = Settings(_env_file=None)
        assert s.sqlserver_port == 1433


class TestSettingsFromEnv:
    def test_load_from_env_vars(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_DB_TYPE", "sqlserver")
        monkeypatch.setenv("NODO_ETL_ENVIRONMENT", "prod")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_HOST", "db.example.com")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_PORT", "1434")
        s = Settings(_env_file=None)
        assert s.db_type == DatabaseType.SQLSERVER
        assert s.environment == Environment.PROD
        assert s.sqlserver_host == "db.example.com"
        assert s.sqlserver_port == 1434

    def test_load_from_env_file(self, monkeypatch):
        # Ensure no env vars interfere with .env file loading
        for key in list(os.environ):
            if key.startswith("NODO_ETL_"):
                monkeypatch.delenv(key)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write("NODO_ETL_DB_TYPE=postgresql\n")
            f.write("NODO_ETL_ENVIRONMENT=staging\n")
            f.write("NODO_ETL_PG_HOST=staging-db.example.com\n")
            f.write("NODO_ETL_PG_PORT=5433\n")
            f.write("NODO_ETL_PG_DB=staging_db\n")
            f.name
            f.flush()
            env_path = f.name

        try:
            s = load_settings(env_file=env_path)
            assert s.db_type == DatabaseType.POSTGRESQL
            assert s.environment == Environment.STAGING
            assert s.pg_host == "staging-db.example.com"
        finally:
            os.unlink(env_path)


class TestSettingsValidation:
    def test_invalid_db_type_raises(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_DB_TYPE", "mongodb")
        with pytest.raises(ValidationError):
            Settings(_env_file=None)

    def test_invalid_environment_raises(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_ENVIRONMENT", "test")
        with pytest.raises(ValidationError):
            Settings(_env_file=None)

    def test_db_type_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_DB_TYPE", "PostgreSQL")
        s = Settings(_env_file=None)
        assert s.db_type == DatabaseType.POSTGRESQL

    def test_environment_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_ENVIRONMENT", "PROD")
        s = Settings(_env_file=None)
        assert s.environment == Environment.PROD

    def test_accepts_dev(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_ENVIRONMENT", "dev")
        s = Settings(_env_file=None)
        assert s.environment == Environment.DEV

    def test_accepts_staging(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_ENVIRONMENT", "staging")
        s = Settings(_env_file=None)
        assert s.environment == Environment.STAGING

    def test_accepts_prod(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_ENVIRONMENT", "prod")
        s = Settings(_env_file=None)
        assert s.environment == Environment.PROD


class TestSettingsProperties:
    def test_db_host_postgresql(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_DB_TYPE", "postgresql")
        monkeypatch.setenv("NODO_ETL_PG_HOST", "pg-host")
        s = Settings(_env_file=None)
        assert s.db_host == "pg-host"

    def test_db_host_sqlserver(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_DB_TYPE", "sqlserver")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_HOST", "sql-host")
        s = Settings(_env_file=None)
        assert s.db_host == "sql-host"

    def test_db_port_postgresql(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_DB_TYPE", "postgresql")
        s = Settings(_env_file=None)
        assert s.db_port == 5433

    def test_db_port_sqlserver(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_DB_TYPE", "sqlserver")
        s = Settings(_env_file=None)
        assert s.db_port == 1433

    def test_connection_string_postgresql(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_DB_TYPE", "postgresql")
        monkeypatch.setenv("NODO_ETL_PG_USER", "user")
        monkeypatch.setenv("NODO_ETL_PG_PASSWORD", "pass")
        monkeypatch.setenv("NODO_ETL_PG_HOST", "localhost")
        monkeypatch.setenv("NODO_ETL_PG_PORT", "5433")
        monkeypatch.setenv("NODO_ETL_PG_DB", "mydb")
        s = Settings(_env_file=None)
        assert s.connection_string == "postgresql+psycopg2://user:pass@localhost:5433/mydb"

    def test_connection_string_sqlserver(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_DB_TYPE", "sqlserver")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_USER", "sa")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_PASSWORD", "pass")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_HOST", "localhost")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_PORT", "1433")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_DB", "mydb")
        s = Settings(_env_file=None)
        assert "mssql+pyodbc://sa:pass@localhost:1433/mydb" in s.connection_string


class TestSettingsSecurity:
    def test_repr_does_not_expose_password(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_PG_PASSWORD", "super_secret")
        s = Settings(_env_file=None)
        repr_str = repr(s)
        assert "super_secret" not in repr_str

    def test_str_does_not_expose_password(self, monkeypatch):
        monkeypatch.setenv("NODO_ETL_PG_PASSWORD", "super_secret")
        s = Settings(_env_file=None)
        str_str = str(s)
        assert "super_secret" not in str_str

    def test_frozen_model(self, monkeypatch):
        s = Settings(_env_file=None)
        with pytest.raises(ValidationError):
            s.db_type = DatabaseType.SQLSERVER
