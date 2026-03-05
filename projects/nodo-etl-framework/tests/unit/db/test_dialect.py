"""Tests for the SQL dialect abstraction."""

import pytest

from nodo_etl.core.enums import DatabaseType
from nodo_etl.db.dialect import (
    PostgreSQLDialect,
    SQLServerDialect,
    get_dialect,
)


class TestPostgreSQLDialect:
    """Tests for PostgreSQL dialect."""

    @pytest.fixture
    def dialect(self):
        return PostgreSQLDialect()

    def test_dialect_type(self, dialect):
        """Test dialect type is PostgreSQL."""
        assert dialect.dialect_type == DatabaseType.POSTGRESQL

    def test_schema_table(self, dialect):
        """Test schema-qualified table name."""
        assert dialect.schema_table("nodo_etl", "etl_process") == "nodo_etl.etl_process"

    def test_json_type(self, dialect):
        """Test JSON type is JSONB."""
        assert dialect.json_type() == "JSONB"

    def test_boolean_type(self, dialect):
        """Test boolean type is BOOLEAN."""
        assert dialect.boolean_type() == "BOOLEAN"

    def test_utc_now(self, dialect):
        """Test UTC now expression."""
        assert dialect.utc_now() == "CURRENT_TIMESTAMP"

    def test_boolean_value_true(self, dialect):
        """Test boolean TRUE value."""
        assert dialect.boolean_value(True) == "TRUE"

    def test_boolean_value_false(self, dialect):
        """Test boolean FALSE value."""
        assert dialect.boolean_value(False) == "FALSE"


class TestSQLServerDialect:
    """Tests for SQL Server dialect."""

    @pytest.fixture
    def dialect(self):
        return SQLServerDialect()

    def test_dialect_type(self, dialect):
        """Test dialect type is SQL Server."""
        assert dialect.dialect_type == DatabaseType.SQLSERVER

    def test_schema_table(self, dialect):
        """Test schema-qualified table name."""
        assert dialect.schema_table("nodo_etl", "etl_process") == "nodo_etl.etl_process"

    def test_json_type(self, dialect):
        """Test JSON type is NVARCHAR(MAX)."""
        assert dialect.json_type() == "NVARCHAR(MAX)"

    def test_boolean_type(self, dialect):
        """Test boolean type is BIT."""
        assert dialect.boolean_type() == "BIT"

    def test_utc_now(self, dialect):
        """Test UTC now expression."""
        assert dialect.utc_now() == "GETUTCDATE()"

    def test_boolean_value_true(self, dialect):
        """Test boolean value for True is '1'."""
        assert dialect.boolean_value(True) == "1"

    def test_boolean_value_false(self, dialect):
        """Test boolean value for False is '0'."""
        assert dialect.boolean_value(False) == "0"


class TestGetDialect:
    """Tests for the dialect factory function."""

    def test_get_postgresql_dialect(self):
        """Test get_dialect returns PostgreSQL dialect."""
        dialect = get_dialect(DatabaseType.POSTGRESQL)
        assert isinstance(dialect, PostgreSQLDialect)

    def test_get_sqlserver_dialect(self):
        """Test get_dialect returns SQL Server dialect."""
        dialect = get_dialect(DatabaseType.SQLSERVER)
        assert isinstance(dialect, SQLServerDialect)

    def test_get_dialect_from_string_postgresql(self):
        """Test get_dialect with string 'postgresql'."""
        dialect = get_dialect("postgresql")
        assert isinstance(dialect, PostgreSQLDialect)

    def test_get_dialect_from_string_sqlserver(self):
        """Test get_dialect with string 'sqlserver'."""
        dialect = get_dialect("sqlserver")
        assert isinstance(dialect, SQLServerDialect)

    def test_get_dialect_unsupported_raises_error(self):
        """Test get_dialect raises error for unsupported type."""
        with pytest.raises(ValueError, match="not a valid DatabaseType"):
            get_dialect("mysql")
