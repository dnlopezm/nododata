"""SQL dialect abstraction for SQL Server vs PostgreSQL differences."""

from abc import ABC, abstractmethod

from nodo_etl.core.enums import DatabaseType


class SQLDialect(ABC):
    """Abstract SQL dialect for handling database-specific differences."""

    @property
    @abstractmethod
    def dialect_type(self) -> DatabaseType:
        """Return the database type."""

    @abstractmethod
    def schema_table(self, schema: str, table: str) -> str:
        """Return a schema-qualified table name."""

    @abstractmethod
    def json_type(self) -> str:
        """Return the JSON column type for this dialect."""

    @abstractmethod
    def boolean_type(self) -> str:
        """Return the boolean column type for this dialect."""

    @abstractmethod
    def utc_now(self) -> str:
        """Return the SQL expression for current UTC timestamp."""

    @abstractmethod
    def boolean_value(self, value: bool) -> str:
        """Return the SQL literal for a boolean value."""


class PostgreSQLDialect(SQLDialect):
    """PostgreSQL-specific SQL dialect."""

    @property
    def dialect_type(self) -> DatabaseType:
        return DatabaseType.POSTGRESQL

    def schema_table(self, schema: str, table: str) -> str:
        return f"{schema}.{table}"

    def json_type(self) -> str:
        return "JSONB"

    def boolean_type(self) -> str:
        return "BOOLEAN"

    def utc_now(self) -> str:
        return "CURRENT_TIMESTAMP"

    def boolean_value(self, value: bool) -> str:
        return "TRUE" if value else "FALSE"


class SQLServerDialect(SQLDialect):
    """SQL Server-specific SQL dialect."""

    @property
    def dialect_type(self) -> DatabaseType:
        return DatabaseType.SQLSERVER

    def schema_table(self, schema: str, table: str) -> str:
        return f"{schema}.{table}"

    def json_type(self) -> str:
        return "NVARCHAR(MAX)"

    def boolean_type(self) -> str:
        return "BIT"

    def utc_now(self) -> str:
        return "GETUTCDATE()"

    def boolean_value(self, value: bool) -> str:
        return "1" if value else "0"


def get_dialect(db_type: DatabaseType | str) -> SQLDialect:
    """Get the SQL dialect for the given database type.

    Args:
        db_type: The database type.

    Returns:
        A SQLDialect instance.

    Raises:
        ValueError: If the database type is not supported.
    """
    if isinstance(db_type, str):
        db_type = DatabaseType(db_type.lower())

    if db_type == DatabaseType.POSTGRESQL:
        return PostgreSQLDialect()
    elif db_type == DatabaseType.SQLSERVER:
        return SQLServerDialect()
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
