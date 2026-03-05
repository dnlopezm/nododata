"""Database connection management for the metadata database."""

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from nodo_etl.config.settings import Settings
from nodo_etl.db.dialect import SQLDialect, get_dialect
from nodo_etl.secrets.base import SecretProvider
from nodo_etl.secrets.factory import get_provider


class MetadataDBConnection:
    """Manages the connection to the ETL metadata database."""

    def __init__(
        self,
        settings: Settings,
        secret_provider: SecretProvider | None = None,
        pool_size: int = 5,
    ) -> None:
        self._settings = settings
        self._secret_provider = secret_provider or get_provider(
            settings.secret_provider
        )
        self._pool_size = pool_size
        self._dialect = get_dialect(settings.db_type)
        self._engine: Engine | None = None
        self._session_factory: sessionmaker | None = None

    @property
    def dialect(self) -> SQLDialect:
        """Return the SQL dialect for this connection."""
        return self._dialect

    @property
    def schema_name(self) -> str:
        """Return the configured schema name."""
        return self._settings.schema_name

    @property
    def engine(self) -> Engine:
        """Get or create the SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(
                self._settings.connection_string,
                pool_size=self._pool_size,
                pool_pre_ping=True,
            )
        return self._engine

    def get_session(self) -> Session:
        """Create a new database session."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory()

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        """Execute a SQL statement and return results as list of dicts.

        Args:
            sql: SQL statement to execute.
            params: Optional parameters for the query.

        Returns:
            List of row dicts for SELECT queries, empty list for others.
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            if result.returns_rows:
                columns = list(result.keys())
                return [dict(zip(columns, row)) for row in result.fetchall()]
            conn.commit()
            return []

    def execute_procedure(
        self, procedure_name: str, params: dict | None = None
    ) -> list[dict]:
        """Execute a stored procedure and return results.

        Args:
            procedure_name: Schema-qualified procedure name.
            params: Parameters to pass to the procedure.

        Returns:
            List of row dicts from the procedure result set.
        """
        schema = self._settings.schema_name
        full_name = f"{schema}.{procedure_name}"
        params = params or {}

        if self._dialect.dialect_type.value == "postgresql":
            # PostgreSQL: functions called with SELECT
            param_placeholders = ", ".join(
                f":{k}" for k in params
            )
            sql = f"SELECT * FROM {full_name}({param_placeholders})"
        else:
            # SQL Server: EXEC procedure
            param_assignments = ", ".join(
                f"@{k} = :{k}" for k in params
            )
            sql = f"EXEC {full_name} {param_assignments}"

        return self.execute(sql, params)

    def test_connection(self) -> bool:
        """Test the database connection.

        Returns:
            True if connection is successful.

        Raises:
            Exception: If connection fails.
        """
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            return True

    def close(self) -> None:
        """Close the database connection and dispose of the engine."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None

    def __repr__(self) -> str:
        return (
            f"MetadataDBConnection(db_type={self._settings.db_type.value}, "
            f"host={self._settings.db_host}:{self._settings.db_port}, "
            f"db={self._settings.db_name})"
        )
