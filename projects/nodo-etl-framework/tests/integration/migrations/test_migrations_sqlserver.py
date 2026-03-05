"""Integration tests: validate migrations applied correctly on SQL Server.

Requires Docker SQL Server service:
    docker compose -f docker/docker-compose.yml up -d sqlserver

Set NODO_ETL_INTEGRATION_TESTS=1 to enable.
"""

import pytest
import sqlalchemy as sa

from tests.integration.conftest import skip_integration

# -- Connection details -------------------------------------------------------

SQLSERVER_URL = (
    "mssql+pyodbc://sa:NodoEtl%402024!@localhost:1433/nodo_etl_db"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)

SCHEMA = "nodo_etl"

EXPECTED_TABLES = [
    "etl_process",
    "etl_schedule",
    "etl_connection",
    "etl_job",
    "etl_dataset",
    "etl_dataset_db_config",
    "etl_dataset_file_config",
    "etl_dataset_api_config",
    "etl_dataset_stream_config",
    "etl_process_execution",
    "etl_job_execution",
    "etl_dataset_execution",
    "etl_watermark",
    "etl_hook",
    "etl_tag",
    "etl_dataset_lineage",
    "etl_system_config",
]

AUDIT_COLUMNS = {"created_at", "created_by", "updated_at", "updated_by"}

# -- Fixtures -----------------------------------------------------------------


@pytest.fixture(scope="module")
def mssql_engine():
    """Create a raw SQLAlchemy engine for SQL Server introspection."""
    engine = sa.create_engine(SQLSERVER_URL)
    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def mssql_connection(mssql_engine):
    """Provide a connection scoped to the test module."""
    with mssql_engine.connect() as conn:
        yield conn


# -- Helpers ------------------------------------------------------------------


def _get_columns(conn, table_name: str) -> dict[str, dict]:
    """Return {column_name: {data_type, column_default, is_nullable}} for a table."""
    result = conn.execute(
        sa.text(
            """
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
            ORDER BY ORDINAL_POSITION
            """
        ),
        {"schema": SCHEMA, "table": table_name},
    )
    return {
        row.COLUMN_NAME: {
            "data_type": row.DATA_TYPE,
            "column_default": row.COLUMN_DEFAULT,
            "is_nullable": row.IS_NULLABLE,
        }
        for row in result
    }


# -- Tests --------------------------------------------------------------------


@skip_integration
class TestMigrationsSQLServer:
    """Validate that migrations were applied correctly on SQL Server."""

    # -- Schema & Table Existence ---------------------------------------------

    def test_schema_exists(self, mssql_connection):
        """The nodo_etl schema must exist."""
        result = mssql_connection.execute(
            sa.text(
                """
                SELECT SCHEMA_NAME
                FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME = :schema
                """
            ),
            {"schema": SCHEMA},
        )
        schemas = [row.SCHEMA_NAME for row in result]
        assert SCHEMA in schemas, f"Schema '{SCHEMA}' does not exist"

    def test_all_tables_exist(self, mssql_connection):
        """All 17 expected tables must exist in the nodo_etl schema."""
        result = mssql_connection.execute(
            sa.text(
                """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = :schema AND TABLE_TYPE = 'BASE TABLE'
                """
            ),
            {"schema": SCHEMA},
        )
        existing_tables = {row.TABLE_NAME for row in result}

        for table in EXPECTED_TABLES:
            assert table in existing_tables, (
                f"Table '{SCHEMA}.{table}' not found. "
                f"Existing tables: {sorted(existing_tables)}"
            )

    def test_all_tables_have_audit_columns(self, mssql_connection):
        """Every table must contain the four audit columns."""
        for table in EXPECTED_TABLES:
            columns = _get_columns(mssql_connection, table)
            column_names = set(columns.keys())
            missing = AUDIT_COLUMNS - column_names
            assert not missing, (
                f"Table '{SCHEMA}.{table}' is missing audit columns: {missing}"
            )

    # -- Column Verification (spot check) -------------------------------------

    def test_etl_process_columns(self, mssql_connection):
        """etl_process must have all expected columns."""
        expected = {
            "id",
            "process_name",
            "description",
            "max_parallelism",
            "execution_order",
            "max_retries",
            "timeout_seconds",
            "is_enabled",
            "is_deleted",
        } | AUDIT_COLUMNS

        columns = _get_columns(mssql_connection, "etl_process")
        column_names = set(columns.keys())

        missing = expected - column_names
        assert not missing, (
            f"etl_process is missing columns: {missing}. "
            f"Actual columns: {sorted(column_names)}"
        )

    def test_etl_dataset_columns(self, mssql_connection):
        """etl_dataset must have all expected columns."""
        expected = {
            "id",
            "job_id",
            "dataset_name",
            "description",
            "source_type",
            "source_connection_id",
            "target_connection_id",
            "layer",
            "load_strategy",
            "idempotency_strategy",
            "execution_order",
            "max_retries",
            "retry_delay_seconds",
            "timeout_seconds",
            "is_enabled",
            "is_deleted",
        } | AUDIT_COLUMNS

        columns = _get_columns(mssql_connection, "etl_dataset")
        column_names = set(columns.keys())

        missing = expected - column_names
        assert not missing, (
            f"etl_dataset is missing columns: {missing}. "
            f"Actual columns: {sorted(column_names)}"
        )

    def test_etl_connection_columns(self, mssql_connection):
        """etl_connection must have all expected columns."""
        expected = {
            "id",
            "connection_name",
            "connection_type",
            "host",
            "port",
            "database_name",
            "schema_name",
            "secret_reference",
            "secret_provider_type",
            "additional_params",
            "environment",
            "is_enabled",
            "is_deleted",
        } | AUDIT_COLUMNS

        columns = _get_columns(mssql_connection, "etl_connection")
        column_names = set(columns.keys())

        missing = expected - column_names
        assert not missing, (
            f"etl_connection is missing columns: {missing}. "
            f"Actual columns: {sorted(column_names)}"
        )

    # -- Default Values -------------------------------------------------------

    def test_etl_process_is_enabled_default(self, mssql_connection):
        """etl_process.is_enabled must default to true."""
        columns = _get_columns(mssql_connection, "etl_process")
        default = columns["is_enabled"]["column_default"]
        assert default is not None, "is_enabled has no default"
        # SQL Server wraps defaults in parens: ((1)) or ('true')
        assert "1" in default or "true" in default.lower(), (
            f"Expected is_enabled default to be true/1, got: {default}"
        )

    def test_etl_process_is_deleted_default(self, mssql_connection):
        """etl_process.is_deleted must default to false."""
        columns = _get_columns(mssql_connection, "etl_process")
        default = columns["is_deleted"]["column_default"]
        assert default is not None, "is_deleted has no default"
        # SQL Server wraps defaults in parens: ((0)) or ('false')
        assert "0" in default or "false" in default.lower(), (
            f"Expected is_deleted default to be false/0, got: {default}"
        )

    def test_etl_process_execution_order_default(self, mssql_connection):
        """etl_process.execution_order must default to 1."""
        columns = _get_columns(mssql_connection, "etl_process")
        default = columns["execution_order"]["column_default"]
        assert default is not None, "execution_order has no default"
        assert "1" in default, (
            f"Expected execution_order default to be 1, got: {default}"
        )

    def test_etl_dataset_execution_retry_count_default(self, mssql_connection):
        """etl_dataset_execution.retry_count must default to 0."""
        columns = _get_columns(mssql_connection, "etl_dataset_execution")
        default = columns["retry_count"]["column_default"]
        assert default is not None, "retry_count has no default"
        assert "0" in default, (
            f"Expected retry_count default to be 0, got: {default}"
        )

    def test_etl_hook_on_status_default(self, mssql_connection):
        """etl_hook.on_status must default to 'any'."""
        columns = _get_columns(mssql_connection, "etl_hook")
        default = columns["on_status"]["column_default"]
        assert default is not None, "on_status has no default"
        assert "any" in default.lower(), (
            f"Expected on_status default to be 'any', got: {default}"
        )

    # -- Seed Data ------------------------------------------------------------

    def test_system_config_has_all_default_rows(self, mssql_connection):
        """etl_system_config must have 9 default seed rows."""
        result = mssql_connection.execute(
            sa.text(
                f"SELECT COUNT(*) AS cnt FROM {SCHEMA}.etl_system_config"
            ),
        )
        count = result.scalar()
        assert count == 9, (
            f"Expected 9 seed rows in etl_system_config, got: {count}"
        )

    def test_seed_default_max_retries(self, mssql_connection):
        """default_max_retries must be 3."""
        result = mssql_connection.execute(
            sa.text(
                f"""
                SELECT config_value
                FROM {SCHEMA}.etl_system_config
                WHERE config_key = 'default_max_retries'
                """
            ),
        )
        value = result.scalar()
        assert value == "3", f"Expected default_max_retries=3, got: {value}"

    def test_seed_default_timeout_seconds(self, mssql_connection):
        """default_timeout_seconds must be 3600."""
        result = mssql_connection.execute(
            sa.text(
                f"""
                SELECT config_value
                FROM {SCHEMA}.etl_system_config
                WHERE config_key = 'default_timeout_seconds'
                """
            ),
        )
        value = result.scalar()
        assert value == "3600", (
            f"Expected default_timeout_seconds=3600, got: {value}"
        )

    def test_seed_default_parallelism(self, mssql_connection):
        """default_parallelism must be 5."""
        result = mssql_connection.execute(
            sa.text(
                f"""
                SELECT config_value
                FROM {SCHEMA}.etl_system_config
                WHERE config_key = 'default_parallelism'
                """
            ),
        )
        value = result.scalar()
        assert value == "5", f"Expected default_parallelism=5, got: {value}"

    def test_seed_secret_provider(self, mssql_connection):
        """secret_provider must be 'env'."""
        result = mssql_connection.execute(
            sa.text(
                f"""
                SELECT config_value
                FROM {SCHEMA}.etl_system_config
                WHERE config_key = 'secret_provider'
                """
            ),
        )
        value = result.scalar()
        assert value == "env", f"Expected secret_provider=env, got: {value}"

    def test_seed_timezone(self, mssql_connection):
        """timezone must be 'America/Mexico_City'."""
        result = mssql_connection.execute(
            sa.text(
                f"""
                SELECT config_value
                FROM {SCHEMA}.etl_system_config
                WHERE config_key = 'timezone'
                """
            ),
        )
        value = result.scalar()
        assert value == "America/Mexico_City", (
            f"Expected timezone=America/Mexico_City, got: {value}"
        )

    def test_seed_log_retention_days(self, mssql_connection):
        """log_retention_days must be 90."""
        result = mssql_connection.execute(
            sa.text(
                f"""
                SELECT config_value
                FROM {SCHEMA}.etl_system_config
                WHERE config_key = 'log_retention_days'
                """
            ),
        )
        value = result.scalar()
        assert value == "90", (
            f"Expected log_retention_days=90, got: {value}"
        )
