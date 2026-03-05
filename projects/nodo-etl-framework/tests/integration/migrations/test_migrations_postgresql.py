"""Integration tests: validate migrations applied correctly on PostgreSQL.

Requires Docker PostgreSQL service:
    docker compose -f docker/docker-compose.yml up -d postgresql

Set NODO_ETL_INTEGRATION_TESTS=1 to enable.
"""

import pytest
import sqlalchemy as sa

from tests.integration.conftest import skip_integration

# -- Connection details -------------------------------------------------------

PG_URL = (
    "postgresql+psycopg2://nodo_etl:nodo_etl_password"
    "@localhost:5433/nodo_etl_db"
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
def pg_engine():
    """Create a raw SQLAlchemy engine for PostgreSQL introspection."""
    engine = sa.create_engine(PG_URL)
    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def pg_connection(pg_engine):
    """Provide a connection scoped to the test module."""
    with pg_engine.connect() as conn:
        yield conn


# -- Helpers ------------------------------------------------------------------


def _get_columns(conn, table_name: str) -> dict[str, dict]:
    """Return {column_name: {data_type, column_default, is_nullable}} for a table."""
    result = conn.execute(
        sa.text(
            """
            SELECT column_name, data_type, column_default, is_nullable
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
            """
        ),
        {"schema": SCHEMA, "table": table_name},
    )
    return {
        row.column_name: {
            "data_type": row.data_type,
            "column_default": row.column_default,
            "is_nullable": row.is_nullable,
        }
        for row in result
    }


# -- Tests --------------------------------------------------------------------


@skip_integration
class TestMigrationsPostgreSQL:
    """Validate that migrations were applied correctly on PostgreSQL."""

    # -- Schema & Table Existence ---------------------------------------------

    def test_schema_exists(self, pg_connection):
        """The nodo_etl schema must exist."""
        result = pg_connection.execute(
            sa.text(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = :schema
                """
            ),
            {"schema": SCHEMA},
        )
        schemas = [row.schema_name for row in result]
        assert SCHEMA in schemas, f"Schema '{SCHEMA}' does not exist"

    def test_all_tables_exist(self, pg_connection):
        """All 17 expected tables must exist in the nodo_etl schema."""
        result = pg_connection.execute(
            sa.text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = :schema AND table_type = 'BASE TABLE'
                """
            ),
            {"schema": SCHEMA},
        )
        existing_tables = {row.table_name for row in result}

        for table in EXPECTED_TABLES:
            assert table in existing_tables, (
                f"Table '{SCHEMA}.{table}' not found. "
                f"Existing tables: {sorted(existing_tables)}"
            )

    def test_all_tables_have_audit_columns(self, pg_connection):
        """Every table must contain the four audit columns."""
        for table in EXPECTED_TABLES:
            columns = _get_columns(pg_connection, table)
            column_names = set(columns.keys())
            missing = AUDIT_COLUMNS - column_names
            assert not missing, (
                f"Table '{SCHEMA}.{table}' is missing audit columns: {missing}"
            )

    # -- Column Verification (spot check) -------------------------------------

    def test_etl_process_columns(self, pg_connection):
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

        columns = _get_columns(pg_connection, "etl_process")
        column_names = set(columns.keys())

        missing = expected - column_names
        assert not missing, (
            f"etl_process is missing columns: {missing}. "
            f"Actual columns: {sorted(column_names)}"
        )

    def test_etl_dataset_columns(self, pg_connection):
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

        columns = _get_columns(pg_connection, "etl_dataset")
        column_names = set(columns.keys())

        missing = expected - column_names
        assert not missing, (
            f"etl_dataset is missing columns: {missing}. "
            f"Actual columns: {sorted(column_names)}"
        )

    def test_etl_connection_columns(self, pg_connection):
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

        columns = _get_columns(pg_connection, "etl_connection")
        column_names = set(columns.keys())

        missing = expected - column_names
        assert not missing, (
            f"etl_connection is missing columns: {missing}. "
            f"Actual columns: {sorted(column_names)}"
        )

    # -- Default Values -------------------------------------------------------

    def test_etl_process_is_enabled_default(self, pg_connection):
        """etl_process.is_enabled must default to true."""
        columns = _get_columns(pg_connection, "etl_process")
        default = columns["is_enabled"]["column_default"]
        assert default is not None, "is_enabled has no default"
        assert "true" in default.lower(), (
            f"Expected is_enabled default to be true, got: {default}"
        )

    def test_etl_process_is_deleted_default(self, pg_connection):
        """etl_process.is_deleted must default to false."""
        columns = _get_columns(pg_connection, "etl_process")
        default = columns["is_deleted"]["column_default"]
        assert default is not None, "is_deleted has no default"
        assert "false" in default.lower(), (
            f"Expected is_deleted default to be false, got: {default}"
        )

    def test_etl_process_execution_order_default(self, pg_connection):
        """etl_process.execution_order must default to 1."""
        columns = _get_columns(pg_connection, "etl_process")
        default = columns["execution_order"]["column_default"]
        assert default is not None, "execution_order has no default"
        assert "1" in default, (
            f"Expected execution_order default to be 1, got: {default}"
        )

    def test_etl_dataset_execution_retry_count_default(self, pg_connection):
        """etl_dataset_execution.retry_count must default to 0."""
        columns = _get_columns(pg_connection, "etl_dataset_execution")
        default = columns["retry_count"]["column_default"]
        assert default is not None, "retry_count has no default"
        assert "0" in default, (
            f"Expected retry_count default to be 0, got: {default}"
        )

    def test_etl_hook_on_status_default(self, pg_connection):
        """etl_hook.on_status must default to 'any'."""
        columns = _get_columns(pg_connection, "etl_hook")
        default = columns["on_status"]["column_default"]
        assert default is not None, "on_status has no default"
        assert "any" in default.lower(), (
            f"Expected on_status default to be 'any', got: {default}"
        )

    # -- Seed Data ------------------------------------------------------------

    def test_system_config_has_all_default_rows(self, pg_connection):
        """etl_system_config must have 9 default seed rows."""
        result = pg_connection.execute(
            sa.text(
                f"SELECT COUNT(*) AS cnt FROM {SCHEMA}.etl_system_config"
            ),
        )
        count = result.scalar()
        assert count == 9, (
            f"Expected 9 seed rows in etl_system_config, got: {count}"
        )

    def test_seed_default_max_retries(self, pg_connection):
        """default_max_retries must be 3."""
        result = pg_connection.execute(
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

    def test_seed_default_timeout_seconds(self, pg_connection):
        """default_timeout_seconds must be 3600."""
        result = pg_connection.execute(
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

    def test_seed_default_parallelism(self, pg_connection):
        """default_parallelism must be 5."""
        result = pg_connection.execute(
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

    def test_seed_secret_provider(self, pg_connection):
        """secret_provider must be 'env'."""
        result = pg_connection.execute(
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

    def test_seed_timezone(self, pg_connection):
        """timezone must be 'America/Mexico_City'."""
        result = pg_connection.execute(
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

    def test_seed_log_retention_days(self, pg_connection):
        """log_retention_days must be 90."""
        result = pg_connection.execute(
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
