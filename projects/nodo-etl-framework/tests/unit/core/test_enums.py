"""Tests for all ETL framework enumerations."""

import pytest

from nodo_etl.core.enums import (
    ActionType,
    ApiAuthMethod,
    Compression,
    ConnectionType,
    DatabaseType,
    EntityType,
    Environment,
    ExecutionStatus,
    FileFormat,
    HookOnStatus,
    HookType,
    IdempotencyStrategy,
    Layer,
    LoadStrategy,
    OffsetStrategy,
    PaginationStrategy,
    SecretProviderType,
    SourceType,
    TriggerType,
)


class TestSourceType:
    def test_has_all_members(self):
        assert set(SourceType) == {
            SourceType.DATABASE,
            SourceType.FILE,
            SourceType.API,
            SourceType.STREAM,
        }

    def test_values_are_strings(self):
        for member in SourceType:
            assert isinstance(member.value, str)

    def test_from_valid_value(self):
        assert SourceType("database") == SourceType.DATABASE
        assert SourceType("file") == SourceType.FILE
        assert SourceType("api") == SourceType.API
        assert SourceType("stream") == SourceType.STREAM

    def test_from_invalid_value(self):
        with pytest.raises(ValueError):
            SourceType("invalid")

    def test_is_str_subclass(self):
        assert isinstance(SourceType.DATABASE, str)
        assert SourceType.DATABASE == "database"

    def test_iteration(self):
        members = list(SourceType)
        assert len(members) == 4


class TestLayer:
    def test_has_all_members(self):
        assert set(Layer) == {Layer.BRONZE, Layer.SILVER, Layer.GOLD}

    def test_values_are_strings(self):
        for member in Layer:
            assert isinstance(member.value, str)

    def test_from_valid_value(self):
        assert Layer("bronze") == Layer.BRONZE
        assert Layer("silver") == Layer.SILVER
        assert Layer("gold") == Layer.GOLD

    def test_from_invalid_value(self):
        with pytest.raises(ValueError):
            Layer("platinum")


class TestLoadStrategy:
    def test_has_all_members(self):
        assert set(LoadStrategy) == {
            LoadStrategy.FULL,
            LoadStrategy.INCREMENTAL,
            LoadStrategy.CDC,
            LoadStrategy.STREAMING,
        }

    def test_from_valid_value(self):
        assert LoadStrategy("full") == LoadStrategy.FULL
        assert LoadStrategy("incremental") == LoadStrategy.INCREMENTAL
        assert LoadStrategy("cdc") == LoadStrategy.CDC
        assert LoadStrategy("streaming") == LoadStrategy.STREAMING

    def test_from_invalid_value(self):
        with pytest.raises(ValueError):
            LoadStrategy("batch")


class TestIdempotencyStrategy:
    def test_has_all_members(self):
        assert set(IdempotencyStrategy) == {
            IdempotencyStrategy.OVERWRITE,
            IdempotencyStrategy.UPSERT,
            IdempotencyStrategy.APPEND,
            IdempotencyStrategy.MERGE,
        }

    def test_from_invalid_value(self):
        with pytest.raises(ValueError):
            IdempotencyStrategy("replace")


class TestExecutionStatus:
    def test_has_all_members(self):
        assert set(ExecutionStatus) == {
            ExecutionStatus.PENDING,
            ExecutionStatus.RUNNING,
            ExecutionStatus.SUCCESS,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.SKIPPED,
        }

    def test_values_are_strings(self):
        for member in ExecutionStatus:
            assert isinstance(member.value, str)

    def test_from_invalid_value(self):
        with pytest.raises(ValueError):
            ExecutionStatus("error")


class TestTriggerType:
    def test_has_all_members(self):
        assert set(TriggerType) == {
            TriggerType.SCHEDULE,
            TriggerType.MANUAL,
            TriggerType.RETRY,
        }

    def test_from_invalid_value(self):
        with pytest.raises(ValueError):
            TriggerType("cron")


class TestHookType:
    def test_has_all_members(self):
        assert set(HookType) == {HookType.PRE, HookType.POST}


class TestActionType:
    def test_has_all_members(self):
        assert set(ActionType) == {
            ActionType.SQL,
            ActionType.API,
            ActionType.COMMAND,
        }


class TestHookOnStatus:
    def test_has_all_members(self):
        assert set(HookOnStatus) == {
            HookOnStatus.SUCCESS,
            HookOnStatus.FAILED,
            HookOnStatus.ANY,
        }


class TestEntityType:
    def test_has_all_members(self):
        assert set(EntityType) == {
            EntityType.PROCESS,
            EntityType.JOB,
            EntityType.DATASET,
        }


class TestConnectionType:
    def test_has_all_members(self):
        expected = {
            ConnectionType.SQLSERVER,
            ConnectionType.POSTGRESQL,
            ConnectionType.MYSQL,
            ConnectionType.ORACLE,
            ConnectionType.API,
            ConnectionType.FILE_STORAGE,
            ConnectionType.KAFKA,
            ConnectionType.DELTA,
            ConnectionType.SNOWFLAKE,
            ConnectionType.DATABRICKS,
        }
        assert set(ConnectionType) == expected

    def test_count(self):
        assert len(list(ConnectionType)) == 10

    def test_from_invalid_value(self):
        with pytest.raises(ValueError):
            ConnectionType("mongodb")


class TestSecretProviderType:
    def test_has_all_members(self):
        assert set(SecretProviderType) == {
            SecretProviderType.ENV,
            SecretProviderType.KEYVAULT,
            SecretProviderType.AWS_SM,
            SecretProviderType.AIRFLOW,
        }


class TestEnvironment:
    def test_has_all_members(self):
        assert set(Environment) == {
            Environment.DEV,
            Environment.STAGING,
            Environment.PROD,
        }

    def test_from_invalid_value(self):
        with pytest.raises(ValueError):
            Environment("test")


class TestFileFormat:
    def test_has_all_members(self):
        assert set(FileFormat) == {
            FileFormat.CSV,
            FileFormat.PARQUET,
            FileFormat.DELTA,
            FileFormat.JSON,
            FileFormat.AVRO,
        }


class TestCompression:
    def test_has_all_members(self):
        assert set(Compression) == {
            Compression.GZIP,
            Compression.SNAPPY,
            Compression.ZSTD,
            Compression.NONE,
        }


class TestPaginationStrategy:
    def test_has_all_members(self):
        assert set(PaginationStrategy) == {
            PaginationStrategy.OFFSET,
            PaginationStrategy.CURSOR,
            PaginationStrategy.NEXT_URL,
            PaginationStrategy.NONE,
        }


class TestApiAuthMethod:
    def test_has_all_members(self):
        assert set(ApiAuthMethod) == {
            ApiAuthMethod.BEARER,
            ApiAuthMethod.API_KEY,
            ApiAuthMethod.OAUTH,
            ApiAuthMethod.NONE,
        }


class TestOffsetStrategy:
    def test_has_all_members(self):
        assert set(OffsetStrategy) == {
            OffsetStrategy.EARLIEST,
            OffsetStrategy.LATEST,
            OffsetStrategy.SPECIFIC,
        }


class TestDatabaseType:
    def test_has_all_members(self):
        assert set(DatabaseType) == {
            DatabaseType.SQLSERVER,
            DatabaseType.POSTGRESQL,
        }


class TestAllEnumsVarcharLength:
    """Verify all enum values fit within their database VARCHAR columns."""

    @pytest.mark.parametrize(
        "enum_cls,max_length",
        [
            (SourceType, 50),
            (Layer, 20),
            (LoadStrategy, 50),
            (IdempotencyStrategy, 50),
            (ExecutionStatus, 50),
            (TriggerType, 50),
            (HookType, 10),
            (ActionType, 50),
            (HookOnStatus, 50),
            (EntityType, 50),
            (ConnectionType, 50),
            (SecretProviderType, 50),
            (Environment, 50),
            (FileFormat, 50),
            (Compression, 50),
            (PaginationStrategy, 50),
            (ApiAuthMethod, 50),
            (OffsetStrategy, 50),
        ],
    )
    def test_values_within_varchar_limit(self, enum_cls, max_length):
        for member in enum_cls:
            assert len(member.value) <= max_length, (
                f"{enum_cls.__name__}.{member.name} value '{member.value}' "
                f"exceeds VARCHAR({max_length})"
            )
