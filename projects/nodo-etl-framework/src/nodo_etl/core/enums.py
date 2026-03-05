"""Enumerations for the Nodo ETL Framework."""

from enum import Enum


class SourceType(str, Enum):
    """Type of data source for a dataset."""

    DATABASE = "database"
    FILE = "file"
    API = "api"
    STREAM = "stream"


class Layer(str, Enum):
    """Medallion architecture layer."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class LoadStrategy(str, Enum):
    """Strategy for loading data."""

    FULL = "full"
    INCREMENTAL = "incremental"
    CDC = "cdc"
    STREAMING = "streaming"


class IdempotencyStrategy(str, Enum):
    """Strategy for ensuring idempotent writes."""

    OVERWRITE = "overwrite"
    UPSERT = "upsert"
    APPEND = "append"
    MERGE = "merge"


class ExecutionStatus(str, Enum):
    """Status of a process, job, or dataset execution."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class TriggerType(str, Enum):
    """How an execution was triggered."""

    SCHEDULE = "schedule"
    MANUAL = "manual"
    RETRY = "retry"


class HookType(str, Enum):
    """When a hook executes relative to its parent entity."""

    PRE = "pre"
    POST = "post"


class ActionType(str, Enum):
    """Type of action a hook performs."""

    SQL = "sql"
    API = "api"
    COMMAND = "command"


class HookOnStatus(str, Enum):
    """When a post-hook should fire based on parent status."""

    SUCCESS = "success"
    FAILED = "failed"
    ANY = "any"


class EntityType(str, Enum):
    """Entity types for hooks, tags, etc."""

    PROCESS = "process"
    JOB = "job"
    DATASET = "dataset"


class ConnectionType(str, Enum):
    """Supported connection types."""

    SQLSERVER = "sqlserver"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    API = "api"
    FILE_STORAGE = "file_storage"
    KAFKA = "kafka"
    DELTA = "delta"
    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"


class SecretProviderType(str, Enum):
    """Supported secret provider backends."""

    ENV = "env"
    KEYVAULT = "keyvault"
    AWS_SM = "aws_sm"
    AIRFLOW = "airflow"


class Environment(str, Enum):
    """Deployment environments."""

    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


class FileFormat(str, Enum):
    """Supported file formats for file sources."""

    CSV = "csv"
    PARQUET = "parquet"
    DELTA = "delta"
    JSON = "json"
    AVRO = "avro"


class Compression(str, Enum):
    """Supported file compression types."""

    GZIP = "gzip"
    SNAPPY = "snappy"
    ZSTD = "zstd"
    NONE = "none"


class PaginationStrategy(str, Enum):
    """API pagination strategies."""

    OFFSET = "offset"
    CURSOR = "cursor"
    NEXT_URL = "next_url"
    NONE = "none"


class ApiAuthMethod(str, Enum):
    """API authentication methods."""

    BEARER = "bearer"
    API_KEY = "api_key"
    OAUTH = "oauth"
    NONE = "none"


class OffsetStrategy(str, Enum):
    """Kafka/stream offset strategies."""

    EARLIEST = "earliest"
    LATEST = "latest"
    SPECIFIC = "specific"


class DatabaseType(str, Enum):
    """Supported metadata database engines."""

    SQLSERVER = "sqlserver"
    POSTGRESQL = "postgresql"
