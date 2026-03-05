"""Pydantic models for all ETL metadata entities."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from nodo_etl.core.enums import (
    ActionType,
    ApiAuthMethod,
    Compression,
    ConnectionType,
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


class AuditMixin(BaseModel):
    """Audit columns present on every table."""

    created_at: datetime | None = None
    created_by: str | None = None
    updated_at: datetime | None = None
    updated_by: str | None = None


# ---------------------------------------------------------------------------
# Core Tables
# ---------------------------------------------------------------------------


class ProcessModel(AuditMixin):
    """Represents an ETL process."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    process_name: str = Field(..., min_length=1, max_length=200)
    description: str | None = Field(None, max_length=1000)
    max_parallelism: int | None = Field(None, gt=0)
    execution_order: int = Field(1, ge=1)
    max_retries: int | None = Field(None, ge=0)
    timeout_seconds: int | None = Field(None, gt=0)
    is_enabled: bool = True
    is_deleted: bool = False


class ScheduleModel(AuditMixin):
    """Represents a cron schedule for a process."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    process_id: int = Field(...)
    schedule_name: str = Field(..., min_length=1, max_length=200)
    cron_expression: str = Field(..., min_length=1, max_length=100)
    is_enabled: bool = True
    is_deleted: bool = False


class JobModel(AuditMixin):
    """Represents a job within a process."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    process_id: int = Field(...)
    job_name: str = Field(..., min_length=1, max_length=200)
    description: str | None = Field(None, max_length=1000)
    execution_order: int = Field(1, ge=1)
    max_parallelism: int | None = Field(None, gt=0)
    max_retries: int | None = Field(None, ge=0)
    timeout_seconds: int | None = Field(None, gt=0)
    is_enabled: bool = True
    is_deleted: bool = False


class ConnectionModel(AuditMixin):
    """Represents a connection to a data source or target."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    connection_name: str = Field(..., min_length=1, max_length=200)
    connection_type: ConnectionType
    host: str | None = Field(None, max_length=500)
    port: int | None = Field(None, gt=0, le=65535)
    database_name: str | None = Field(None, max_length=200)
    schema_name: str | None = Field(None, max_length=200)
    secret_reference: str | None = Field(None, max_length=500)
    secret_provider_type: SecretProviderType | None = None
    additional_params: dict[str, Any] | None = None
    environment: Environment
    is_enabled: bool = True
    is_deleted: bool = False

    def __repr__(self) -> str:
        return (
            f"ConnectionModel(id={self.id}, name={self.connection_name!r}, "
            f"type={self.connection_type.value}, env={self.environment.value})"
        )

    def __str__(self) -> str:
        return self.__repr__()


class DatasetModel(AuditMixin):
    """Represents a dataset (table/file/api/stream) within a job."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    job_id: int = Field(...)
    dataset_name: str = Field(..., min_length=1, max_length=200)
    description: str | None = Field(None, max_length=1000)
    source_type: SourceType
    source_connection_id: int = Field(...)
    target_connection_id: int | None = None
    layer: Layer
    load_strategy: LoadStrategy
    idempotency_strategy: IdempotencyStrategy
    execution_order: int = Field(1, ge=1)
    max_retries: int | None = Field(None, ge=0)
    retry_delay_seconds: int | None = Field(None, ge=0)
    timeout_seconds: int | None = Field(None, gt=0)
    is_enabled: bool = True
    is_deleted: bool = False


# ---------------------------------------------------------------------------
# Source Configuration Tables
# ---------------------------------------------------------------------------


class DatasetDbConfigModel(AuditMixin):
    """Database source configuration for a dataset."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    dataset_id: int = Field(...)
    source_schema: str | None = Field(None, max_length=200)
    source_table: str | None = Field(None, max_length=200)
    extraction_query: str | None = None
    watermark_column: str | None = Field(None, max_length=200)
    primary_key_columns: str | None = Field(None, max_length=500)


class DatasetFileConfigModel(AuditMixin):
    """File source configuration for a dataset."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    dataset_id: int = Field(...)
    file_path_pattern: str = Field(..., min_length=1, max_length=1000)
    file_format: FileFormat
    delimiter: str | None = Field(None, max_length=10)
    has_header: bool | None = True
    encoding: str | None = Field("utf-8", max_length=50)
    compression: Compression | None = None


class DatasetApiConfigModel(AuditMixin):
    """API source configuration for a dataset."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    dataset_id: int = Field(...)
    api_url: str = Field(..., min_length=1, max_length=2000)
    api_method: str = Field(..., pattern=r"^(GET|POST|PUT)$")
    api_headers: dict[str, Any] | None = None
    api_body_template: dict[str, Any] | None = None
    pagination_strategy: PaginationStrategy | None = None
    pagination_config: dict[str, Any] | None = None
    rate_limit_per_second: int | None = Field(None, gt=0)
    auth_method: ApiAuthMethod | None = None
    response_path: str | None = Field(None, max_length=500)


class DatasetStreamConfigModel(AuditMixin):
    """Streaming/CDC source configuration for a dataset."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    dataset_id: int = Field(...)
    topic: str = Field(..., min_length=1, max_length=500)
    consumer_group: str | None = Field(None, max_length=200)
    offset_strategy: OffsetStrategy
    specific_offset: str | None = Field(None, max_length=200)
    batch_size: int | None = Field(None, gt=0)

    @model_validator(mode="after")
    def validate_specific_offset(self) -> "DatasetStreamConfigModel":
        if (
            self.offset_strategy == OffsetStrategy.SPECIFIC
            and not self.specific_offset
        ):
            raise ValueError(
                "specific_offset is required when offset_strategy is 'specific'"
            )
        return self


# ---------------------------------------------------------------------------
# Execution Tracking Tables
# ---------------------------------------------------------------------------


class ProcessExecutionModel(AuditMixin):
    """Tracks a single execution of a process."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    process_id: int = Field(...)
    environment: Environment
    status: ExecutionStatus = ExecutionStatus.PENDING
    triggered_by: TriggerType
    start_time: datetime | None = None
    end_time: datetime | None = None
    parameters: dict[str, Any] | None = None
    error_message: str | None = None
    total_jobs: int | None = None
    completed_jobs: int | None = None
    failed_jobs: int | None = None


class JobExecutionModel(AuditMixin):
    """Tracks a single execution of a job."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    process_execution_id: int = Field(...)
    job_id: int = Field(...)
    status: ExecutionStatus = ExecutionStatus.PENDING
    start_time: datetime | None = None
    end_time: datetime | None = None
    error_message: str | None = None
    total_datasets: int | None = None
    completed_datasets: int | None = None
    failed_datasets: int | None = None


class DatasetExecutionModel(AuditMixin):
    """Tracks a single execution of a dataset."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    job_execution_id: int = Field(...)
    dataset_id: int = Field(...)
    status: ExecutionStatus = ExecutionStatus.PENDING
    start_time: datetime | None = None
    end_time: datetime | None = None
    rows_read: int | None = Field(None, ge=0)
    rows_written: int | None = Field(None, ge=0)
    rows_errored: int | None = Field(None, ge=0)
    bytes_processed: int | None = Field(None, ge=0)
    error_message: str | None = None
    retry_count: int = Field(0, ge=0)
    execution_duration_seconds: int | None = Field(None, ge=0)


class WatermarkModel(AuditMixin):
    """Tracks the last successful watermark value for incremental loads."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    dataset_id: int = Field(...)
    environment: Environment
    watermark_column: str = Field(..., min_length=1, max_length=200)
    last_watermark_value: str = Field(..., min_length=1, max_length=500)
    last_successful_execution_id: int | None = None


# ---------------------------------------------------------------------------
# Supporting Tables
# ---------------------------------------------------------------------------


class HookModel(AuditMixin):
    """Pre/post execution hook for a process, job, or dataset."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    entity_type: EntityType
    entity_id: int = Field(...)
    hook_type: HookType
    execution_order: int = Field(1, ge=1)
    action_type: ActionType
    action_config: dict[str, Any] = Field(...)
    on_status: HookOnStatus = HookOnStatus.ANY
    is_enabled: bool = True
    is_deleted: bool = False


class TagModel(AuditMixin):
    """Key-value tag for a process, job, or dataset."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    entity_type: EntityType
    entity_id: int = Field(...)
    tag_key: str = Field(..., min_length=1, max_length=100)
    tag_value: str = Field(..., min_length=1, max_length=500)


class DatasetLineageModel(AuditMixin):
    """Tracks lineage between datasets (source → target)."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    source_dataset_id: int = Field(...)
    target_dataset_id: int = Field(...)
    description: str | None = Field(None, max_length=1000)


class SystemConfigModel(AuditMixin):
    """Framework-level configuration key-value pair."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None
    config_key: str = Field(..., min_length=1, max_length=200)
    config_value: str = Field(..., max_length=2000)
    description: str | None = Field(None, max_length=1000)
