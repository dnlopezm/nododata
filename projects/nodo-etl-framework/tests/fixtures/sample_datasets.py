"""Factory functions for creating test dataset metadata."""

from nodo_etl.core.enums import (
    ApiAuthMethod,
    Compression,
    FileFormat,
    IdempotencyStrategy,
    Layer,
    LoadStrategy,
    OffsetStrategy,
    PaginationStrategy,
    SourceType,
)
from nodo_etl.core.models import (
    DatasetApiConfigModel,
    DatasetDbConfigModel,
    DatasetFileConfigModel,
    DatasetModel,
    DatasetStreamConfigModel,
)


def make_dataset(**overrides) -> DatasetModel:
    """Create a DatasetModel with sensible defaults (database source)."""
    defaults = {
        "job_id": 1,
        "dataset_name": "transactions_bronze",
        "description": "Raw transactions from source DB",
        "source_type": SourceType.DATABASE,
        "source_connection_id": 1,
        "layer": Layer.BRONZE,
        "load_strategy": LoadStrategy.INCREMENTAL,
        "idempotency_strategy": IdempotencyStrategy.OVERWRITE,
        "execution_order": 1,
        "max_retries": 3,
        "retry_delay_seconds": 60,
        "timeout_seconds": 3600,
        "is_enabled": True,
        "is_deleted": False,
    }
    defaults.update(overrides)
    return DatasetModel(**defaults)


def make_db_config(**overrides) -> DatasetDbConfigModel:
    """Create a DatasetDbConfigModel with sensible defaults."""
    defaults = {
        "dataset_id": 1,
        "source_schema": "dbo",
        "source_table": "transactions",
        "extraction_query": (
            "SELECT * FROM dbo.transactions "
            "WHERE updated_at > '{{watermark}}'"
        ),
        "watermark_column": "updated_at",
        "primary_key_columns": "id",
    }
    defaults.update(overrides)
    return DatasetDbConfigModel(**defaults)


def make_file_config(**overrides) -> DatasetFileConfigModel:
    """Create a DatasetFileConfigModel with sensible defaults."""
    defaults = {
        "dataset_id": 1,
        "file_path_pattern": "/data/{yyyy}/{MM}/{dd}/transactions_*.parquet",
        "file_format": FileFormat.PARQUET,
        "compression": Compression.SNAPPY,
    }
    defaults.update(overrides)
    return DatasetFileConfigModel(**defaults)


def make_api_config(**overrides) -> DatasetApiConfigModel:
    """Create a DatasetApiConfigModel with sensible defaults."""
    defaults = {
        "dataset_id": 1,
        "api_url": "https://api.example.com/v1/exchange-rates",
        "api_method": "GET",
        "api_headers": {"Authorization": "Bearer {{token}}"},
        "pagination_strategy": PaginationStrategy.OFFSET,
        "pagination_config": {"page_size": 100, "offset_param": "offset"},
        "rate_limit_per_second": 10,
        "auth_method": ApiAuthMethod.BEARER,
        "response_path": "$.data.results",
    }
    defaults.update(overrides)
    return DatasetApiConfigModel(**defaults)


def make_stream_config(**overrides) -> DatasetStreamConfigModel:
    """Create a DatasetStreamConfigModel with sensible defaults."""
    defaults = {
        "dataset_id": 1,
        "topic": "finance.transactions",
        "consumer_group": "nodo-etl-bronze",
        "offset_strategy": OffsetStrategy.LATEST,
        "batch_size": 1000,
    }
    defaults.update(overrides)
    return DatasetStreamConfigModel(**defaults)
