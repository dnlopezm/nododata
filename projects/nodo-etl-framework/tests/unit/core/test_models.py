"""Tests for all Pydantic models."""

from datetime import datetime

import pytest
from pydantic import ValidationError

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
from nodo_etl.core.models import (
    ConnectionModel,
    DatasetApiConfigModel,
    DatasetDbConfigModel,
    DatasetExecutionModel,
    DatasetFileConfigModel,
    DatasetLineageModel,
    DatasetModel,
    DatasetStreamConfigModel,
    HookModel,
    JobExecutionModel,
    JobModel,
    ProcessExecutionModel,
    ProcessModel,
    ScheduleModel,
    SystemConfigModel,
    TagModel,
    WatermarkModel,
)


# -----------------------------------------------------------------------
# ProcessModel
# -----------------------------------------------------------------------
class TestProcessModel:
    def test_create_with_all_fields(self):
        p = ProcessModel(
            id=1,
            process_name="finance_etl",
            description="Finance pipeline",
            max_parallelism=5,
            execution_order=2,
            max_retries=3,
            timeout_seconds=7200,
            is_enabled=True,
            is_deleted=False,
        )
        assert p.process_name == "finance_etl"
        assert p.max_parallelism == 5
        assert p.execution_order == 2

    def test_create_with_only_required_fields(self):
        p = ProcessModel(process_name="test")
        assert p.is_enabled is True
        assert p.is_deleted is False
        assert p.execution_order == 1
        assert p.max_parallelism is None
        assert p.max_retries is None
        assert p.timeout_seconds is None

    def test_empty_process_name_raises(self):
        with pytest.raises(ValidationError):
            ProcessModel(process_name="")

    def test_process_name_too_long_raises(self):
        with pytest.raises(ValidationError):
            ProcessModel(process_name="x" * 201)

    def test_negative_max_parallelism_raises(self):
        with pytest.raises(ValidationError):
            ProcessModel(process_name="test", max_parallelism=-1)

    def test_zero_max_parallelism_raises(self):
        with pytest.raises(ValidationError):
            ProcessModel(process_name="test", max_parallelism=0)

    def test_execution_order_zero_raises(self):
        with pytest.raises(ValidationError):
            ProcessModel(process_name="test", execution_order=0)

    def test_audit_fields_optional(self):
        p = ProcessModel(process_name="test")
        assert p.created_at is None
        assert p.created_by is None
        assert p.updated_at is None
        assert p.updated_by is None

    def test_audit_fields_set(self):
        now = datetime.now()
        p = ProcessModel(
            process_name="test",
            created_at=now,
            created_by="admin",
            updated_at=now,
            updated_by="admin",
        )
        assert p.created_at == now
        assert p.created_by == "admin"

    def test_model_dump_roundtrip(self):
        p = ProcessModel(process_name="test", max_parallelism=3)
        data = p.model_dump()
        p2 = ProcessModel.model_validate(data)
        assert p == p2

    def test_frozen_model(self):
        p = ProcessModel(process_name="test")
        with pytest.raises(ValidationError):
            p.process_name = "changed"


# -----------------------------------------------------------------------
# ScheduleModel
# -----------------------------------------------------------------------
class TestScheduleModel:
    def test_create_with_valid_cron(self):
        s = ScheduleModel(
            process_id=1,
            schedule_name="daily",
            cron_expression="0 6 * * *",
        )
        assert s.cron_expression == "0 6 * * *"

    def test_empty_cron_raises(self):
        with pytest.raises(ValidationError):
            ScheduleModel(
                process_id=1,
                schedule_name="daily",
                cron_expression="",
            )

    def test_defaults(self):
        s = ScheduleModel(
            process_id=1,
            schedule_name="hourly",
            cron_expression="0 * * * *",
        )
        assert s.is_enabled is True
        assert s.is_deleted is False

    def test_model_dump_roundtrip(self):
        s = ScheduleModel(
            process_id=1,
            schedule_name="daily",
            cron_expression="0 6 * * *",
        )
        data = s.model_dump()
        s2 = ScheduleModel.model_validate(data)
        assert s == s2


# -----------------------------------------------------------------------
# JobModel
# -----------------------------------------------------------------------
class TestJobModel:
    def test_create_with_all_fields(self):
        j = JobModel(
            process_id=1,
            job_name="bronze_ingestion",
            description="Ingest into bronze",
            execution_order=1,
            max_parallelism=5,
            max_retries=3,
            timeout_seconds=3600,
        )
        assert j.job_name == "bronze_ingestion"

    def test_execution_order_zero_raises(self):
        with pytest.raises(ValidationError):
            JobModel(process_id=1, job_name="test", execution_order=0)

    def test_missing_process_id_raises(self):
        with pytest.raises(ValidationError):
            JobModel(job_name="test")

    def test_defaults(self):
        j = JobModel(process_id=1, job_name="test")
        assert j.execution_order == 1
        assert j.is_enabled is True
        assert j.is_deleted is False

    def test_model_dump_roundtrip(self):
        j = JobModel(process_id=1, job_name="test")
        data = j.model_dump()
        j2 = JobModel.model_validate(data)
        assert j == j2


# -----------------------------------------------------------------------
# DatasetModel
# -----------------------------------------------------------------------
class TestDatasetModel:
    def test_create_database_source(self):
        d = DatasetModel(
            job_id=1,
            dataset_name="transactions_bronze",
            source_type=SourceType.DATABASE,
            source_connection_id=1,
            layer=Layer.BRONZE,
            load_strategy=LoadStrategy.FULL,
            idempotency_strategy=IdempotencyStrategy.OVERWRITE,
        )
        assert d.source_type == SourceType.DATABASE
        assert d.layer == Layer.BRONZE

    def test_invalid_source_type_raises(self):
        with pytest.raises(ValidationError):
            DatasetModel(
                job_id=1,
                dataset_name="test",
                source_type="invalid",
                source_connection_id=1,
                layer=Layer.BRONZE,
                load_strategy=LoadStrategy.FULL,
                idempotency_strategy=IdempotencyStrategy.OVERWRITE,
            )

    def test_invalid_layer_raises(self):
        with pytest.raises(ValidationError):
            DatasetModel(
                job_id=1,
                dataset_name="test",
                source_type=SourceType.DATABASE,
                source_connection_id=1,
                layer="platinum",
                load_strategy=LoadStrategy.FULL,
                idempotency_strategy=IdempotencyStrategy.OVERWRITE,
            )

    def test_negative_retry_delay_raises(self):
        with pytest.raises(ValidationError):
            DatasetModel(
                job_id=1,
                dataset_name="test",
                source_type=SourceType.DATABASE,
                source_connection_id=1,
                layer=Layer.BRONZE,
                load_strategy=LoadStrategy.FULL,
                idempotency_strategy=IdempotencyStrategy.OVERWRITE,
                retry_delay_seconds=-1,
            )

    def test_defaults(self):
        d = DatasetModel(
            job_id=1,
            dataset_name="test",
            source_type=SourceType.DATABASE,
            source_connection_id=1,
            layer=Layer.BRONZE,
            load_strategy=LoadStrategy.FULL,
            idempotency_strategy=IdempotencyStrategy.OVERWRITE,
        )
        assert d.execution_order == 1
        assert d.is_enabled is True
        assert d.target_connection_id is None

    def test_model_dump_roundtrip(self):
        d = DatasetModel(
            job_id=1,
            dataset_name="test",
            source_type=SourceType.DATABASE,
            source_connection_id=1,
            layer=Layer.BRONZE,
            load_strategy=LoadStrategy.FULL,
            idempotency_strategy=IdempotencyStrategy.OVERWRITE,
        )
        data = d.model_dump()
        d2 = DatasetModel.model_validate(data)
        assert d == d2


# -----------------------------------------------------------------------
# ConnectionModel
# -----------------------------------------------------------------------
class TestConnectionModel:
    def test_create_with_all_fields(self):
        c = ConnectionModel(
            connection_name="finance_db",
            connection_type=ConnectionType.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="finance",
            schema_name="public",
            secret_reference="DB_PASS",
            secret_provider_type=SecretProviderType.ENV,
            additional_params={"sslmode": "require"},
            environment=Environment.DEV,
        )
        assert c.connection_name == "finance_db"

    def test_host_nullable(self):
        c = ConnectionModel(
            connection_name="test",
            connection_type=ConnectionType.SQLSERVER,
            environment=Environment.DEV,
        )
        assert c.host is None

    def test_invalid_environment_raises(self):
        with pytest.raises(ValidationError):
            ConnectionModel(
                connection_name="test",
                connection_type=ConnectionType.SQLSERVER,
                environment="invalid",
            )

    def test_additional_params_accepts_dict(self):
        c = ConnectionModel(
            connection_name="test",
            connection_type=ConnectionType.API,
            additional_params={"timeout": 30, "verify_ssl": True},
            environment=Environment.PROD,
        )
        assert c.additional_params["timeout"] == 30

    def test_repr_does_not_expose_secret(self):
        c = ConnectionModel(
            connection_name="test",
            connection_type=ConnectionType.POSTGRESQL,
            secret_reference="SUPER_SECRET_KEY",
            environment=Environment.DEV,
        )
        repr_str = repr(c)
        assert "SUPER_SECRET_KEY" not in repr_str

    def test_str_does_not_expose_secret(self):
        c = ConnectionModel(
            connection_name="test",
            connection_type=ConnectionType.POSTGRESQL,
            secret_reference="SUPER_SECRET_KEY",
            environment=Environment.DEV,
        )
        str_str = str(c)
        assert "SUPER_SECRET_KEY" not in str_str

    def test_port_validation(self):
        with pytest.raises(ValidationError):
            ConnectionModel(
                connection_name="test",
                connection_type=ConnectionType.POSTGRESQL,
                port=70000,
                environment=Environment.DEV,
            )

    def test_model_dump_roundtrip(self):
        c = ConnectionModel(
            connection_name="test",
            connection_type=ConnectionType.POSTGRESQL,
            environment=Environment.DEV,
        )
        data = c.model_dump()
        c2 = ConnectionModel.model_validate(data)
        assert c == c2


# -----------------------------------------------------------------------
# Source Config Models
# -----------------------------------------------------------------------
class TestDatasetDbConfigModel:
    def test_create_with_watermark(self):
        cfg = DatasetDbConfigModel(
            dataset_id=1,
            source_schema="dbo",
            source_table="transactions",
            extraction_query="SELECT * FROM dbo.transactions WHERE updated_at > '{{watermark}}'",
            watermark_column="updated_at",
            primary_key_columns="id",
        )
        assert "{{watermark}}" in cfg.extraction_query

    def test_all_fields_nullable(self):
        cfg = DatasetDbConfigModel(dataset_id=1)
        assert cfg.source_schema is None
        assert cfg.source_table is None
        assert cfg.extraction_query is None

    def test_model_dump_roundtrip(self):
        cfg = DatasetDbConfigModel(dataset_id=1, source_table="test")
        data = cfg.model_dump()
        cfg2 = DatasetDbConfigModel.model_validate(data)
        assert cfg == cfg2


class TestDatasetFileConfigModel:
    def test_create_parquet(self):
        cfg = DatasetFileConfigModel(
            dataset_id=1,
            file_path_pattern="/data/*.parquet",
            file_format=FileFormat.PARQUET,
        )
        assert cfg.file_format == FileFormat.PARQUET

    def test_invalid_file_format_raises(self):
        with pytest.raises(ValidationError):
            DatasetFileConfigModel(
                dataset_id=1,
                file_path_pattern="/data/*.xyz",
                file_format="xyz",
            )

    def test_defaults(self):
        cfg = DatasetFileConfigModel(
            dataset_id=1,
            file_path_pattern="/data/*.csv",
            file_format=FileFormat.CSV,
        )
        assert cfg.has_header is True
        assert cfg.encoding == "utf-8"
        assert cfg.delimiter is None


class TestDatasetApiConfigModel:
    def test_create_with_pagination(self):
        cfg = DatasetApiConfigModel(
            dataset_id=1,
            api_url="https://api.example.com/data",
            api_method="GET",
            pagination_strategy=PaginationStrategy.CURSOR,
            pagination_config={"cursor_field": "next_cursor"},
            auth_method=ApiAuthMethod.BEARER,
        )
        assert cfg.pagination_strategy == PaginationStrategy.CURSOR

    def test_invalid_method_raises(self):
        with pytest.raises(ValidationError):
            DatasetApiConfigModel(
                dataset_id=1,
                api_url="https://api.example.com",
                api_method="DELETE",
            )

    def test_headers_accept_dict(self):
        cfg = DatasetApiConfigModel(
            dataset_id=1,
            api_url="https://api.example.com",
            api_method="POST",
            api_headers={"Content-Type": "application/json"},
        )
        assert cfg.api_headers["Content-Type"] == "application/json"


class TestDatasetStreamConfigModel:
    def test_create_latest(self):
        cfg = DatasetStreamConfigModel(
            dataset_id=1,
            topic="transactions",
            offset_strategy=OffsetStrategy.LATEST,
        )
        assert cfg.offset_strategy == OffsetStrategy.LATEST

    def test_specific_without_offset_raises(self):
        with pytest.raises(ValidationError):
            DatasetStreamConfigModel(
                dataset_id=1,
                topic="transactions",
                offset_strategy=OffsetStrategy.SPECIFIC,
                specific_offset=None,
            )

    def test_specific_with_offset_ok(self):
        cfg = DatasetStreamConfigModel(
            dataset_id=1,
            topic="transactions",
            offset_strategy=OffsetStrategy.SPECIFIC,
            specific_offset="12345",
        )
        assert cfg.specific_offset == "12345"

    def test_earliest_without_offset_ok(self):
        cfg = DatasetStreamConfigModel(
            dataset_id=1,
            topic="transactions",
            offset_strategy=OffsetStrategy.EARLIEST,
        )
        assert cfg.specific_offset is None


# -----------------------------------------------------------------------
# Execution Models
# -----------------------------------------------------------------------
class TestProcessExecutionModel:
    def test_create(self):
        pe = ProcessExecutionModel(
            process_id=1,
            environment=Environment.DEV,
            triggered_by=TriggerType.MANUAL,
        )
        assert pe.status == ExecutionStatus.PENDING

    def test_parameters_json(self):
        pe = ProcessExecutionModel(
            process_id=1,
            environment=Environment.PROD,
            triggered_by=TriggerType.SCHEDULE,
            parameters={"report_date": "2026-03-01", "full_refresh": True},
        )
        assert pe.parameters["full_refresh"] is True


class TestJobExecutionModel:
    def test_create(self):
        je = JobExecutionModel(
            process_execution_id=1,
            job_id=1,
        )
        assert je.status == ExecutionStatus.PENDING
        assert je.total_datasets is None


class TestDatasetExecutionModel:
    def test_create(self):
        de = DatasetExecutionModel(
            job_execution_id=1,
            dataset_id=1,
        )
        assert de.retry_count == 0
        assert de.rows_read is None

    def test_negative_rows_raises(self):
        with pytest.raises(ValidationError):
            DatasetExecutionModel(
                job_execution_id=1,
                dataset_id=1,
                rows_read=-1,
            )

    def test_with_results(self):
        de = DatasetExecutionModel(
            job_execution_id=1,
            dataset_id=1,
            status=ExecutionStatus.SUCCESS,
            rows_read=1000,
            rows_written=950,
            rows_errored=50,
            bytes_processed=1048576,
            execution_duration_seconds=120,
        )
        assert de.rows_read == 1000
        assert de.bytes_processed == 1048576


class TestWatermarkModel:
    def test_create(self):
        w = WatermarkModel(
            dataset_id=1,
            environment=Environment.DEV,
            watermark_column="updated_at",
            last_watermark_value="2026-03-01 00:00:00",
        )
        assert w.last_watermark_value == "2026-03-01 00:00:00"

    def test_model_dump_roundtrip(self):
        w = WatermarkModel(
            dataset_id=1,
            environment=Environment.DEV,
            watermark_column="updated_at",
            last_watermark_value="2026-03-01",
        )
        data = w.model_dump()
        w2 = WatermarkModel.model_validate(data)
        assert w == w2


# -----------------------------------------------------------------------
# Supporting Models
# -----------------------------------------------------------------------
class TestHookModel:
    def test_create_pre_sql(self):
        h = HookModel(
            entity_type=EntityType.JOB,
            entity_id=1,
            hook_type=HookType.PRE,
            action_type=ActionType.SQL,
            action_config={"query": "TRUNCATE TABLE staging.transactions"},
        )
        assert h.on_status == HookOnStatus.ANY

    def test_invalid_on_status_raises(self):
        with pytest.raises(ValidationError):
            HookModel(
                entity_type=EntityType.JOB,
                entity_id=1,
                hook_type=HookType.POST,
                action_type=ActionType.SQL,
                action_config={"query": "SELECT 1"},
                on_status="invalid",
            )

    def test_defaults(self):
        h = HookModel(
            entity_type=EntityType.PROCESS,
            entity_id=1,
            hook_type=HookType.POST,
            action_type=ActionType.API,
            action_config={"url": "https://hooks.example.com"},
        )
        assert h.is_enabled is True
        assert h.is_deleted is False
        assert h.execution_order == 1


class TestTagModel:
    def test_create(self):
        t = TagModel(
            entity_type=EntityType.PROCESS,
            entity_id=1,
            tag_key="domain",
            tag_value="finance",
        )
        assert t.tag_key == "domain"

    def test_invalid_entity_type_raises(self):
        with pytest.raises(ValidationError):
            TagModel(
                entity_type="invalid",
                entity_id=1,
                tag_key="domain",
                tag_value="finance",
            )

    def test_empty_key_raises(self):
        with pytest.raises(ValidationError):
            TagModel(
                entity_type=EntityType.PROCESS,
                entity_id=1,
                tag_key="",
                tag_value="finance",
            )


class TestDatasetLineageModel:
    def test_create(self):
        l = DatasetLineageModel(
            source_dataset_id=1,
            target_dataset_id=2,
            description="Bronze to silver transformation",
        )
        assert l.source_dataset_id == 1
        assert l.target_dataset_id == 2


class TestSystemConfigModel:
    def test_create(self):
        sc = SystemConfigModel(
            config_key="default_max_retries",
            config_value="3",
            description="Default retry count",
        )
        assert sc.config_key == "default_max_retries"
        assert sc.config_value == "3"

    def test_empty_key_raises(self):
        with pytest.raises(ValidationError):
            SystemConfigModel(config_key="", config_value="3")


# -----------------------------------------------------------------------
# Cross-cutting tests
# -----------------------------------------------------------------------
class TestAllModelsDumpRoundtrip:
    """Verify all models survive dump → validate round trips."""

    @pytest.mark.parametrize(
        "model_instance",
        [
            ProcessModel(process_name="test"),
            ScheduleModel(process_id=1, schedule_name="d", cron_expression="0 * * * *"),
            JobModel(process_id=1, job_name="test"),
            DatasetModel(
                job_id=1,
                dataset_name="test",
                source_type=SourceType.DATABASE,
                source_connection_id=1,
                layer=Layer.BRONZE,
                load_strategy=LoadStrategy.FULL,
                idempotency_strategy=IdempotencyStrategy.OVERWRITE,
            ),
            ConnectionModel(
                connection_name="test",
                connection_type=ConnectionType.POSTGRESQL,
                environment=Environment.DEV,
            ),
            DatasetDbConfigModel(dataset_id=1),
            DatasetFileConfigModel(
                dataset_id=1,
                file_path_pattern="/data/*.csv",
                file_format=FileFormat.CSV,
            ),
            DatasetApiConfigModel(
                dataset_id=1,
                api_url="https://api.example.com",
                api_method="GET",
            ),
            DatasetStreamConfigModel(
                dataset_id=1,
                topic="test",
                offset_strategy=OffsetStrategy.EARLIEST,
            ),
            ProcessExecutionModel(
                process_id=1,
                environment=Environment.DEV,
                triggered_by=TriggerType.MANUAL,
            ),
            JobExecutionModel(process_execution_id=1, job_id=1),
            DatasetExecutionModel(job_execution_id=1, dataset_id=1),
            WatermarkModel(
                dataset_id=1,
                environment=Environment.DEV,
                watermark_column="updated_at",
                last_watermark_value="2026-01-01",
            ),
            HookModel(
                entity_type=EntityType.JOB,
                entity_id=1,
                hook_type=HookType.PRE,
                action_type=ActionType.SQL,
                action_config={"query": "SELECT 1"},
            ),
            TagModel(
                entity_type=EntityType.PROCESS,
                entity_id=1,
                tag_key="domain",
                tag_value="finance",
            ),
            DatasetLineageModel(source_dataset_id=1, target_dataset_id=2),
            SystemConfigModel(config_key="test_key", config_value="test_value"),
        ],
    )
    def test_roundtrip(self, model_instance):
        data = model_instance.model_dump()
        rebuilt = type(model_instance).model_validate(data)
        assert model_instance == rebuilt
