"""Factory functions for creating test execution metadata."""

from datetime import datetime, timedelta

from nodo_etl.core.enums import Environment, ExecutionStatus, TriggerType
from nodo_etl.core.models import (
    DatasetExecutionModel,
    JobExecutionModel,
    ProcessExecutionModel,
)


def make_process_execution(**overrides) -> ProcessExecutionModel:
    """Create a ProcessExecutionModel with sensible defaults."""
    now = datetime.now()
    defaults = {
        "process_id": 1,
        "environment": Environment.DEV,
        "status": ExecutionStatus.RUNNING,
        "triggered_by": TriggerType.MANUAL,
        "start_time": now,
        "parameters": {"report_date": "2026-03-01"},
    }
    defaults.update(overrides)
    return ProcessExecutionModel(**defaults)


def make_job_execution(**overrides) -> JobExecutionModel:
    """Create a JobExecutionModel with sensible defaults."""
    defaults = {
        "process_execution_id": 1,
        "job_id": 1,
        "status": ExecutionStatus.PENDING,
    }
    defaults.update(overrides)
    return JobExecutionModel(**defaults)


def make_dataset_execution(**overrides) -> DatasetExecutionModel:
    """Create a DatasetExecutionModel with sensible defaults."""
    defaults = {
        "job_execution_id": 1,
        "dataset_id": 1,
        "status": ExecutionStatus.PENDING,
        "retry_count": 0,
    }
    defaults.update(overrides)
    return DatasetExecutionModel(**defaults)
