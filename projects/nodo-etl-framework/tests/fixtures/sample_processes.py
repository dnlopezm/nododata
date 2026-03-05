"""Factory functions for creating test process metadata."""

from nodo_etl.core.models import ProcessModel, ScheduleModel


def make_process(**overrides) -> ProcessModel:
    """Create a ProcessModel with sensible defaults."""
    defaults = {
        "process_name": "test_finance_etl",
        "description": "Test finance ETL process",
        "max_parallelism": 3,
        "execution_order": 1,
        "is_enabled": True,
        "is_deleted": False,
    }
    defaults.update(overrides)
    return ProcessModel(**defaults)


def make_schedule(**overrides) -> ScheduleModel:
    """Create a ScheduleModel with sensible defaults."""
    defaults = {
        "process_id": 1,
        "schedule_name": "daily_morning",
        "cron_expression": "0 6 * * *",
        "is_enabled": True,
        "is_deleted": False,
    }
    defaults.update(overrides)
    return ScheduleModel(**defaults)
