"""Factory functions for creating test job metadata."""

from nodo_etl.core.models import JobModel


def make_job(**overrides) -> JobModel:
    """Create a JobModel with sensible defaults."""
    defaults = {
        "process_id": 1,
        "job_name": "bronze_ingestion",
        "description": "Ingest raw data into bronze layer",
        "execution_order": 1,
        "max_parallelism": 5,
        "is_enabled": True,
        "is_deleted": False,
    }
    defaults.update(overrides)
    return JobModel(**defaults)
