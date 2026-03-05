"""E2E test configuration.

These tests require the full Docker stack (Airflow + databases).
Enable with NODO_ETL_E2E_TESTS=1.
"""

import os
import uuid

import pytest

from nodo_etl.config.settings import Settings
from nodo_etl.db.connection import MetadataDBConnection
from nodo_etl.db.repositories import (
    ConnectionRepository,
    DatasetRepository,
    ExecutionRepository,
    HookRepository,
    JobRepository,
    ProcessRepository,
    TagRepository,
)

E2E_ENABLED = os.environ.get("NODO_ETL_E2E_TESTS", "0") == "1"

skip_e2e = pytest.mark.skipif(
    not E2E_ENABLED,
    reason="E2E tests disabled. Set NODO_ETL_E2E_TESTS=1 to enable.",
)


@pytest.fixture
def db_connection(monkeypatch):
    """Create a PostgreSQL connection for E2E tests."""
    monkeypatch.setenv("NODO_ETL_DB_TYPE", "postgresql")
    monkeypatch.setenv("NODO_ETL_PG_HOST", "localhost")
    monkeypatch.setenv("NODO_ETL_PG_PORT", "5433")
    monkeypatch.setenv("NODO_ETL_PG_DB", "nodo_etl_db")
    monkeypatch.setenv("NODO_ETL_PG_USER", "nodo_etl")
    monkeypatch.setenv("NODO_ETL_PG_PASSWORD", "nodo_etl_password")
    settings = Settings(_env_file=None)
    conn = MetadataDBConnection(settings)
    yield conn
    conn.close()


@pytest.fixture
def repos(db_connection):
    """Create all repository instances."""
    return {
        "process": ProcessRepository(db_connection),
        "job": JobRepository(db_connection),
        "dataset": DatasetRepository(db_connection),
        "connection": ConnectionRepository(db_connection),
        "execution": ExecutionRepository(db_connection),
        "hook": HookRepository(db_connection),
        "tag": TagRepository(db_connection),
    }


@pytest.fixture
def unique_id():
    """Generate unique suffix for test data."""
    return uuid.uuid4().hex[:8]


@pytest.fixture
def sample_connection(repos, unique_id):
    """Create a sample connection for test datasets."""
    return repos["connection"].create({
        "connection_name": f"e2e_conn_{unique_id}",
        "connection_type": "postgresql",
        "host": "localhost",
        "port": 5433,
        "environment": "dev",
    })


@pytest.fixture
def sample_process(repos, unique_id):
    """Create a sample process."""
    return repos["process"].create({
        "process_name": f"e2e_proc_{unique_id}",
        "execution_order": 1,
        "max_parallelism": 5,
    })


@pytest.fixture
def full_pipeline(repos, unique_id, sample_connection, sample_process):
    """Create a full pipeline: process -> 3 jobs -> datasets."""
    conn_id = sample_connection["id"]
    proc_id = sample_process["id"]

    bronze_job = repos["job"].create({
        "process_id": proc_id,
        "job_name": f"e2e_bronze_{unique_id}",
        "execution_order": 1,
        "max_parallelism": 3,
    })
    silver_job = repos["job"].create({
        "process_id": proc_id,
        "job_name": f"e2e_silver_{unique_id}",
        "execution_order": 2,
    })
    gold_job = repos["job"].create({
        "process_id": proc_id,
        "job_name": f"e2e_gold_{unique_id}",
        "execution_order": 3,
    })

    ds1 = repos["dataset"].create(
        {"job_id": bronze_job["id"], "dataset_name": f"e2e_txn_bronze_{unique_id}",
         "source_type": "database", "source_connection_id": conn_id,
         "layer": "bronze", "load_strategy": "incremental",
         "idempotency_strategy": "upsert", "execution_order": 1},
        source_config={"source_schema": "fin", "source_table": "transactions",
                      "watermark_column": "updated_at"},
    )
    ds2 = repos["dataset"].create(
        {"job_id": bronze_job["id"], "dataset_name": f"e2e_acct_bronze_{unique_id}",
         "source_type": "database", "source_connection_id": conn_id,
         "layer": "bronze", "load_strategy": "full",
         "idempotency_strategy": "overwrite", "execution_order": 1},
        source_config={"source_schema": "fin", "source_table": "accounts"},
    )
    ds3 = repos["dataset"].create(
        {"job_id": silver_job["id"], "dataset_name": f"e2e_txn_silver_{unique_id}",
         "source_type": "database", "source_connection_id": conn_id,
         "layer": "silver", "load_strategy": "incremental",
         "idempotency_strategy": "upsert", "execution_order": 1},
        source_config={"source_schema": "silver", "source_table": "transactions",
                      "watermark_column": "updated_at"},
    )
    ds4 = repos["dataset"].create(
        {"job_id": gold_job["id"], "dataset_name": f"e2e_rev_gold_{unique_id}",
         "source_type": "database", "source_connection_id": conn_id,
         "layer": "gold", "load_strategy": "full",
         "idempotency_strategy": "overwrite", "execution_order": 1},
        source_config={"source_schema": "gold", "source_table": "daily_revenue"},
    )

    return {
        "process": sample_process,
        "jobs": [bronze_job, silver_job, gold_job],
        "datasets": [ds1, ds2, ds3, ds4],
        "connection": sample_connection,
    }
