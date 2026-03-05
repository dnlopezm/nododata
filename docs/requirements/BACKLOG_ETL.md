# Nodo ETL Framework - Detailed Backlog

Metadata-driven ETL framework for orchestrating data pipelines across bronze, silver, and gold layers.

**Last Updated:** March 4, 2026

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION LAYER                              │
│                     (Airflow / ADF / Databricks)                        │
│                              │                                          │
│                    Reads metadata from DB                                │
│                    Triggers pipelines by schedule                        │
│                    Controls parallelism & order                          │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────┐
│                      METADATA DATABASE                                  │
│                   (SQL Server / PostgreSQL)                              │
│                                                                         │
│   ┌─────────┐    ┌─────────┐    ┌───────────┐    ┌──────────────────┐  │
│   │ Process │───▶│   Job   │───▶│  Dataset   │───▶│ Source Config    │  │
│   │         │    │         │    │            │    │ (DB/File/API/    │  │
│   │schedule │    │  order  │    │ load_type  │    │  Stream)         │  │
│   │parallel │    │ parallel│    │ layer      │    │                  │  │
│   └─────────┘    └─────────┘    └───────────┘    └──────────────────┘  │
│        │              │              │                                   │
│   ┌─────────┐    ┌─────────┐    ┌───────────┐                          │
│   │Process  │    │  Job    │    │ Dataset   │    Execution Tracking     │
│   │Execution│───▶│Execution│───▶│ Execution │                          │
│   └─────────┘    └─────────┘    └───────────┘                          │
│                                                                         │
│   ┌─────────────┐  ┌────────┐  ┌────────┐  ┌───────────────────────┐  │
│   │ Connections  │  │  Tags  │  │ Hooks  │  │  System Config        │  │
│   │ + Secrets    │  │        │  │pre/post│  │  (defaults, settings) │  │
│   └─────────────┘  └────────┘  └────────┘  └───────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────┐
│                        DATA LAYER                                       │
│                                                                         │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐                         │
│   │  BRONZE  │───▶│  SILVER  │───▶│   GOLD   │                         │
│   │ (raw)    │    │ (clean)  │    │ (business)│                         │
│   └──────────┘    └──────────┘    └──────────┘                         │
│                                                                         │
│   Sources: SQL Server, PostgreSQL, CSV, Parquet, Delta, JSON, APIs,    │
│            Kafka, CDC                                                   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Data Model Hierarchy

```
Process (Finance ETL)
├── Schedule (cron: 0 6 * * *)
├── Schedule (cron: 0 */2 * * *)
│
├── Job: Bronze Ingestion (order=1)
│   ├── Dataset: transactions_bronze (order=1, source=db, incremental)
│   ├── Dataset: accounts_bronze (order=1, source=db, full)
│   └── Dataset: exchange_rates_bronze (order=2, source=api, full)
│
├── Job: Silver Transformation (order=2)
│   ├── Dataset: transactions_silver (order=1)
│   └── Dataset: accounts_silver (order=1)
│
└── Job: Gold Aggregation (order=3)
    ├── Dataset: daily_revenue_gold (order=1)
    └── Dataset: monthly_summary_gold (order=2)
```

---

## Project Structure

```
projects/nodo-etl-framework/
├── pyproject.toml
├── README.md
│
├── docker/
│   ├── docker-compose.yml              # All services
│   ├── sqlserver/
│   │   └── init/                       # SQL Server init scripts
│   ├── postgresql/
│   │   └── init/                       # PostgreSQL init scripts
│   └── airflow/
│       ├── Dockerfile                  # Custom Airflow image
│       └── dags/                       # Airflow DAGs
│           └── nodo_etl_orchestrator.py
│
├── migrations/
│   ├── sqlserver/
│   │   ├── V1__create_schema.sql
│   │   ├── V2__core_tables.sql
│   │   ├── V3__source_config_tables.sql
│   │   ├── V4__execution_tables.sql
│   │   ├── V5__supporting_tables.sql
│   │   └── V6__stored_procedures.sql
│   └── postgresql/
│       ├── V1__create_schema.sql
│       ├── V2__core_tables.sql
│       ├── V3__source_config_tables.sql
│       ├── V4__execution_tables.sql
│       ├── V5__supporting_tables.sql
│       └── V6__stored_procedures.sql
│
├── src/
│   └── nodo_etl/
│       ├── __init__.py
│       ├── cli/                        # CLI commands
│       │   ├── __init__.py
│       │   ├── main.py                 # CLI entry point
│       │   ├── process_commands.py
│       │   ├── job_commands.py
│       │   ├── dataset_commands.py
│       │   └── connection_commands.py
│       ├── core/                       # Core abstractions
│       │   ├── __init__.py
│       │   ├── models.py              # Pydantic models
│       │   └── enums.py               # Enumerations
│       ├── db/                         # Database layer
│       │   ├── __init__.py
│       │   ├── connection.py          # DB connection manager
│       │   ├── repositories.py        # CRUD operations
│       │   └── dialect.py             # SQL Server / PostgreSQL abstraction
│       ├── secrets/                    # Secret provider
│       │   ├── __init__.py
│       │   ├── base.py                # SecretProvider interface
│       │   ├── env_provider.py        # Environment variables
│       │   └── factory.py             # Provider factory
│       └── config/                     # Configuration
│           ├── __init__.py
│           └── settings.py            # Environment-based config
│
└── tests/
    ├── conftest.py                     # Shared fixtures, DB connections, cleanup
    ├── fixtures/                       # Test data fixtures
    │   ├── __init__.py
    │   ├── sample_processes.py        # Factory functions for test processes
    │   ├── sample_jobs.py             # Factory functions for test jobs
    │   ├── sample_datasets.py         # Factory functions for test datasets
    │   ├── sample_connections.py      # Factory functions for test connections
    │   └── sample_executions.py       # Factory functions for test executions
    ├── unit/
    │   ├── __init__.py
    │   ├── core/
    │   │   ├── test_enums.py          # Enum validation tests
    │   │   └── test_models.py         # Pydantic model tests
    │   ├── config/
    │   │   └── test_settings.py       # Settings loading tests
    │   ├── secrets/
    │   │   ├── test_env_provider.py   # Env secret provider tests
    │   │   └── test_factory.py        # Provider factory tests
    │   ├── db/
    │   │   ├── test_dialect.py        # SQL dialect abstraction tests
    │   │   └── test_connection.py     # Connection manager tests (mocked)
    │   └── cli/
    │       ├── test_process_commands.py
    │       ├── test_job_commands.py
    │       ├── test_dataset_commands.py
    │       ├── test_connection_commands.py
    │       ├── test_execution_commands.py
    │       └── test_utility_commands.py
    ├── integration/
    │   ├── __init__.py
    │   ├── conftest.py                # Integration-specific fixtures (real DB)
    │   ├── db/
    │   │   ├── test_repositories_sqlserver.py
    │   │   ├── test_repositories_postgresql.py
    │   │   └── test_repositories_common.py   # Shared repo test scenarios
    │   ├── migrations/
    │   │   ├── test_migrations_sqlserver.py
    │   │   └── test_migrations_postgresql.py
    │   ├── stored_procedures/
    │   │   ├── test_sp_process_lifecycle.py
    │   │   ├── test_sp_job_lifecycle.py
    │   │   ├── test_sp_dataset_lifecycle.py
    │   │   ├── test_sp_queries.py
    │   │   └── test_sp_retry.py
    │   └── cli/
    │       ├── test_cli_sqlserver.py
    │       └── test_cli_postgresql.py
    └── e2e/
        ├── __init__.py
        ├── conftest.py                # E2E fixtures (Docker, Airflow)
        ├── test_full_process_execution.py
        ├── test_single_entity_execution.py
        ├── test_retry_scenarios.py
        ├── test_parallelism.py
        ├── test_hooks_execution.py
        └── test_multi_environment.py
```

---

## Database Schema Design

### Schema Name

Configurable per deployment. Default: `nodo_etl`
All tables prefixed with `etl_` for clarity within the schema.

### Audit Columns (on every table)

| Column | Type | Description |
|--------|------|-------------|
| `created_at` | DATETIME/TIMESTAMP | Row creation timestamp |
| `created_by` | VARCHAR(150) | Who created the row |
| `updated_at` | DATETIME/TIMESTAMP | Last update timestamp |
| `updated_by` | VARCHAR(150) | Who last updated |

---

### Core Tables

#### `etl_process`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Process ID |
| `process_name` | VARCHAR(200) | No | Unique name (e.g., `finance_etl`) |
| `description` | VARCHAR(1000) | Yes | Human-readable description |
| `max_parallelism` | INT | Yes | Max parallel jobs (null = system default) |
| `execution_order` | INT | No | Order when multiple processes run (default 1) |
| `max_retries` | INT | Yes | Override system default retries |
| `timeout_seconds` | INT | Yes | Override system default timeout |
| `is_enabled` | BIT/BOOLEAN | No | Whether process is active (default true) |
| `is_deleted` | BIT/BOOLEAN | No | Soft delete flag (default false) |
| `+ audit columns` | | | |

**Unique constraint:** `process_name` (where `is_deleted = false`)

#### `etl_schedule`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Schedule ID |
| `process_id` | INT (FK) | No | References `etl_process.id` |
| `schedule_name` | VARCHAR(200) | No | Name (e.g., `daily_morning`) |
| `cron_expression` | VARCHAR(100) | No | Cron format (e.g., `0 6 * * *`) |
| `is_enabled` | BIT/BOOLEAN | No | Whether schedule is active (default true) |
| `is_deleted` | BIT/BOOLEAN | No | Soft delete flag (default false) |
| `+ audit columns` | | | |

#### `etl_job`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Job ID |
| `process_id` | INT (FK) | No | References `etl_process.id` |
| `job_name` | VARCHAR(200) | No | Name (e.g., `bronze_ingestion`) |
| `description` | VARCHAR(1000) | Yes | Human-readable description |
| `execution_order` | INT | No | Order within process (1=first, same number=parallel) |
| `max_parallelism` | INT | Yes | Max parallel datasets (null = system default) |
| `max_retries` | INT | Yes | Override system default retries |
| `timeout_seconds` | INT | Yes | Override system default timeout |
| `is_enabled` | BIT/BOOLEAN | No | Whether job is active (default true) |
| `is_deleted` | BIT/BOOLEAN | No | Soft delete flag (default false) |
| `+ audit columns` | | | |

**Unique constraint:** `process_id + job_name` (where `is_deleted = false`)

#### `etl_dataset`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Dataset ID |
| `job_id` | INT (FK) | No | References `etl_job.id` |
| `dataset_name` | VARCHAR(200) | No | Name (e.g., `transactions_bronze`) |
| `description` | VARCHAR(1000) | Yes | Human-readable description |
| `source_type` | VARCHAR(50) | No | `database`, `file`, `api`, `stream` |
| `source_connection_id` | INT (FK) | No | References `etl_connection.id` |
| `target_connection_id` | INT (FK) | Yes | References `etl_connection.id` (future use) |
| `layer` | VARCHAR(20) | No | `bronze`, `silver`, `gold` |
| `load_strategy` | VARCHAR(50) | No | `full`, `incremental`, `cdc`, `streaming` |
| `idempotency_strategy` | VARCHAR(50) | No | `overwrite`, `upsert`, `append`, `merge` |
| `execution_order` | INT | No | Order within job (same number=parallel) |
| `max_retries` | INT | Yes | Override system/job default |
| `retry_delay_seconds` | INT | Yes | Delay between retries (default 60) |
| `timeout_seconds` | INT | Yes | Override system/job default |
| `is_enabled` | BIT/BOOLEAN | No | Whether dataset is active (default true) |
| `is_deleted` | BIT/BOOLEAN | No | Soft delete flag (default false) |
| `+ audit columns` | | | |

**Unique constraint:** `job_id + dataset_name` (where `is_deleted = false`)

#### `etl_connection`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Connection ID |
| `connection_name` | VARCHAR(200) | No | Unique name (e.g., `finance_db_prod`) |
| `connection_type` | VARCHAR(50) | No | `sqlserver`, `postgresql`, `mysql`, `oracle`, `api`, `file_storage`, `kafka`, `delta`, `snowflake`, `databricks` |
| `host` | VARCHAR(500) | Yes | Hostname or IP |
| `port` | INT | Yes | Port number |
| `database_name` | VARCHAR(200) | Yes | Database name |
| `schema_name` | VARCHAR(200) | Yes | Schema name |
| `secret_reference` | VARCHAR(500) | Yes | Reference key for credentials |
| `secret_provider_type` | VARCHAR(50) | Yes | `env`, `keyvault`, `aws_sm`, `airflow` (null = system default) |
| `additional_params` | NVARCHAR(MAX)/JSONB | Yes | JSON with extra connection parameters |
| `environment` | VARCHAR(50) | No | `dev`, `staging`, `prod` |
| `is_enabled` | BIT/BOOLEAN | No | Whether connection is active (default true) |
| `is_deleted` | BIT/BOOLEAN | No | Soft delete flag (default false) |
| `+ audit columns` | | | |

**Unique constraint:** `connection_name + environment` (where `is_deleted = false`)

---

### Source Configuration Tables

#### `etl_dataset_db_config`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Config ID |
| `dataset_id` | INT (FK, unique) | No | References `etl_dataset.id` |
| `source_schema` | VARCHAR(200) | Yes | Source schema name |
| `source_table` | VARCHAR(200) | Yes | Source table name |
| `extraction_query` | NVARCHAR(MAX)/TEXT | Yes | Dynamic SQL (supports `{{watermark}}`, `{{start_date}}`, `{{end_date}}` placeholders) |
| `watermark_column` | VARCHAR(200) | Yes | Column for incremental loads (e.g., `updated_at`) |
| `primary_key_columns` | VARCHAR(500) | Yes | Comma-separated PKs (e.g., `id,tenant_id`) |
| `+ audit columns` | | | |

#### `etl_dataset_file_config`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Config ID |
| `dataset_id` | INT (FK, unique) | No | References `etl_dataset.id` |
| `file_path_pattern` | VARCHAR(1000) | No | Path with tokens (e.g., `/data/{yyyy}/{MM}/{dd}/transactions_*.parquet`) |
| `file_format` | VARCHAR(50) | No | `csv`, `parquet`, `delta`, `json`, `avro` |
| `delimiter` | VARCHAR(10) | Yes | CSV delimiter (default `,`) |
| `has_header` | BIT/BOOLEAN | Yes | CSV has header row (default true) |
| `encoding` | VARCHAR(50) | Yes | File encoding (default `utf-8`) |
| `compression` | VARCHAR(50) | Yes | `gzip`, `snappy`, `zstd`, `none` |
| `+ audit columns` | | | |

#### `etl_dataset_api_config`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Config ID |
| `dataset_id` | INT (FK, unique) | No | References `etl_dataset.id` |
| `api_url` | VARCHAR(2000) | No | Endpoint URL (supports `{{placeholder}}` tokens) |
| `api_method` | VARCHAR(10) | No | `GET`, `POST`, `PUT` |
| `api_headers` | NVARCHAR(MAX)/JSONB | Yes | JSON headers |
| `api_body_template` | NVARCHAR(MAX)/JSONB | Yes | JSON body with placeholders |
| `pagination_strategy` | VARCHAR(50) | Yes | `offset`, `cursor`, `next_url`, `none` |
| `pagination_config` | NVARCHAR(MAX)/JSONB | Yes | JSON pagination details (page_size, offset_param, cursor_field, etc.) |
| `rate_limit_per_second` | INT | Yes | Max requests per second |
| `auth_method` | VARCHAR(50) | Yes | `bearer`, `api_key`, `oauth`, `none` |
| `response_path` | VARCHAR(500) | Yes | JSONPath to extract data (e.g., `$.data.results`) |
| `+ audit columns` | | | |

#### `etl_dataset_stream_config`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Config ID |
| `dataset_id` | INT (FK, unique) | No | References `etl_dataset.id` |
| `topic` | VARCHAR(500) | No | Kafka topic name |
| `consumer_group` | VARCHAR(200) | Yes | Consumer group ID |
| `offset_strategy` | VARCHAR(50) | No | `earliest`, `latest`, `specific` |
| `specific_offset` | VARCHAR(200) | Yes | Specific offset value (when strategy=specific) |
| `batch_size` | INT | Yes | Messages per batch |
| `+ audit columns` | | | |

---

### Execution Tracking Tables

#### `etl_process_execution`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Execution ID |
| `process_id` | INT (FK) | No | References `etl_process.id` |
| `environment` | VARCHAR(50) | No | `dev`, `staging`, `prod` |
| `status` | VARCHAR(50) | No | `pending`, `running`, `success`, `failed`, `cancelled` |
| `triggered_by` | VARCHAR(50) | No | `schedule`, `manual`, `retry` |
| `start_time` | DATETIME/TIMESTAMP | Yes | Execution start |
| `end_time` | DATETIME/TIMESTAMP | Yes | Execution end |
| `parameters` | NVARCHAR(MAX)/JSONB | Yes | Runtime parameters JSON |
| `error_message` | NVARCHAR(MAX)/TEXT | Yes | Error details |
| `total_jobs` | INT | Yes | Total jobs in this execution |
| `completed_jobs` | INT | Yes | Successfully completed jobs |
| `failed_jobs` | INT | Yes | Failed jobs |
| `+ audit columns` | | | |

#### `etl_job_execution`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Execution ID |
| `process_execution_id` | INT (FK) | No | References `etl_process_execution.id` |
| `job_id` | INT (FK) | No | References `etl_job.id` |
| `status` | VARCHAR(50) | No | `pending`, `running`, `success`, `failed`, `cancelled` |
| `start_time` | DATETIME/TIMESTAMP | Yes | Execution start |
| `end_time` | DATETIME/TIMESTAMP | Yes | Execution end |
| `error_message` | NVARCHAR(MAX)/TEXT | Yes | Error details |
| `total_datasets` | INT | Yes | Total datasets in this execution |
| `completed_datasets` | INT | Yes | Successfully completed |
| `failed_datasets` | INT | Yes | Failed datasets |
| `+ audit columns` | | | |

#### `etl_dataset_execution`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Execution ID |
| `job_execution_id` | INT (FK) | No | References `etl_job_execution.id` |
| `dataset_id` | INT (FK) | No | References `etl_dataset.id` |
| `status` | VARCHAR(50) | No | `pending`, `running`, `success`, `failed`, `cancelled`, `skipped` |
| `start_time` | DATETIME/TIMESTAMP | Yes | Execution start |
| `end_time` | DATETIME/TIMESTAMP | Yes | Execution end |
| `rows_read` | BIGINT | Yes | Rows extracted from source |
| `rows_written` | BIGINT | Yes | Rows written to target |
| `rows_errored` | BIGINT | Yes | Rows that failed |
| `bytes_processed` | BIGINT | Yes | Data volume processed |
| `error_message` | NVARCHAR(MAX)/TEXT | Yes | Error details |
| `retry_count` | INT | No | Current retry number (default 0) |
| `execution_duration_seconds` | INT | Yes | Computed duration |
| `+ audit columns` | | | |

#### `etl_watermark`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Watermark ID |
| `dataset_id` | INT (FK) | No | References `etl_dataset.id` |
| `environment` | VARCHAR(50) | No | `dev`, `staging`, `prod` |
| `watermark_column` | VARCHAR(200) | No | Column tracked |
| `last_watermark_value` | VARCHAR(500) | No | Last successful value |
| `last_successful_execution_id` | INT (FK) | Yes | References `etl_dataset_execution.id` |
| `+ audit columns` | | | |

**Unique constraint:** `dataset_id + environment`

---

### Supporting Tables

#### `etl_hook`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Hook ID |
| `entity_type` | VARCHAR(50) | No | `process`, `job`, `dataset` |
| `entity_id` | INT | No | ID of the process/job/dataset |
| `hook_type` | VARCHAR(10) | No | `pre`, `post` |
| `execution_order` | INT | No | Order of hook execution (default 1) |
| `action_type` | VARCHAR(50) | No | `sql`, `api`, `command` |
| `action_config` | NVARCHAR(MAX)/JSONB | No | JSON config: `{"query": "TRUNCATE..."}` or `{"url": "...", "method": "POST"}` or `{"command": "python script.py"}` |
| `on_status` | VARCHAR(50) | Yes | For post hooks: `success`, `failed`, `any` (default `any`) |
| `is_enabled` | BIT/BOOLEAN | No | Whether hook is active (default true) |
| `is_deleted` | BIT/BOOLEAN | No | Soft delete flag (default false) |
| `+ audit columns` | | | |

#### `etl_tag`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Tag ID |
| `entity_type` | VARCHAR(50) | No | `process`, `job`, `dataset` |
| `entity_id` | INT | No | ID of the process/job/dataset |
| `tag_key` | VARCHAR(100) | No | Key (e.g., `domain`, `priority`, `team`) |
| `tag_value` | VARCHAR(500) | No | Value (e.g., `finance`, `high`, `data-eng`) |
| `+ audit columns` | | | |

**Unique constraint:** `entity_type + entity_id + tag_key`

#### `etl_dataset_lineage`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Lineage ID |
| `source_dataset_id` | INT (FK) | No | References `etl_dataset.id` |
| `target_dataset_id` | INT (FK) | No | References `etl_dataset.id` |
| `description` | VARCHAR(1000) | Yes | Lineage description |
| `+ audit columns` | | | |

**Unique constraint:** `source_dataset_id + target_dataset_id`

#### `etl_system_config`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INT (PK, auto) | No | Config ID |
| `config_key` | VARCHAR(200) | No | Unique key |
| `config_value` | VARCHAR(2000) | No | Value |
| `description` | VARCHAR(1000) | Yes | What this config does |
| `+ audit columns` | | | |

**Unique constraint:** `config_key`

**Default seed values:**

| config_key | config_value | description |
|------------|-------------|-------------|
| `default_max_retries` | `3` | Default retry count when not set on entity |
| `default_retry_delay_seconds` | `60` | Default delay between retries |
| `default_timeout_seconds` | `3600` | Default timeout per dataset (1 hour) |
| `default_parallelism` | `5` | Default max parallel datasets/jobs |
| `schema_version` | `1.0.0` | Current metadata schema version |
| `framework_version` | `1.0.0` | Framework version |
| `secret_provider` | `env` | Default secret provider type |
| `timezone` | `America/Mexico_City` | Timezone for schedule evaluation |
| `log_retention_days` | `90` | Days to keep execution history |

---

### Stored Procedures

All stored procedures created for both SQL Server and PostgreSQL.

#### Process Execution Lifecycle

| Procedure | Description |
|-----------|-------------|
| `sp_start_process_execution` | Creates process_execution record with status=running, start_time=now. Also creates job_execution records (status=pending) for all enabled jobs, and dataset_execution records (status=pending) for all enabled datasets. Returns the process_execution_id. |
| `sp_complete_process_execution` | Updates process_execution with end_time, status (computed from job results), totals (total_jobs, completed_jobs, failed_jobs). |

#### Job Execution Lifecycle

| Procedure | Description |
|-----------|-------------|
| `sp_start_job_execution` | Updates job_execution to status=running, sets start_time. Can be called standalone to run a single job. |
| `sp_complete_job_execution` | Updates job_execution with end_time, status (computed from dataset results), totals. |

#### Dataset Execution Lifecycle

| Procedure | Description |
|-----------|-------------|
| `sp_start_dataset_execution` | Updates dataset_execution to status=running, sets start_time. Can be called standalone to run a single dataset. |
| `sp_complete_dataset_execution` | Updates with end_time, status, rows_read, rows_written, rows_errored, bytes_processed, error_message, duration. If successful and incremental, updates etl_watermark. |

#### Query Procedures

| Procedure | Description |
|-----------|-------------|
| `sp_get_scheduled_processes` | Returns enabled processes with active schedules matching current time window. Checks cron expressions against the configured timezone. |
| `sp_get_jobs_to_execute` | For a given process_execution_id, returns enabled jobs in execution_order. Respects parallelism limits. |
| `sp_get_datasets_to_execute` | For a given job_execution_id, returns enabled datasets in execution_order. Respects parallelism limits. |
| `sp_get_execution_summary` | The "big query" — joins process_execution → job_execution → dataset_execution with all metadata (process name, job name, dataset name, source type, layer, load strategy, times, status, row counts, errors). Supports filtering by process_id, environment, date range, status. |

#### Retry Procedures

| Procedure | Description |
|-----------|-------------|
| `sp_retry_failed_datasets` | For a given job_execution_id, finds failed datasets where retry_count < max_retries. Resets their status to pending and increments retry_count. Returns list of datasets to retry. |

---

## Phases

---

### Phase E1: Project Foundation & Docker Setup
**Status:** Not Started
**Goal:** Set up the Python project, Docker environment, and local development infrastructure.

#### E1.1 — Python Project Setup
- [ ] Create `projects/nodo-etl-framework/` directory
- [ ] Create `pyproject.toml` with project metadata, dependencies:
  - `click` (CLI framework)
  - `pydantic` (data models & validation)
  - `sqlalchemy` (database abstraction)
  - `pyodbc` (SQL Server driver)
  - `psycopg2` (PostgreSQL driver)
  - `python-dotenv` (environment config)
  - `rich` (CLI output formatting)
  - `pytest` (testing)
- [ ] Create `src/nodo_etl/__init__.py` with version
- [ ] Create `src/nodo_etl/core/enums.py` with all enumerations:
  - `SourceType`: database, file, api, stream
  - `Layer`: bronze, silver, gold
  - `LoadStrategy`: full, incremental, cdc, streaming
  - `IdempotencyStrategy`: overwrite, upsert, append, merge
  - `ExecutionStatus`: pending, running, success, failed, cancelled, skipped
  - `TriggerType`: schedule, manual, retry
  - `HookType`: pre, post
  - `ActionType`: sql, api, command
  - `EntityType`: process, job, dataset
  - `ConnectionType`: sqlserver, postgresql, mysql, oracle, api, file_storage, kafka, delta, snowflake, databricks
  - `SecretProviderType`: env, keyvault, aws_sm, airflow
  - `Environment`: dev, staging, prod
  - `FileFormat`: csv, parquet, delta, json, avro
  - `PaginationStrategy`: offset, cursor, next_url, none
  - `ApiAuthMethod`: bearer, api_key, oauth, none
  - `OffsetStrategy`: earliest, latest, specific
- [ ] Create `src/nodo_etl/core/models.py` with Pydantic models for all entities
- [ ] Create initial `README.md` for the project

#### E1.2 — Docker Setup
- [ ] Create `docker/docker-compose.yml` with services:
  - SQL Server 2022 (port 1433) with custom schema initialization
  - PostgreSQL 15 (port 5432) with custom schema initialization
  - Apache Airflow (webserver port 8080, scheduler)
  - Airflow metadata database (PostgreSQL, separate from ETL metadata)
- [ ] Create `docker/sqlserver/init/` scripts to create configurable schema
- [ ] Create `docker/postgresql/init/` scripts to create configurable schema
- [ ] Create `docker/airflow/Dockerfile` with Python dependencies for nodo_etl
- [ ] Create `.env.example` with all configurable values:
  - `NODO_ETL_SCHEMA` (default: `nodo_etl`)
  - `NODO_ETL_DB_TYPE` (`sqlserver` or `postgresql`)
  - `NODO_ETL_DB_HOST`, `NODO_ETL_DB_PORT`, `NODO_ETL_DB_NAME`
  - `NODO_ETL_DB_USER`, `NODO_ETL_DB_PASSWORD`
  - `NODO_ETL_ENVIRONMENT` (dev/staging/prod)
  - Airflow config vars
- [ ] Test `docker-compose up` brings up all services
- [ ] Document Docker setup in project README

#### E1.3 — Configuration & Environment Support
- [ ] Create `src/nodo_etl/config/settings.py`:
  - Load from `.env` file or environment variables
  - Support dev/staging/prod environments
  - Validate required settings on startup
  - Expose all config via a `Settings` Pydantic model
- [ ] Create tests for settings loading and validation

#### E1.4 — Tests: Enums & Models (`tests/unit/core/`)

**`test_enums.py`** — Verify all enumerations are valid and complete:
- [ ] Test every enum has expected members (e.g., `SourceType` has `database`, `file`, `api`, `stream`)
- [ ] Test enum values are strings (not ints) for JSON serialization
- [ ] Test enum `from_value()` works for valid values
- [ ] Test enum `from_value()` raises error for invalid values (e.g., `SourceType("invalid")`)
- [ ] Test all enums used in database columns match their expected VARCHAR lengths
- [ ] Test enum iteration (e.g., `list(SourceType)` returns all members)

**`test_models.py`** — Validate all Pydantic models:
- [ ] **ProcessModel**: Create with all fields → validates OK
- [ ] **ProcessModel**: Create with only required fields → defaults applied (is_enabled=True, is_deleted=False, execution_order=1)
- [ ] **ProcessModel**: Create with empty process_name → validation error
- [ ] **ProcessModel**: Create with process_name > 200 chars → validation error
- [ ] **ProcessModel**: Create with negative max_parallelism → validation error
- [ ] **ProcessModel**: Create with max_parallelism=0 → validation error
- [ ] **ScheduleModel**: Create with valid cron expression → validates OK
- [ ] **ScheduleModel**: Create with invalid cron expression (e.g., `"not a cron"`) → validation error
- [ ] **ScheduleModel**: Create with 6-field cron (with seconds) → validation error or accepted (define which)
- [ ] **JobModel**: Create with all fields → validates OK
- [ ] **JobModel**: Create with execution_order=0 → validation error (must be >= 1)
- [ ] **JobModel**: Create with missing process_id → validation error
- [ ] **DatasetModel**: Create with source_type=database → validates OK
- [ ] **DatasetModel**: Create with invalid source_type → validation error
- [ ] **DatasetModel**: Create with layer=bronze, load_strategy=full → validates OK
- [ ] **DatasetModel**: Create with invalid layer → validation error
- [ ] **DatasetModel**: Create with retry_delay_seconds < 0 → validation error
- [ ] **ConnectionModel**: Create with all fields → validates OK
- [ ] **ConnectionModel**: Create with connection_type=sqlserver, host=None → validation OK (host is nullable)
- [ ] **ConnectionModel**: Create with environment=invalid → validation error
- [ ] **ConnectionModel**: Verify additional_params accepts valid JSON dict
- [ ] **ConnectionModel**: Verify secret_reference is not exposed in `__repr__` or `__str__`
- [ ] **DbConfigModel**: Create with extraction_query containing `{{watermark}}` placeholder → validates OK
- [ ] **FileConfigModel**: Create with file_format=parquet → validates OK
- [ ] **FileConfigModel**: Create with invalid file_format → validation error
- [ ] **ApiConfigModel**: Create with pagination_strategy=cursor, pagination_config with cursor_field → validates OK
- [ ] **StreamConfigModel**: Create with offset_strategy=specific, specific_offset=None → validation error
- [ ] **StreamConfigModel**: Create with offset_strategy=earliest, specific_offset=None → validates OK
- [ ] **HookModel**: Create with hook_type=pre, action_type=sql, action_config={"query": "..."} → validates OK
- [ ] **HookModel**: Create with on_status=invalid → validation error
- [ ] **TagModel**: Create with valid entity_type → validates OK
- [ ] **TagModel**: Create with entity_type=invalid → validation error
- [ ] **WatermarkModel**: Create with all fields → validates OK
- [ ] **SystemConfigModel**: Create with key/value → validates OK
- [ ] **All models**: Test `to_dict()` / `model_dump()` outputs correct JSON-serializable dict
- [ ] **All models**: Test `from_dict()` / `model_validate()` round-trip (create → dump → recreate → compare)
- [ ] **All models**: Verify audit fields (created_at, created_by, updated_at, updated_by) are optional with defaults

#### E1.5 — Tests: Configuration (`tests/unit/config/`)

**`test_settings.py`**:
- [ ] Test loading settings from environment variables
- [ ] Test loading settings from .env file
- [ ] Test default values when env vars are not set (NODO_ETL_SCHEMA defaults to "nodo_etl")
- [ ] Test validation error when required var NODO_ETL_DB_HOST is missing
- [ ] Test validation error when required var NODO_ETL_DB_NAME is missing
- [ ] Test NODO_ETL_DB_TYPE accepts only "sqlserver" or "postgresql"
- [ ] Test NODO_ETL_DB_TYPE with invalid value raises error
- [ ] Test NODO_ETL_ENVIRONMENT accepts "dev", "staging", "prod"
- [ ] Test NODO_ETL_ENVIRONMENT with invalid value raises error
- [ ] Test NODO_ETL_DB_PORT defaults correctly per DB type (1433 for sqlserver, 5432 for postgresql)
- [ ] Test settings are immutable after loading (frozen model)
- [ ] Test settings repr/str does not expose password

#### E1.6 — Tests: Docker Validation
- [ ] Test `docker-compose up` starts SQL Server container and is reachable on port 1433
- [ ] Test `docker-compose up` starts PostgreSQL container and is reachable on port 5432
- [ ] Test `docker-compose up` starts Airflow webserver on port 8080
- [ ] Test SQL Server container has the configured schema created
- [ ] Test PostgreSQL container has the configured schema created
- [ ] Test Airflow can connect to its own metadata database
- [ ] Test `docker-compose down` cleanly stops all services
- [ ] Test `docker-compose up` is idempotent (run twice without errors)

---

### Phase E2: Metadata Database — Flyway Migrations
**Status:** Not Started
**Goal:** Create all metadata tables via Flyway migrations for both SQL Server and PostgreSQL.

#### E2.1 — Schema Creation
- [ ] `V1__create_schema.sql` — Create configurable schema (SQL Server: `CREATE SCHEMA`, PostgreSQL: `CREATE SCHEMA`)

#### E2.2 — Core Tables
- [ ] `V2__core_tables.sql` — Create tables:
  - `etl_process` with all columns, constraints, indexes
  - `etl_schedule` with FK to process, indexes
  - `etl_job` with FK to process, unique constraint, indexes
  - `etl_connection` with unique constraint per environment, indexes
  - `etl_dataset` with FKs to job and connection, unique constraint, indexes
- [ ] Verify all audit columns present
- [ ] Verify all `is_enabled` defaults to true
- [ ] Verify all `is_deleted` defaults to false
- [ ] Create indexes on foreign keys and commonly queried columns

#### E2.3 — Source Configuration Tables
- [ ] `V3__source_config_tables.sql` — Create tables:
  - `etl_dataset_db_config` with FK to dataset (unique), indexes
  - `etl_dataset_file_config` with FK to dataset (unique), indexes
  - `etl_dataset_api_config` with FK to dataset (unique), indexes
  - `etl_dataset_stream_config` with FK to dataset (unique), indexes

#### E2.4 — Execution Tracking Tables
- [ ] `V4__execution_tables.sql` — Create tables:
  - `etl_process_execution` with FK to process, indexes on status/environment/start_time
  - `etl_job_execution` with FKs to process_execution and job, indexes
  - `etl_dataset_execution` with FKs to job_execution and dataset, indexes
  - `etl_watermark` with FK to dataset, unique constraint per environment, indexes

#### E2.5 — Supporting Tables
- [ ] `V5__supporting_tables.sql` — Create tables:
  - `etl_hook` with indexes on entity_type/entity_id
  - `etl_tag` with unique constraint, indexes
  - `etl_dataset_lineage` with FKs, unique constraint
  - `etl_system_config` with unique key constraint
- [ ] `V5.1__seed_system_config.sql` — Insert default system config values

#### E2.6 — Validation
- [ ] Run migrations against SQL Server Docker container
- [ ] Run migrations against PostgreSQL Docker container
- [ ] Verify all tables, constraints, indexes created correctly
- [ ] Verify seed data inserted

#### E2.7 — Tests: Migrations (`tests/integration/migrations/`)

**`test_migrations_postgresql.py`** and **`test_migrations_sqlserver.py`** — Same scenarios, both engines:

**Schema & Table Existence:**
- [ ] Test schema exists after migration
- [ ] Test all 17 tables exist: etl_process, etl_schedule, etl_job, etl_connection, etl_dataset, etl_dataset_db_config, etl_dataset_file_config, etl_dataset_api_config, etl_dataset_stream_config, etl_process_execution, etl_job_execution, etl_dataset_execution, etl_watermark, etl_hook, etl_tag, etl_dataset_lineage, etl_system_config
- [ ] Test all tables have audit columns (created_at, created_by, updated_at, updated_by)

**Column Verification (per table):**
- [ ] Test etl_process has all columns with correct types and nullability
- [ ] Test etl_schedule has all columns with correct types and nullability
- [ ] Test etl_job has all columns with correct types and nullability
- [ ] Test etl_dataset has all columns with correct types and nullability
- [ ] Test etl_connection has all columns with correct types and nullability
- [ ] Test etl_dataset_db_config has all columns with correct types and nullability
- [ ] Test etl_dataset_file_config has all columns with correct types and nullability
- [ ] Test etl_dataset_api_config has all columns with correct types and nullability
- [ ] Test etl_dataset_stream_config has all columns with correct types and nullability
- [ ] Test etl_process_execution has all columns with correct types and nullability
- [ ] Test etl_job_execution has all columns with correct types and nullability
- [ ] Test etl_dataset_execution has all columns with correct types and nullability
- [ ] Test etl_watermark has all columns with correct types and nullability
- [ ] Test etl_hook has all columns with correct types and nullability
- [ ] Test etl_tag has all columns with correct types and nullability
- [ ] Test etl_dataset_lineage has all columns with correct types and nullability
- [ ] Test etl_system_config has all columns with correct types and nullability

**Default Values:**
- [ ] Test etl_process.is_enabled defaults to true
- [ ] Test etl_process.is_deleted defaults to false
- [ ] Test etl_process.execution_order defaults to 1
- [ ] Test etl_job.is_enabled defaults to true
- [ ] Test etl_dataset.is_enabled defaults to true
- [ ] Test etl_dataset_execution.retry_count defaults to 0
- [ ] Test etl_hook.on_status defaults to 'any'
- [ ] Test all is_enabled/is_deleted defaults across all tables

**Constraints:**
- [ ] Test etl_process unique constraint on process_name (insert duplicate → error)
- [ ] Test etl_process unique constraint allows same name if one is soft-deleted
- [ ] Test etl_job unique constraint on process_id + job_name
- [ ] Test etl_dataset unique constraint on job_id + dataset_name
- [ ] Test etl_connection unique constraint on connection_name + environment
- [ ] Test etl_connection allows same name in different environments
- [ ] Test etl_tag unique constraint on entity_type + entity_id + tag_key
- [ ] Test etl_watermark unique constraint on dataset_id + environment
- [ ] Test etl_dataset_lineage unique constraint on source_dataset_id + target_dataset_id

**Foreign Keys:**
- [ ] Test etl_schedule.process_id FK → cannot insert with non-existent process_id
- [ ] Test etl_job.process_id FK → cannot insert with non-existent process_id
- [ ] Test etl_dataset.job_id FK → cannot insert with non-existent job_id
- [ ] Test etl_dataset.source_connection_id FK → cannot insert with non-existent connection_id
- [ ] Test etl_dataset_db_config.dataset_id FK → cannot insert with non-existent dataset_id
- [ ] Test etl_dataset_db_config.dataset_id unique → cannot insert two configs for same dataset
- [ ] Test etl_process_execution.process_id FK
- [ ] Test etl_job_execution.process_execution_id FK
- [ ] Test etl_job_execution.job_id FK
- [ ] Test etl_dataset_execution.job_execution_id FK
- [ ] Test etl_dataset_execution.dataset_id FK
- [ ] Test etl_watermark.dataset_id FK
- [ ] Test etl_dataset_lineage source/target FKs

**Indexes:**
- [ ] Test indexes exist on all foreign key columns
- [ ] Test indexes exist on etl_process_execution(status, environment, start_time)
- [ ] Test indexes exist on etl_job_execution(status)
- [ ] Test indexes exist on etl_dataset_execution(status)
- [ ] Test indexes exist on etl_hook(entity_type, entity_id)
- [ ] Test indexes exist on etl_tag(entity_type, entity_id)

**Seed Data:**
- [ ] Test etl_system_config has all 9 default rows
- [ ] Test default_max_retries = 3
- [ ] Test default_timeout_seconds = 3600
- [ ] Test default_parallelism = 5
- [ ] Test secret_provider = env
- [ ] Test timezone = America/Mexico_City
- [ ] Test log_retention_days = 90

**Data Insertion Smoke Tests:**
- [ ] Test insert a complete process with all fields → success
- [ ] Test insert a schedule linked to process → success
- [ ] Test insert a connection → success
- [ ] Test insert a job linked to process → success
- [ ] Test insert a dataset linked to job and connection → success
- [ ] Test insert a db_config linked to dataset → success
- [ ] Test insert a file_config linked to dataset → success
- [ ] Test insert an api_config linked to dataset → success
- [ ] Test insert a stream_config linked to dataset → success
- [ ] Test insert a hook → success
- [ ] Test insert a tag → success
- [ ] Test insert a lineage record → success
- [ ] Test insert a process_execution → success
- [ ] Test insert a job_execution → success
- [ ] Test insert a dataset_execution → success
- [ ] Test insert a watermark → success

**JSON/JSONB Fields:**
- [ ] Test etl_connection.additional_params accepts valid JSON
- [ ] Test etl_api_config.api_headers accepts valid JSON
- [ ] Test etl_api_config.pagination_config accepts valid JSON
- [ ] Test etl_hook.action_config accepts valid JSON
- [ ] Test etl_process_execution.parameters accepts valid JSON

---

### Phase E3: Stored Procedures
**Status:** Complete
**Goal:** Create all stored procedures for both SQL Server and PostgreSQL.

#### E3.1 — Process Execution SPs
- [x] `V6__sp_start_process_execution.sql`
  - Input: process_id, environment, triggered_by, parameters (optional)
  - Creates process_execution (status=running)
  - Creates job_execution records for all enabled, non-deleted jobs (status=pending)
  - Creates dataset_execution records for all enabled, non-deleted datasets (status=pending)
  - Returns: process_execution_id
- [x] `V7__sp_complete_process_execution.sql`
  - Input: process_execution_id
  - Computes totals from job_execution records
  - Sets status: success (all jobs success), failed (any job failed), cancelled
  - Sets end_time

#### E3.2 — Job Execution SPs
- [x] `V8__sp_start_job_execution.sql`
  - Input: job_execution_id
  - Updates status=running, start_time=now
- [x] `V9__sp_complete_job_execution.sql`
  - Input: job_execution_id
  - Computes totals from dataset_execution records
  - Sets status based on dataset results
  - Sets end_time

#### E3.3 — Dataset Execution SPs
- [x] `V10__sp_start_dataset_execution.sql`
  - Input: dataset_execution_id
  - Updates status=running, start_time=now
- [x] `V11__sp_complete_dataset_execution.sql`
  - Input: dataset_execution_id, status, rows_read, rows_written, rows_errored, bytes_processed, error_message
  - Updates all fields, computes duration
  - If status=success and load_strategy=incremental: update etl_watermark

#### E3.4 — Query SPs
- [x] `V12__sp_get_scheduled_processes.sql`
  - Returns enabled processes with active cron schedules matching current time
- [x] `V13__sp_get_jobs_to_execute.sql`
  - Input: process_execution_id
  - Returns pending jobs ordered by execution_order
  - Considers max_parallelism from process
- [x] `V14__sp_get_datasets_to_execute.sql`
  - Input: job_execution_id
  - Returns pending datasets ordered by execution_order
  - Considers max_parallelism from job
- [x] `V15__sp_get_execution_summary.sql`
  - Joins all execution tables with metadata tables
  - Filters: process_id, environment, date range, status
  - Returns: process name, job name, dataset name, source type, layer, load strategy, all execution details

#### E3.5 — Retry SPs
- [x] `V16__sp_retry_failed_datasets.sql`
  - Input: job_execution_id
  - Finds failed datasets where retry_count < max_retries (resolves from dataset → job → process → system_config)
  - Resets status=pending, increments retry_count
  - Returns list of datasets to retry

#### E3.6 — Tests: Stored Procedures (`tests/integration/stored_procedures/`)

All tests run against both SQL Server and PostgreSQL (parameterized).

**`test_sp_process_lifecycle.py`** — Process execution lifecycle:
- [ ] Test sp_start_process_execution: creates process_execution with status=running, start_time set
- [ ] Test sp_start_process_execution: creates job_execution records for all enabled jobs (status=pending)
- [ ] Test sp_start_process_execution: creates dataset_execution records for all enabled datasets (status=pending)
- [ ] Test sp_start_process_execution: skips disabled jobs (is_enabled=false)
- [ ] Test sp_start_process_execution: skips soft-deleted jobs (is_deleted=true)
- [ ] Test sp_start_process_execution: skips disabled datasets within enabled jobs
- [ ] Test sp_start_process_execution: returns valid process_execution_id
- [ ] Test sp_start_process_execution: sets triggered_by correctly (schedule, manual, retry)
- [ ] Test sp_start_process_execution: stores parameters JSON correctly
- [ ] Test sp_start_process_execution: with non-existent process_id → error
- [ ] Test sp_start_process_execution: with disabled process → error or handled gracefully
- [ ] Test sp_start_process_execution: process with 0 jobs → creates process_execution, 0 job_executions
- [ ] Test sp_start_process_execution: process with 3 jobs, each with 5 datasets → creates 3 job_executions + 15 dataset_executions
- [ ] Test sp_complete_process_execution: all jobs success → process status=success
- [ ] Test sp_complete_process_execution: one job failed → process status=failed
- [ ] Test sp_complete_process_execution: all jobs cancelled → process status=cancelled
- [ ] Test sp_complete_process_execution: mix of success and failed → process status=failed
- [ ] Test sp_complete_process_execution: sets end_time correctly
- [ ] Test sp_complete_process_execution: computes total_jobs, completed_jobs, failed_jobs correctly
- [ ] Test sp_complete_process_execution: with non-existent process_execution_id → error

**`test_sp_job_lifecycle.py`** — Job execution lifecycle:
- [ ] Test sp_start_job_execution: updates status to running, sets start_time
- [ ] Test sp_start_job_execution: does not affect other job_executions
- [ ] Test sp_start_job_execution: with already-running job → error or idempotent
- [ ] Test sp_start_job_execution: with non-existent job_execution_id → error
- [ ] Test sp_complete_job_execution: all datasets success → job status=success
- [ ] Test sp_complete_job_execution: one dataset failed → job status=failed
- [ ] Test sp_complete_job_execution: some datasets skipped, rest success → job status=success
- [ ] Test sp_complete_job_execution: all datasets skipped → job status=success (or skipped?)
- [ ] Test sp_complete_job_execution: sets end_time correctly
- [ ] Test sp_complete_job_execution: computes total_datasets, completed_datasets, failed_datasets

**`test_sp_dataset_lifecycle.py`** — Dataset execution lifecycle:
- [ ] Test sp_start_dataset_execution: updates status to running, sets start_time
- [ ] Test sp_start_dataset_execution: with non-existent dataset_execution_id → error
- [ ] Test sp_complete_dataset_execution: status=success → updates all fields correctly
- [ ] Test sp_complete_dataset_execution: status=failed → stores error_message
- [ ] Test sp_complete_dataset_execution: computes execution_duration_seconds from start/end
- [ ] Test sp_complete_dataset_execution: rows_read=1000, rows_written=950, rows_errored=50
- [ ] Test sp_complete_dataset_execution: bytes_processed stored correctly
- [ ] Test sp_complete_dataset_execution: success + incremental load → updates etl_watermark
- [ ] Test sp_complete_dataset_execution: success + full load → does NOT update etl_watermark
- [ ] Test sp_complete_dataset_execution: failed + incremental load → does NOT update etl_watermark
- [ ] Test sp_complete_dataset_execution: watermark update creates new record if none exists
- [ ] Test sp_complete_dataset_execution: watermark update overwrites existing record
- [ ] Test sp_complete_dataset_execution: watermark stores correct last_successful_execution_id

**`test_sp_queries.py`** — Query stored procedures:
- [ ] Test sp_get_scheduled_processes: returns process with active cron matching current time
- [ ] Test sp_get_scheduled_processes: skips disabled processes
- [ ] Test sp_get_scheduled_processes: skips disabled schedules
- [ ] Test sp_get_scheduled_processes: skips soft-deleted processes
- [ ] Test sp_get_scheduled_processes: process with multiple schedules, one matches → returned
- [ ] Test sp_get_scheduled_processes: no processes match current time → empty result
- [ ] Test sp_get_scheduled_processes: multiple processes match → returns all
- [ ] Test sp_get_jobs_to_execute: returns jobs ordered by execution_order
- [ ] Test sp_get_jobs_to_execute: respects max_parallelism from process (e.g., max=2, 3 jobs at order=1 → returns 2)
- [ ] Test sp_get_jobs_to_execute: returns only pending jobs (not running or completed)
- [ ] Test sp_get_jobs_to_execute: with no pending jobs → empty result
- [ ] Test sp_get_datasets_to_execute: returns datasets ordered by execution_order
- [ ] Test sp_get_datasets_to_execute: respects max_parallelism from job
- [ ] Test sp_get_datasets_to_execute: returns only pending datasets
- [ ] Test sp_get_datasets_to_execute: with no pending datasets → empty result
- [ ] Test sp_get_datasets_to_execute: datasets with same order returned together (parallel batch)
- [ ] Test sp_get_execution_summary: returns all fields joined correctly
- [ ] Test sp_get_execution_summary: filter by process_id → only that process
- [ ] Test sp_get_execution_summary: filter by environment → only that environment
- [ ] Test sp_get_execution_summary: filter by date range → only executions in range
- [ ] Test sp_get_execution_summary: filter by status → only matching status
- [ ] Test sp_get_execution_summary: no filters → returns all executions
- [ ] Test sp_get_execution_summary: with no executions → empty result
- [ ] Test sp_get_execution_summary: verify row counts, duration, error messages present
- [ ] Test sp_get_execution_summary: verify process/job/dataset names (not just IDs) are returned

**`test_sp_retry.py`** — Retry stored procedures:
- [ ] Test sp_retry_failed_datasets: finds failed datasets with retry_count < max_retries
- [ ] Test sp_retry_failed_datasets: resets status to pending
- [ ] Test sp_retry_failed_datasets: increments retry_count by 1
- [ ] Test sp_retry_failed_datasets: skips datasets at max_retries (retry_count=3, max_retries=3)
- [ ] Test sp_retry_failed_datasets: resolves max_retries from dataset level first
- [ ] Test sp_retry_failed_datasets: falls back to job max_retries if dataset is null
- [ ] Test sp_retry_failed_datasets: falls back to process max_retries if job is null
- [ ] Test sp_retry_failed_datasets: falls back to system_config default_max_retries if all null
- [ ] Test sp_retry_failed_datasets: returns list of datasets reset for retry
- [ ] Test sp_retry_failed_datasets: with no failed datasets → empty result
- [ ] Test sp_retry_failed_datasets: with all datasets at max retries → empty result
- [ ] Test sp_retry_failed_datasets: does not affect successful or pending datasets
- [ ] Test sp_retry_failed_datasets: mixed scenario (2 failed retryable, 1 failed at max, 1 success) → returns 2

---

### Phase E4: Python Core — Database Layer & Secret Provider
**Status:** Complete
**Goal:** Build the Python core: database connection, repositories, and secret provider abstraction.

#### E4.1 — Secret Provider
- [x] Create `src/nodo_etl/secrets/base.py`:
  - `SecretProvider` abstract class with `get_secret(reference: str) -> str`
- [x] Create `src/nodo_etl/secrets/env_provider.py`:
  - Reads secrets from environment variables
  - `secret_reference` = env var name
- [x] Create `src/nodo_etl/secrets/factory.py`:
  - `get_provider(provider_type: str) -> SecretProvider`
  - Defaults to system_config `secret_provider` if not specified
- [x] Unit tests for secret providers

#### E4.2 — Database Connection Layer
- [x] Create `src/nodo_etl/db/dialect.py`:
  - Abstract SQL dialect to handle SQL Server vs PostgreSQL differences
  - Schema-qualified table names
  - Data type mappings
  - JSON field handling (NVARCHAR(MAX) vs JSONB)
- [x] Create `src/nodo_etl/db/connection.py`:
  - `MetadataDBConnection` class using SQLAlchemy
  - Reads connection config from settings
  - Uses secret provider for credentials
  - Connection pooling
  - Schema-aware queries
- [x] Unit tests for connection and dialect

#### E4.3 — Repository Layer
- [x] Create `src/nodo_etl/db/repositories.py`:
  - `ProcessRepository` — CRUD for etl_process + etl_schedule
  - `JobRepository` — CRUD for etl_job
  - `DatasetRepository` — CRUD for etl_dataset + source config tables
  - `ConnectionRepository` — CRUD for etl_connection
  - `HookRepository` — CRUD for etl_hook
  - `TagRepository` — CRUD for etl_tag
  - `LineageRepository` — CRUD for etl_dataset_lineage
  - `SystemConfigRepository` — CRUD for etl_system_config
  - `ExecutionRepository` — Calls stored procedures for execution lifecycle
  - All repositories respect soft deletes (filter `is_deleted = false`)
  - All repositories set audit columns automatically
- [ ] Integration tests for each repository against both databases

#### E4.4 — Tests: Secret Provider (`tests/unit/secrets/`)

**`test_env_provider.py`**:
- [x] Test get_secret with existing env var → returns value
- [x] Test get_secret with non-existent env var → raises SecretNotFoundError
- [x] Test get_secret with empty env var value → returns empty string (or error, define behavior)
- [x] Test get_secret with special characters in value → returns correctly
- [x] Test get_secret reference name is case-sensitive
- [x] Test provider type identifier returns "env"

**`test_factory.py`**:
- [x] Test get_provider("env") → returns EnvSecretProvider instance
- [x] Test get_provider("keyvault") → raises NotImplementedError (not yet implemented)
- [x] Test get_provider("aws_sm") → raises NotImplementedError
- [x] Test get_provider("airflow") → raises NotImplementedError
- [x] Test get_provider("invalid") → raises ValueError
- [x] Test get_provider(None) → uses system_config default provider
- [x] Test factory caches providers (returns same instance for same type)

#### E4.5 — Tests: Database Connection (`tests/unit/db/`)

**`test_dialect.py`**:
- [x] Test PostgreSQL dialect generates correct schema-qualified table name (e.g., `nodo_etl.etl_process`)
- [x] Test SQL Server dialect generates correct schema-qualified table name (e.g., `nodo_etl.etl_process`)
- [x] Test PostgreSQL dialect maps JSON type to JSONB
- [x] Test SQL Server dialect maps JSON type to NVARCHAR(MAX)
- [x] Test PostgreSQL dialect maps BOOLEAN correctly
- [x] Test SQL Server dialect maps BOOLEAN to BIT
- [x] Test dialect factory returns correct dialect for "postgresql"
- [x] Test dialect factory returns correct dialect for "sqlserver"
- [x] Test dialect factory raises error for unsupported type

**`test_connection.py`** (mocked):
- [x] Test connection string built correctly for PostgreSQL
- [x] Test connection string built correctly for SQL Server
- [x] Test connection uses secret provider to resolve password
- [x] Test connection uses settings for host/port/database
- [x] Test connection pool size is configurable
- [x] Test connection raises error when database is unreachable (mocked)

#### E4.6 — Tests: Repositories (`tests/integration/db/`)

**`test_repositories_common.py`** — Shared test scenarios (parameterized for both DB engines):

**ProcessRepository:**
- [ ] Test create process with all fields → returns created process with ID
- [ ] Test create process with only required fields → defaults applied
- [ ] Test create process with duplicate name → raises error
- [ ] Test create process with duplicate name but other is soft-deleted → success
- [ ] Test get process by ID → returns correct process
- [ ] Test get process by ID that doesn't exist → raises NotFoundError
- [ ] Test get process by ID that is soft-deleted → raises NotFoundError
- [ ] Test list processes → returns all non-deleted processes
- [ ] Test list processes with is_enabled filter → returns only enabled/disabled
- [ ] Test list processes → does not return soft-deleted processes
- [ ] Test update process fields → updated_at and updated_by set
- [ ] Test update process name to existing name → raises error
- [ ] Test delete process (soft) → sets is_deleted=true
- [ ] Test delete process cascades soft-delete to jobs → jobs soft-deleted
- [ ] Test delete process cascades soft-delete to datasets → datasets soft-deleted
- [ ] Test create schedule for process → success
- [ ] Test create multiple schedules for same process → success
- [ ] Test list schedules for process → returns all non-deleted schedules
- [ ] Test delete schedule → soft delete

**JobRepository:**
- [ ] Test create job with all fields → success
- [ ] Test create job with duplicate name under same process → error
- [ ] Test create job with same name under different process → success
- [ ] Test get job by ID → correct job returned
- [ ] Test list jobs by process_id → returns jobs ordered by execution_order
- [ ] Test list jobs filters out soft-deleted
- [ ] Test update job execution_order → success
- [ ] Test delete job (soft) → cascades to datasets

**DatasetRepository:**
- [ ] Test create dataset with db source → creates dataset + etl_dataset_db_config
- [ ] Test create dataset with file source → creates dataset + etl_dataset_file_config
- [ ] Test create dataset with api source → creates dataset + etl_dataset_api_config
- [ ] Test create dataset with stream source → creates dataset + etl_dataset_stream_config
- [ ] Test create dataset with source_type=database but no db_config → error
- [ ] Test create dataset with duplicate name under same job → error
- [ ] Test get dataset by ID → returns dataset with source config
- [ ] Test list datasets by job_id → returns ordered by execution_order
- [ ] Test update dataset source config → updates config table
- [ ] Test delete dataset (soft) → soft deletes dataset and source config
- [ ] Test update dataset load_strategy from full to incremental → success
- [ ] Test dataset with all source types: verify correct config table populated

**ConnectionRepository:**
- [ ] Test create connection → success
- [ ] Test create connection with same name, same environment → error
- [ ] Test create connection with same name, different environment → success
- [ ] Test get connection by ID → returns connection
- [ ] Test get connection masks secret_reference in output
- [ ] Test list connections filtered by type → returns only matching type
- [ ] Test list connections filtered by environment → returns only matching env
- [ ] Test update connection → success
- [ ] Test delete connection (soft) → success
- [ ] Test delete connection that is referenced by a dataset → error or warning
- [ ] Test test connection (mocked) → returns connectivity result

**HookRepository:**
- [ ] Test create hook for process → success
- [ ] Test create hook for job → success
- [ ] Test create hook for dataset → success
- [ ] Test list hooks by entity_type and entity_id → returns ordered by execution_order
- [ ] Test list hooks filters out disabled hooks
- [ ] Test update hook → success
- [ ] Test delete hook (soft) → success

**TagRepository:**
- [ ] Test add tag to process → success
- [ ] Test add tag to job → success
- [ ] Test add tag to dataset → success
- [ ] Test add duplicate tag (same entity, same key) → error (or upsert?)
- [ ] Test list tags by entity → returns all tags
- [ ] Test remove tag → deletes row

**LineageRepository:**
- [ ] Test add lineage link → success
- [ ] Test add duplicate lineage → error
- [ ] Test get upstream lineage (what feeds into dataset X) → correct results
- [ ] Test get downstream lineage (what dataset X feeds) → correct results
- [ ] Test multi-hop lineage (bronze → silver → gold) → traverses correctly

**SystemConfigRepository:**
- [ ] Test get config by key → returns value
- [ ] Test get config by non-existent key → raises error or returns default
- [ ] Test set config (update existing) → updates value
- [ ] Test set config (new key) → inserts
- [ ] Test list all configs → returns all entries

**ExecutionRepository:**
- [ ] Test start process execution → calls SP, returns execution_id
- [ ] Test complete process execution → calls SP
- [ ] Test start job execution → calls SP
- [ ] Test complete job execution → calls SP
- [ ] Test start dataset execution → calls SP
- [ ] Test complete dataset execution → calls SP
- [ ] Test get execution summary → calls SP, returns formatted results
- [ ] Test retry failed datasets → calls SP, returns retryable datasets

---

### Phase E5: Python CLI
**Status:** Complete
**Goal:** Build the CLI tool for managing ETL metadata.

#### E5.1 — CLI Framework Setup
- [x] Create `src/nodo_etl/cli/main.py`:
  - Click group with global options: `--env`, `--db-type`, `--config`
  - Version command
  - Initialize settings on startup
- [x] Register as console script in pyproject.toml: `nodo-etl`

#### E5.2 — Connection Commands
- [x] `nodo-etl connection create` — Interactive or flag-based connection creation
- [x] `nodo-etl connection list` — List all connections (filterable by type, environment)
- [x] `nodo-etl connection get <id>` — Show connection details (mask secrets)
- [x] `nodo-etl connection update <id>` — Update connection fields
- [x] `nodo-etl connection delete <id>` — Soft delete
- [x] `nodo-etl connection test <id>` — Test connectivity

#### E5.3 — Process Commands
- [x] `nodo-etl process create` — Create process with schedules
- [x] `nodo-etl process list` — List all processes (filterable by enabled, tags)
- [x] `nodo-etl process get <id>` — Show process with jobs, datasets, schedules
- [x] `nodo-etl process update <id>` — Update process fields
- [x] `nodo-etl process delete <id>` — Soft delete (cascades to jobs/datasets)
- [x] `nodo-etl process enable/disable <id>` — Toggle enabled flag

#### E5.4 — Job Commands
- [x] `nodo-etl job create` — Create job under a process
- [x] `nodo-etl job list --process-id <id>` — List jobs for a process
- [x] `nodo-etl job get <id>` — Show job with datasets
- [x] `nodo-etl job update <id>` — Update job fields
- [x] `nodo-etl job delete <id>` — Soft delete
- [x] `nodo-etl job enable/disable <id>` — Toggle enabled flag

#### E5.5 — Dataset Commands
- [x] `nodo-etl dataset create` — Create dataset with source config
- [x] `nodo-etl dataset list --job-id <id>` — List datasets for a job
- [x] `nodo-etl dataset get <id>` — Show dataset with source config
- [x] `nodo-etl dataset update <id>` — Update dataset fields
- [x] `nodo-etl dataset delete <id>` — Soft delete
- [x] `nodo-etl dataset enable/disable <id>` — Toggle enabled flag

#### E5.6 — Execution Commands
- [x] `nodo-etl run process <id>` — Trigger a full process execution (calls SP)
- [x] `nodo-etl run job <id>` — Trigger a single job execution
- [x] `nodo-etl run dataset <id>` — Trigger a single dataset execution
- [x] `nodo-etl status <process-execution-id>` — Show execution summary (calls SP)
- [x] `nodo-etl history --process-id <id>` — Show execution history
- [x] `nodo-etl retry <job-execution-id>` — Retry failed datasets (calls SP)

#### E5.7 — Utility Commands
- [x] `nodo-etl config list` — Show system config values
- [x] `nodo-etl config set <key> <value>` — Update system config
- [x] `nodo-etl tag add <entity-type> <id> <key> <value>` — Add tag
- [x] `nodo-etl tag list <entity-type> <id>` — List tags
- [x] `nodo-etl hook add` — Add pre/post hook
- [x] `nodo-etl hook list <entity-type> <id>` — List hooks
- [x] `nodo-etl lineage add <source-id> <target-id>` — Add lineage
- [x] `nodo-etl lineage show <dataset-id>` — Show upstream/downstream lineage

#### E5.8 — Tests: CLI Unit Tests (`tests/unit/cli/`)

All CLI tests use Click's `CliRunner` with mocked repositories.

**`test_connection_commands.py`**:
- [x] Test `connection create` with all flags → calls repository.create, shows success message
- [x] Test `connection create` with missing required flag → shows error
- [x] Test `connection create` with invalid connection_type → shows error
- [x] Test `connection list` → shows table of connections
- [x] Test `connection list --type sqlserver` → shows only sqlserver connections
- [x] Test `connection list --environment prod` → shows only prod connections
- [x] Test `connection list` with no connections → shows "no connections found"
- [x] Test `connection get 1` → shows connection details
- [x] Test `connection get 999` → shows "not found" error
- [x] Test `connection get` masks secret_reference in output
- [x] Test `connection update 1 --host new-host` → calls repository.update
- [x] Test `connection delete 1` → calls repository.delete (soft), shows confirmation
- [x] Test `connection test 1` → calls repository.test_connection, shows result

**`test_process_commands.py`**:
- [x] Test `process create --name finance_etl --description "..."` → success
- [x] Test `process create` with schedule flags → creates process + schedule
- [x] Test `process create` with duplicate name → shows error
- [x] Test `process list` → shows table with name, enabled status, schedule count
- [x] Test `process list --enabled` → filters enabled only
- [x] Test `process list --tag domain=finance` → filters by tag
- [x] Test `process get 1` → shows process details with jobs, datasets, schedules tree
- [x] Test `process get 999` → shows "not found"
- [x] Test `process update 1 --max-parallelism 10` → success
- [x] Test `process delete 1` → soft deletes with confirmation prompt
- [x] Test `process enable 1` → sets is_enabled=true
- [x] Test `process disable 1` → sets is_enabled=false

**`test_job_commands.py`**:
- [x] Test `job create --process-id 1 --name bronze_ingestion --order 1` → success
- [x] Test `job create` with invalid process-id → shows error
- [x] Test `job create` with duplicate name under same process → shows error
- [x] Test `job list --process-id 1` → shows jobs ordered by execution_order
- [x] Test `job list` without process-id → shows error (required)
- [x] Test `job get 1` → shows job with datasets list
- [x] Test `job update 1 --order 2` → success
- [x] Test `job delete 1` → soft deletes job and its datasets
- [x] Test `job enable/disable 1` → toggles is_enabled

**`test_dataset_commands.py`**:
- [x] Test `dataset create` with source_type=database and db config flags → creates dataset + db_config
- [x] Test `dataset create` with source_type=file and file config flags → creates dataset + file_config
- [x] Test `dataset create` with source_type=api and api config flags → creates dataset + api_config
- [x] Test `dataset create` with source_type=stream and stream config flags → creates dataset + stream_config
- [x] Test `dataset create` with mismatched source_type and config → shows error
- [x] Test `dataset create` with invalid load_strategy → shows error
- [x] Test `dataset create` with invalid layer → shows error
- [x] Test `dataset list --job-id 1` → shows datasets ordered
- [x] Test `dataset get 1` → shows dataset with source config details
- [x] Test `dataset update 1 --load-strategy incremental` → success
- [x] Test `dataset delete 1` → soft delete
- [x] Test `dataset enable/disable 1` → toggles

**`test_execution_commands.py`**:
- [x] Test `run process 1` → calls start_process_execution, shows execution_id
- [x] Test `run process 999` → shows "process not found"
- [x] Test `run job 1` → calls start_job_execution
- [x] Test `run dataset 1` → calls start_dataset_execution
- [x] Test `status 1` → calls get_execution_summary, shows formatted table
- [x] Test `status 999` → shows "execution not found"
- [x] Test `history --process-id 1` → shows execution history list
- [x] Test `history --process-id 1 --limit 5` → shows last 5 executions
- [x] Test `retry 1` → calls retry_failed_datasets, shows retryable datasets
- [x] Test `retry 1` with no failed datasets → shows "nothing to retry"

**`test_utility_commands.py`**:
- [x] Test `config list` → shows all system config entries
- [x] Test `config set timezone UTC` → updates system config
- [x] Test `config set invalid_key value` → success (allows custom keys)
- [x] Test `tag add process 1 domain finance` → adds tag
- [x] Test `tag add` with invalid entity_type → shows error
- [x] Test `tag list process 1` → shows tags
- [x] Test `hook add` with all flags → creates hook
- [x] Test `hook list process 1` → shows hooks
- [x] Test `lineage add 1 2` → creates lineage link
- [x] Test `lineage add 1 1` → shows error (self-reference)
- [x] Test `lineage show 1` → shows upstream and downstream

#### E5.9 — Tests: CLI Integration (`tests/integration/cli/`)

**`test_cli_postgresql.py`** and **`test_cli_sqlserver.py`**:
- [ ] Test full workflow: create connection → create process → create job → create dataset → verify in DB
- [ ] Test full workflow: run process → check status → verify execution records in DB
- [ ] Test full workflow: create metadata → disable dataset → run process → verify dataset skipped
- [ ] Test full workflow: run process → force failure → retry → verify retry_count incremented
- [ ] Test CLI output formatting matches expected format (table alignment, colors)
- [ ] Test CLI with --env flag switches environment context
- [ ] Test CLI with --db-type flag works for both database engines

---

### Phase E6: Airflow Orchestration
**Status:** Not Started
**Goal:** Build Airflow DAGs that read the metadata database and orchestrate pipeline execution.

#### E6.1 — Airflow DAG: Process Scheduler
- [ ] Create `docker/airflow/dags/nodo_etl_orchestrator.py`:
  - DAG that runs on a short interval (e.g., every 5 minutes)
  - Calls `sp_get_scheduled_processes` to find due processes
  - For each due process, triggers a process execution DAG run
- [ ] Configure Airflow connection to metadata database

#### E6.2 — Airflow DAG: Process Executor
- [ ] Create `docker/airflow/dags/nodo_etl_process_executor.py`:
  - Receives process_id and environment as parameters
  - Calls `sp_start_process_execution`
  - Gets jobs via `sp_get_jobs_to_execute`
  - Executes jobs in order (respecting execution_order and parallelism):
    - Jobs with same execution_order run in parallel
    - Next order waits for previous to complete
  - For each job:
    - Calls `sp_start_job_execution`
    - Gets datasets via `sp_get_datasets_to_execute`
    - Executes datasets in order (respecting execution_order and parallelism)
    - For each dataset:
      - Calls `sp_start_dataset_execution`
      - Executes pre-hooks
      - **Dummy execution for now** (logs "Processing dataset X")
      - Executes post-hooks
      - Calls `sp_complete_dataset_execution`
    - Handles retries via `sp_retry_failed_datasets`
    - Calls `sp_complete_job_execution`
  - Calls `sp_complete_process_execution`

#### E6.3 — Airflow DAG: Single Entity Executor
- [ ] Create `docker/airflow/dags/nodo_etl_single_executor.py`:
  - Supports running a single process, job, or dataset
  - Triggered manually via Airflow UI or CLI
  - Same execution lifecycle as process executor but scoped

#### E6.4 — Hook Executor
- [ ] Create hook execution logic:
  - SQL hooks: execute query against the hook's configured connection
  - API hooks: make HTTP request with configured URL/method/body
  - Command hooks: execute shell command
- [ ] Respect hook execution_order and on_status filter

#### E6.5 — Tests: Airflow Orchestration (`tests/e2e/`)

All E2E tests run against Docker (Airflow + metadata DB).

**`test_full_process_execution.py`** — Complete process lifecycle:
- [ ] **Scenario: Happy path** — Process with 3 jobs (bronze→silver→gold), each with 2 datasets. All succeed. Verify:
  - Process execution status = success
  - All job executions status = success
  - All dataset executions status = success
  - Jobs executed in order (bronze before silver before gold)
  - Datasets within same order executed in parallel
  - Start/end times are set and logical (end > start)
  - Total/completed/failed counts are correct at all levels
- [ ] **Scenario: Job ordering** — Process with jobs at order 1, 2, 3. Verify order 2 jobs don't start until all order 1 jobs complete
- [ ] **Scenario: Mixed execution orders** — 2 jobs at order=1 (parallel), 1 job at order=2. Verify both order=1 jobs start simultaneously, order=2 waits
- [ ] **Scenario: Single job process** — Process with 1 job, 1 dataset → full lifecycle completes correctly
- [ ] **Scenario: Large process** — Process with 5 jobs, 10 datasets each → all 50 dataset executions tracked correctly
- [ ] **Scenario: Process with parameters** — Pass `{"report_date": "2026-03-01"}` → parameters stored in process_execution

**`test_single_entity_execution.py`** — Running individual entities:
- [ ] **Scenario: Run single process** — Trigger via Airflow → full process lifecycle
- [ ] **Scenario: Run single job** — Only the specified job executes, not the whole process
- [ ] **Scenario: Run single dataset** — Only the specified dataset executes
- [ ] **Scenario: Run disabled process** → error or skip (define behavior)
- [ ] **Scenario: Run disabled dataset** → error or skip
- [ ] **Scenario: Run dataset that belongs to disabled job** → should it run or not? (define behavior)

**`test_retry_scenarios.py`** — Retry and failure handling:
- [ ] **Scenario: One dataset fails, rest succeed** → job status=failed, process status=failed, other datasets unaffected
- [ ] **Scenario: Failed dataset with retries=3** → retry runs 3 times, each incrementing retry_count
- [ ] **Scenario: Failed dataset succeeds on retry** → retry_count=1, status=success after retry
- [ ] **Scenario: Failed dataset exhausts retries** → retry_count=3, status=failed, error_message preserved
- [ ] **Scenario: Multiple datasets fail** → all retried independently
- [ ] **Scenario: Retry uses retry_delay_seconds** → verify delay between retries (or mock time)
- [ ] **Scenario: Dataset max_retries overrides job default** → dataset retries=5, job retries=3, dataset retries 5 times
- [ ] **Scenario: Job max_retries used when dataset is null** → falls back to job setting
- [ ] **Scenario: System config default used when all null** → falls back to default_max_retries=3
- [ ] **Scenario: Dataset timeout** → dataset running longer than timeout_seconds → marked as failed
- [ ] **Scenario: Retry after timeout failure** → retries the timed-out dataset

**`test_parallelism.py`** — Parallelism control:
- [ ] **Scenario: Job max_parallelism=2, 5 datasets at order=1** → only 2 run at a time, next 2 after first 2 complete, last 1 after
- [ ] **Scenario: Job max_parallelism=null** → falls back to process max_parallelism
- [ ] **Scenario: Process max_parallelism=null** → falls back to system default_parallelism=5
- [ ] **Scenario: Process max_parallelism=1** → jobs run strictly sequential even if same order
- [ ] **Scenario: Datasets at different orders** → order=1 all complete before order=2 starts
- [ ] **Scenario: Mix of orders and parallelism** → 3 datasets at order=1 (parallel, max=2), 2 datasets at order=2 (parallel)
- [ ] **Scenario: Parallelism with failures** → failed dataset at order=1 doesn't block order=2 datasets from starting (or does it? define behavior)

**`test_hooks_execution.py`** — Pre/post hooks:
- [ ] **Scenario: Pre-hook on job** → hook executes before first dataset in job starts
- [ ] **Scenario: Post-hook on job** → hook executes after all datasets complete
- [ ] **Scenario: Pre-hook SQL** → executes SQL against configured connection (e.g., TRUNCATE staging)
- [ ] **Scenario: Post-hook API** → makes HTTP call with configured URL/method/body
- [ ] **Scenario: Post-hook command** → executes shell command
- [ ] **Scenario: Post-hook on_status=success** → only runs when job succeeds
- [ ] **Scenario: Post-hook on_status=failed** → only runs when job fails
- [ ] **Scenario: Post-hook on_status=any** → runs regardless of outcome
- [ ] **Scenario: Multiple hooks with execution_order** → hooks run in order (1 before 2 before 3)
- [ ] **Scenario: Disabled hook** → skipped during execution
- [ ] **Scenario: Hook on process level** → pre-hook runs before first job, post-hook after last job
- [ ] **Scenario: Hook on dataset level** → pre-hook before dataset execution, post-hook after
- [ ] **Scenario: Hook failure** → should it fail the parent entity? (define behavior — log error and continue vs propagate failure)

**`test_multi_environment.py`** — Environment support:
- [ ] **Scenario: Same process metadata, different connections per env** → dev uses dev DB, prod uses prod DB
- [ ] **Scenario: Run process in dev** → uses dev connections, tracks as environment=dev
- [ ] **Scenario: Run same process in prod** → uses prod connections, tracks as environment=prod
- [ ] **Scenario: Watermarks are per-environment** → dev watermark doesn't affect prod watermark
- [ ] **Scenario: Execution history filtered by environment** → summary shows only selected env
- [ ] **Scenario: Connection with environment=dev not usable in prod run** → error or fallback (define)

---

### Phase E7: Integration Testing & Sample Data
**Status:** Not Started
**Goal:** End-to-end testing with realistic sample data and documentation.

#### E7.1 — Sample Metadata
- [ ] Create seed scripts with a complete example:
  - **Process:** `finance_etl` (enabled, cron: daily at 6am)
  - **Connections:** source DB (PostgreSQL), target storage
  - **Job 1:** `bronze_ingestion` (order=1, parallelism=3)
    - Dataset: `transactions_bronze` (db source, incremental, watermark=updated_at)
    - Dataset: `accounts_bronze` (db source, full load)
    - Dataset: `exchange_rates_bronze` (api source, full load)
  - **Job 2:** `silver_transformation` (order=2, parallelism=2)
    - Dataset: `transactions_silver` (order=1)
    - Dataset: `accounts_silver` (order=1)
  - **Job 3:** `gold_aggregation` (order=3, parallelism=1)
    - Dataset: `daily_revenue_gold` (order=1)
  - **Hooks:** Pre-hook on bronze job (truncate staging), post-hook on process (log completion)
  - **Tags:** domain=finance, priority=high
  - **Lineage:** transactions_bronze → transactions_silver → daily_revenue_gold

#### E7.2 — End-to-End Integration Tests

All scenarios run against both SQL Server and PostgreSQL.

**Scenario 1: Full Finance ETL Pipeline**
- [ ] Create all metadata via CLI (connections, process, schedules, jobs, datasets, hooks, tags, lineage)
- [ ] Trigger process via Airflow scheduler (verify cron triggers correctly)
- [ ] Verify bronze job (order=1) executes first with 3 datasets in parallel
- [ ] Verify silver job (order=2) starts only after bronze completes
- [ ] Verify gold job (order=3) starts only after silver completes
- [ ] Verify all dataset_execution records have status=success
- [ ] Verify all job_execution records have correct totals
- [ ] Verify process_execution has status=success with correct totals
- [ ] Verify execution_summary SP returns all details with process/job/dataset names
- [ ] Verify tags are queryable and filter correctly
- [ ] Verify lineage shows bronze → silver → gold chain

**Scenario 2: Incremental Load with Watermarks**
- [ ] Create dataset with load_strategy=incremental, watermark_column=updated_at
- [ ] Run first execution → watermark set to max(updated_at) from result
- [ ] Run second execution → verify watermark used as filter ({{watermark}} placeholder resolved)
- [ ] Run third execution → watermark updated again
- [ ] Verify watermark history is per-environment (dev watermark != prod watermark)
- [ ] Verify failed execution does NOT update watermark
- [ ] Verify watermark table has correct last_successful_execution_id

**Scenario 3: Failure and Recovery**
- [ ] Run process where 1 of 3 bronze datasets fails
- [ ] Verify failed dataset has error_message stored
- [ ] Verify other datasets completed successfully
- [ ] Verify job status=failed, process status=failed
- [ ] Run retry → verify only failed dataset re-runs
- [ ] Verify retry_count incremented
- [ ] After successful retry → verify job can be marked success
- [ ] Run process again → all datasets run fresh (new execution, not retry)

**Scenario 4: Disabled Entities**
- [ ] Disable one dataset → run process → verify that dataset has no execution record
- [ ] Disable one job → run process → verify that job and its datasets are skipped
- [ ] Disable process → try to trigger → verify process does not run
- [ ] Re-enable all → run process → everything executes normally

**Scenario 5: Multi-Process Orchestration**
- [ ] Create 3 processes with execution_order 1, 2, 3
- [ ] Trigger all via scheduler → verify they run in order
- [ ] Create 2 processes with same execution_order → verify they run in parallel

**Scenario 6: Hooks End-to-End**
- [ ] Add pre-hook (SQL) to bronze job → verify SQL executes before datasets
- [ ] Add post-hook (API) to process → verify API called after process completes
- [ ] Add post-hook on_status=failed to job → verify it runs only when job fails
- [ ] Add multiple hooks with different execution_orders → verify order respected

**Scenario 7: All Source Types**
- [ ] Create dataset with source_type=database → verify db_config stored and retrieved
- [ ] Create dataset with source_type=file → verify file_config stored and retrieved
- [ ] Create dataset with source_type=api → verify api_config stored and retrieved
- [ ] Create dataset with source_type=stream → verify stream_config stored and retrieved
- [ ] Run process with mixed source types → all execute correctly (dummy for now)

**Scenario 8: Edge Cases**
- [ ] Process with 0 jobs → process starts and completes immediately with success
- [ ] Job with 0 datasets → job starts and completes immediately with success
- [ ] Process with 100 datasets across 10 jobs → large-scale execution tracking works
- [ ] Dataset with very long extraction_query (10KB) → stored and retrieved correctly
- [ ] Dataset with special characters in name → handled correctly
- [ ] Concurrent process executions → two executions of same process don't interfere
- [ ] Connection with all fields null except required → works correctly
- [ ] System config changes take effect on next execution (no restart needed)

**Scenario 9: Execution Summary Queries**
- [ ] Run 5 process executions with different statuses
- [ ] Query execution summary with no filters → returns all 5
- [ ] Query filtered by process_id → returns only that process
- [ ] Query filtered by environment=dev → returns only dev executions
- [ ] Query filtered by status=failed → returns only failed executions
- [ ] Query filtered by date range → returns only executions in range
- [ ] Query with multiple filters combined → correct intersection
- [ ] Verify summary includes: process name, job name, dataset name, source type, layer, load strategy, start/end times, status, row counts, error messages, duration

**Scenario 10: Cross-Database Consistency**
- [ ] Run identical scenario on SQL Server and PostgreSQL
- [ ] Verify same metadata produces same execution behavior
- [ ] Verify stored procedure outputs match between engines
- [ ] Verify JSON fields handled correctly on both engines
- [ ] Verify datetime precision consistent between engines

#### E7.3 — Documentation
- [ ] Project README with:
  - Quick start guide
  - Architecture overview
  - Configuration reference
  - CLI command reference
- [ ] Create `docs/guidelines/PYTHON.md` with coding conventions
- [ ] Create `docs/reference/ETL_CLI.md` with CLI documentation
- [ ] Create `docs/reference/ETL_DATABASE.md` with schema reference
- [ ] Update master BACKLOG.md with progress

---

## Future Phases (Deferred)

### Phase E8: Target Configuration & Column Mapping
- [ ] Design and create `etl_dataset_target_config` table
- [ ] Design and create `etl_dataset_column_mapping` table
- [ ] Implement target-specific loading logic (upsert, merge, overwrite)
- [ ] Column mapping resolution in transformation code

### Phase E9: DDL Generation
- [ ] Connect to transactional databases, extract metadata
- [ ] Auto-generate bronze DDLs (mirror source schema)
- [ ] Auto-generate silver DDLs (cleaned, typed, standardized)
- [ ] Auto-generate gold DDLs (aggregated, business-ready)
- [ ] Support Databricks (Delta tables) and Snowflake
- [ ] Handle schema drift detection and alerts

### Phase E10: Dynamic Transformation Code
- [ ] Generate dynamic SQL/Spark code for bronze→silver→gold
- [ ] Handle schema changes automatically
- [ ] Support custom transformation logic
- [ ] Integration with Databricks notebooks

### Phase E11: ADF & Databricks Orchestration
- [ ] ADF pipeline templates that read metadata
- [ ] ADF → Databricks notebook execution
- [ ] Replace dummy execution with real ADF/Databricks calls
- [ ] Support both Airflow and ADF as orchestration engines

### Phase E12: dbt Integration
- [ ] Generate dbt models from metadata
- [ ] dbt source/model YAML generation
- [ ] dbt test generation from metadata
- [ ] Integrate dbt runs into pipeline execution

### Phase E13: Notifications & Alerting
- [ ] Email notifications on process/job/dataset failure
- [ ] Slack webhook notifications
- [ ] Custom webhook support
- [ ] Notification configuration per process/job/dataset

### Phase E14: Data Quality Checks
- [ ] Row count validation between layers
- [ ] Null checks on key columns
- [ ] Custom data quality rules per dataset
- [ ] Quality check results stored in execution tracking
- [ ] Quality gate: block silver if bronze quality fails

### Phase E15: Schema Drift Detection
- [ ] Track expected schema per dataset (etl_dataset_columns table)
- [ ] Compare source schema vs expected on each run
- [ ] Alert on new/removed/changed columns
- [ ] Auto-propose migration scripts

### Phase E16: Additional Secret Providers
- [ ] Azure Key Vault provider
- [ ] AWS Secrets Manager provider
- [ ] Airflow connections provider
- [ ] HashiCorp Vault provider

### Phase E17: Audit History
- [ ] `etl_change_log` table for tracking metadata changes
- [ ] Trigger-based or application-level change capture
- [ ] Query: "who changed this dataset's query last week?"

### Phase E18: CDC & Streaming
- [ ] Kafka source implementation
- [ ] CDC implementation (Debezium integration)
- [ ] Custom incremental change detection
- [ ] Streaming pipeline execution mode
