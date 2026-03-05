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
    ├── conftest.py                     # Shared fixtures
    ├── unit/
    │   ├── test_models.py
    │   ├── test_secrets.py
    │   └── test_config.py
    └── integration/
        ├── test_repositories.py
        ├── test_cli.py
        └── test_stored_procedures.py
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

---

### Phase E3: Stored Procedures
**Status:** Not Started
**Goal:** Create all stored procedures for both SQL Server and PostgreSQL.

#### E3.1 — Process Execution SPs
- [ ] `V6__sp_start_process_execution.sql`
  - Input: process_id, environment, triggered_by, parameters (optional)
  - Creates process_execution (status=running)
  - Creates job_execution records for all enabled, non-deleted jobs (status=pending)
  - Creates dataset_execution records for all enabled, non-deleted datasets (status=pending)
  - Returns: process_execution_id
- [ ] `V7__sp_complete_process_execution.sql`
  - Input: process_execution_id
  - Computes totals from job_execution records
  - Sets status: success (all jobs success), failed (any job failed), cancelled
  - Sets end_time

#### E3.2 — Job Execution SPs
- [ ] `V8__sp_start_job_execution.sql`
  - Input: job_execution_id
  - Updates status=running, start_time=now
- [ ] `V9__sp_complete_job_execution.sql`
  - Input: job_execution_id
  - Computes totals from dataset_execution records
  - Sets status based on dataset results
  - Sets end_time

#### E3.3 — Dataset Execution SPs
- [ ] `V10__sp_start_dataset_execution.sql`
  - Input: dataset_execution_id
  - Updates status=running, start_time=now
- [ ] `V11__sp_complete_dataset_execution.sql`
  - Input: dataset_execution_id, status, rows_read, rows_written, rows_errored, bytes_processed, error_message
  - Updates all fields, computes duration
  - If status=success and load_strategy=incremental: update etl_watermark

#### E3.4 — Query SPs
- [ ] `V12__sp_get_scheduled_processes.sql`
  - Returns enabled processes with active cron schedules matching current time
- [ ] `V13__sp_get_jobs_to_execute.sql`
  - Input: process_execution_id
  - Returns pending jobs ordered by execution_order
  - Considers max_parallelism from process
- [ ] `V14__sp_get_datasets_to_execute.sql`
  - Input: job_execution_id
  - Returns pending datasets ordered by execution_order
  - Considers max_parallelism from job
- [ ] `V15__sp_get_execution_summary.sql`
  - Joins all execution tables with metadata tables
  - Filters: process_id, environment, date range, status
  - Returns: process name, job name, dataset name, source type, layer, load strategy, all execution details

#### E3.5 — Retry SPs
- [ ] `V16__sp_retry_failed_datasets.sql`
  - Input: job_execution_id
  - Finds failed datasets where retry_count < max_retries (resolves from dataset → job → process → system_config)
  - Resets status=pending, increments retry_count
  - Returns list of datasets to retry

#### E3.6 — Validation
- [ ] Test all SPs against SQL Server
- [ ] Test all SPs against PostgreSQL
- [ ] Integration tests with sample data (insert process → jobs → datasets → execute lifecycle)

---

### Phase E4: Python Core — Database Layer & Secret Provider
**Status:** Not Started
**Goal:** Build the Python core: database connection, repositories, and secret provider abstraction.

#### E4.1 — Secret Provider
- [ ] Create `src/nodo_etl/secrets/base.py`:
  - `SecretProvider` abstract class with `get_secret(reference: str) -> str`
- [ ] Create `src/nodo_etl/secrets/env_provider.py`:
  - Reads secrets from environment variables
  - `secret_reference` = env var name
- [ ] Create `src/nodo_etl/secrets/factory.py`:
  - `get_provider(provider_type: str) -> SecretProvider`
  - Defaults to system_config `secret_provider` if not specified
- [ ] Unit tests for secret providers

#### E4.2 — Database Connection Layer
- [ ] Create `src/nodo_etl/db/dialect.py`:
  - Abstract SQL dialect to handle SQL Server vs PostgreSQL differences
  - Schema-qualified table names
  - Data type mappings
  - JSON field handling (NVARCHAR(MAX) vs JSONB)
- [ ] Create `src/nodo_etl/db/connection.py`:
  - `MetadataDBConnection` class using SQLAlchemy
  - Reads connection config from settings
  - Uses secret provider for credentials
  - Connection pooling
  - Schema-aware queries
- [ ] Unit tests for connection and dialect

#### E4.3 — Repository Layer
- [ ] Create `src/nodo_etl/db/repositories.py`:
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

---

### Phase E5: Python CLI
**Status:** Not Started
**Goal:** Build the CLI tool for managing ETL metadata.

#### E5.1 — CLI Framework Setup
- [ ] Create `src/nodo_etl/cli/main.py`:
  - Click group with global options: `--env`, `--db-type`, `--config`
  - Version command
  - Initialize settings on startup
- [ ] Register as console script in pyproject.toml: `nodo-etl`

#### E5.2 — Connection Commands
- [ ] `nodo-etl connection create` — Interactive or flag-based connection creation
- [ ] `nodo-etl connection list` — List all connections (filterable by type, environment)
- [ ] `nodo-etl connection get <id>` — Show connection details (mask secrets)
- [ ] `nodo-etl connection update <id>` — Update connection fields
- [ ] `nodo-etl connection delete <id>` — Soft delete
- [ ] `nodo-etl connection test <id>` — Test connectivity

#### E5.3 — Process Commands
- [ ] `nodo-etl process create` — Create process with schedules
- [ ] `nodo-etl process list` — List all processes (filterable by enabled, tags)
- [ ] `nodo-etl process get <id>` — Show process with jobs, datasets, schedules
- [ ] `nodo-etl process update <id>` — Update process fields
- [ ] `nodo-etl process delete <id>` — Soft delete (cascades to jobs/datasets)
- [ ] `nodo-etl process enable/disable <id>` — Toggle enabled flag

#### E5.4 — Job Commands
- [ ] `nodo-etl job create` — Create job under a process
- [ ] `nodo-etl job list --process-id <id>` — List jobs for a process
- [ ] `nodo-etl job get <id>` — Show job with datasets
- [ ] `nodo-etl job update <id>` — Update job fields
- [ ] `nodo-etl job delete <id>` — Soft delete
- [ ] `nodo-etl job enable/disable <id>` — Toggle enabled flag

#### E5.5 — Dataset Commands
- [ ] `nodo-etl dataset create` — Create dataset with source config
- [ ] `nodo-etl dataset list --job-id <id>` — List datasets for a job
- [ ] `nodo-etl dataset get <id>` — Show dataset with source config
- [ ] `nodo-etl dataset update <id>` — Update dataset fields
- [ ] `nodo-etl dataset delete <id>` — Soft delete
- [ ] `nodo-etl dataset enable/disable <id>` — Toggle enabled flag

#### E5.6 — Execution Commands
- [ ] `nodo-etl run process <id>` — Trigger a full process execution (calls SP)
- [ ] `nodo-etl run job <id>` — Trigger a single job execution
- [ ] `nodo-etl run dataset <id>` — Trigger a single dataset execution
- [ ] `nodo-etl status <process-execution-id>` — Show execution summary (calls SP)
- [ ] `nodo-etl history --process-id <id>` — Show execution history
- [ ] `nodo-etl retry <job-execution-id>` — Retry failed datasets (calls SP)

#### E5.7 — Utility Commands
- [ ] `nodo-etl config list` — Show system config values
- [ ] `nodo-etl config set <key> <value>` — Update system config
- [ ] `nodo-etl tag add <entity-type> <id> <key> <value>` — Add tag
- [ ] `nodo-etl tag list <entity-type> <id>` — List tags
- [ ] `nodo-etl hook add` — Add pre/post hook
- [ ] `nodo-etl hook list <entity-type> <id>` — List hooks
- [ ] `nodo-etl lineage add <source-id> <target-id>` — Add lineage
- [ ] `nodo-etl lineage show <dataset-id>` — Show upstream/downstream lineage

#### E5.8 — CLI Tests
- [ ] Unit tests for all commands (mocked DB)
- [ ] Integration tests against Docker databases

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

#### E6.5 — Validation & Testing
- [ ] Test full orchestration flow with sample metadata:
  - Create a sample "finance" process with 3 jobs (bronze, silver, gold)
  - Each job has 2-3 dummy datasets
  - Verify execution order, parallelism, status tracking
- [ ] Test single dataset execution
- [ ] Test retry logic
- [ ] Test pre/post hooks
- [ ] Verify execution summary SP returns correct data
- [ ] Test with both SQL Server and PostgreSQL metadata databases

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

#### E7.2 — End-to-End Tests
- [ ] Test: Create all metadata via CLI → Run process via Airflow → Verify execution tracking
- [ ] Test: Disable a dataset → Run process → Verify dataset is skipped
- [ ] Test: Force a dataset failure → Verify retry logic works
- [ ] Test: Run single dataset independently
- [ ] Test: Verify watermark updates after successful incremental load
- [ ] Test: Verify hooks execute at correct times
- [ ] Test: Verify execution summary shows all details
- [ ] Test: Run same scenario on SQL Server and PostgreSQL

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
