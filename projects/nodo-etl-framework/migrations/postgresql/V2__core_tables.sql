-- V2: Core metadata tables
-- Tables: etl_process, etl_schedule, etl_connection, etl_job, etl_dataset

-- -------------------------------------------------------
-- etl_process
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_process (
    id              SERIAL PRIMARY KEY,
    process_name    VARCHAR(200)    NOT NULL,
    description     VARCHAR(1000),
    max_parallelism INT,
    execution_order INT             NOT NULL DEFAULT 1,
    max_retries     INT,
    timeout_seconds INT,
    is_enabled      BOOLEAN         NOT NULL DEFAULT TRUE,
    is_deleted      BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by      VARCHAR(150),
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(150)
);

CREATE UNIQUE INDEX uq_etl_process_name
    ON nodo_etl.etl_process (process_name)
    WHERE is_deleted = FALSE;

CREATE INDEX ix_etl_process_enabled
    ON nodo_etl.etl_process (is_enabled)
    WHERE is_deleted = FALSE;

-- -------------------------------------------------------
-- etl_schedule
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_schedule (
    id              SERIAL PRIMARY KEY,
    process_id      INT             NOT NULL REFERENCES nodo_etl.etl_process(id),
    schedule_name   VARCHAR(200)    NOT NULL,
    cron_expression VARCHAR(100)    NOT NULL,
    is_enabled      BOOLEAN         NOT NULL DEFAULT TRUE,
    is_deleted      BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by      VARCHAR(150),
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(150)
);

CREATE INDEX ix_etl_schedule_process_id
    ON nodo_etl.etl_schedule (process_id);

-- -------------------------------------------------------
-- etl_connection
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_connection (
    id                   SERIAL PRIMARY KEY,
    connection_name      VARCHAR(200)    NOT NULL,
    connection_type      VARCHAR(50)     NOT NULL,
    host                 VARCHAR(500),
    port                 INT,
    database_name        VARCHAR(200),
    schema_name          VARCHAR(200),
    secret_reference     VARCHAR(500),
    secret_provider_type VARCHAR(50),
    additional_params    JSONB,
    environment          VARCHAR(50)     NOT NULL,
    is_enabled           BOOLEAN         NOT NULL DEFAULT TRUE,
    is_deleted           BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by           VARCHAR(150),
    updated_at           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by           VARCHAR(150)
);

CREATE UNIQUE INDEX uq_etl_connection_name_env
    ON nodo_etl.etl_connection (connection_name, environment)
    WHERE is_deleted = FALSE;

CREATE INDEX ix_etl_connection_type
    ON nodo_etl.etl_connection (connection_type);

CREATE INDEX ix_etl_connection_environment
    ON nodo_etl.etl_connection (environment);

-- -------------------------------------------------------
-- etl_job
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_job (
    id              SERIAL PRIMARY KEY,
    process_id      INT             NOT NULL REFERENCES nodo_etl.etl_process(id),
    job_name        VARCHAR(200)    NOT NULL,
    description     VARCHAR(1000),
    execution_order INT             NOT NULL DEFAULT 1,
    max_parallelism INT,
    max_retries     INT,
    timeout_seconds INT,
    is_enabled      BOOLEAN         NOT NULL DEFAULT TRUE,
    is_deleted      BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by      VARCHAR(150),
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(150)
);

CREATE UNIQUE INDEX uq_etl_job_process_name
    ON nodo_etl.etl_job (process_id, job_name)
    WHERE is_deleted = FALSE;

CREATE INDEX ix_etl_job_process_id
    ON nodo_etl.etl_job (process_id);

CREATE INDEX ix_etl_job_execution_order
    ON nodo_etl.etl_job (process_id, execution_order);

-- -------------------------------------------------------
-- etl_dataset
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset (
    id                    SERIAL PRIMARY KEY,
    job_id                INT             NOT NULL REFERENCES nodo_etl.etl_job(id),
    dataset_name          VARCHAR(200)    NOT NULL,
    description           VARCHAR(1000),
    source_type           VARCHAR(50)     NOT NULL,
    source_connection_id  INT             NOT NULL REFERENCES nodo_etl.etl_connection(id),
    target_connection_id  INT             REFERENCES nodo_etl.etl_connection(id),
    layer                 VARCHAR(20)     NOT NULL,
    load_strategy         VARCHAR(50)     NOT NULL,
    idempotency_strategy  VARCHAR(50)     NOT NULL,
    execution_order       INT             NOT NULL DEFAULT 1,
    max_retries           INT,
    retry_delay_seconds   INT,
    timeout_seconds       INT,
    is_enabled            BOOLEAN         NOT NULL DEFAULT TRUE,
    is_deleted            BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at            TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by            VARCHAR(150),
    updated_at            TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by            VARCHAR(150)
);

CREATE UNIQUE INDEX uq_etl_dataset_job_name
    ON nodo_etl.etl_dataset (job_id, dataset_name)
    WHERE is_deleted = FALSE;

CREATE INDEX ix_etl_dataset_job_id
    ON nodo_etl.etl_dataset (job_id);

CREATE INDEX ix_etl_dataset_source_connection
    ON nodo_etl.etl_dataset (source_connection_id);

CREATE INDEX ix_etl_dataset_layer
    ON nodo_etl.etl_dataset (layer);

CREATE INDEX ix_etl_dataset_execution_order
    ON nodo_etl.etl_dataset (job_id, execution_order);
