-- V2: Core metadata tables (SQL Server)

-- -------------------------------------------------------
-- etl_process
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_process (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    process_name    VARCHAR(200)    NOT NULL,
    description     VARCHAR(1000)   NULL,
    max_parallelism INT             NULL,
    execution_order INT             NOT NULL DEFAULT 1,
    max_retries     INT             NULL,
    timeout_seconds INT             NULL,
    is_enabled      BIT             NOT NULL DEFAULT 1,
    is_deleted      BIT             NOT NULL DEFAULT 0,
    created_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by      VARCHAR(150)    NULL,
    updated_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by      VARCHAR(150)    NULL
);
GO

CREATE UNIQUE INDEX uq_etl_process_name
    ON nodo_etl.etl_process (process_name)
    WHERE is_deleted = 0;
GO

CREATE INDEX ix_etl_process_enabled
    ON nodo_etl.etl_process (is_enabled)
    WHERE is_deleted = 0;
GO

-- -------------------------------------------------------
-- etl_schedule
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_schedule (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    process_id      INT             NOT NULL REFERENCES nodo_etl.etl_process(id),
    schedule_name   VARCHAR(200)    NOT NULL,
    cron_expression VARCHAR(100)    NOT NULL,
    is_enabled      BIT             NOT NULL DEFAULT 1,
    is_deleted      BIT             NOT NULL DEFAULT 0,
    created_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by      VARCHAR(150)    NULL,
    updated_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by      VARCHAR(150)    NULL
);
GO

CREATE INDEX ix_etl_schedule_process_id
    ON nodo_etl.etl_schedule (process_id);
GO

-- -------------------------------------------------------
-- etl_connection
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_connection (
    id                   INT IDENTITY(1,1) PRIMARY KEY,
    connection_name      VARCHAR(200)    NOT NULL,
    connection_type      VARCHAR(50)     NOT NULL,
    host                 VARCHAR(500)    NULL,
    port                 INT             NULL,
    database_name        VARCHAR(200)    NULL,
    schema_name          VARCHAR(200)    NULL,
    secret_reference     VARCHAR(500)    NULL,
    secret_provider_type VARCHAR(50)     NULL,
    additional_params    NVARCHAR(MAX)   NULL,
    environment          VARCHAR(50)     NOT NULL,
    is_enabled           BIT             NOT NULL DEFAULT 1,
    is_deleted           BIT             NOT NULL DEFAULT 0,
    created_at           DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by           VARCHAR(150)    NULL,
    updated_at           DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by           VARCHAR(150)    NULL
);
GO

CREATE UNIQUE INDEX uq_etl_connection_name_env
    ON nodo_etl.etl_connection (connection_name, environment)
    WHERE is_deleted = 0;
GO

CREATE INDEX ix_etl_connection_type
    ON nodo_etl.etl_connection (connection_type);
GO

CREATE INDEX ix_etl_connection_environment
    ON nodo_etl.etl_connection (environment);
GO

-- -------------------------------------------------------
-- etl_job
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_job (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    process_id      INT             NOT NULL REFERENCES nodo_etl.etl_process(id),
    job_name        VARCHAR(200)    NOT NULL,
    description     VARCHAR(1000)   NULL,
    execution_order INT             NOT NULL DEFAULT 1,
    max_parallelism INT             NULL,
    max_retries     INT             NULL,
    timeout_seconds INT             NULL,
    is_enabled      BIT             NOT NULL DEFAULT 1,
    is_deleted      BIT             NOT NULL DEFAULT 0,
    created_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by      VARCHAR(150)    NULL,
    updated_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by      VARCHAR(150)    NULL
);
GO

CREATE UNIQUE INDEX uq_etl_job_process_name
    ON nodo_etl.etl_job (process_id, job_name)
    WHERE is_deleted = 0;
GO

CREATE INDEX ix_etl_job_process_id
    ON nodo_etl.etl_job (process_id);
GO

CREATE INDEX ix_etl_job_execution_order
    ON nodo_etl.etl_job (process_id, execution_order);
GO

-- -------------------------------------------------------
-- etl_dataset
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset (
    id                    INT IDENTITY(1,1) PRIMARY KEY,
    job_id                INT             NOT NULL REFERENCES nodo_etl.etl_job(id),
    dataset_name          VARCHAR(200)    NOT NULL,
    description           VARCHAR(1000)   NULL,
    source_type           VARCHAR(50)     NOT NULL,
    source_connection_id  INT             NOT NULL REFERENCES nodo_etl.etl_connection(id),
    target_connection_id  INT             NULL REFERENCES nodo_etl.etl_connection(id),
    layer                 VARCHAR(20)     NOT NULL,
    load_strategy         VARCHAR(50)     NOT NULL,
    idempotency_strategy  VARCHAR(50)     NOT NULL,
    execution_order       INT             NOT NULL DEFAULT 1,
    max_retries           INT             NULL,
    retry_delay_seconds   INT             NULL,
    timeout_seconds       INT             NULL,
    is_enabled            BIT             NOT NULL DEFAULT 1,
    is_deleted            BIT             NOT NULL DEFAULT 0,
    created_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by            VARCHAR(150)    NULL,
    updated_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by            VARCHAR(150)    NULL
);
GO

CREATE UNIQUE INDEX uq_etl_dataset_job_name
    ON nodo_etl.etl_dataset (job_id, dataset_name)
    WHERE is_deleted = 0;
GO

CREATE INDEX ix_etl_dataset_job_id
    ON nodo_etl.etl_dataset (job_id);
GO

CREATE INDEX ix_etl_dataset_source_connection
    ON nodo_etl.etl_dataset (source_connection_id);
GO

CREATE INDEX ix_etl_dataset_layer
    ON nodo_etl.etl_dataset (layer);
GO

CREATE INDEX ix_etl_dataset_execution_order
    ON nodo_etl.etl_dataset (job_id, execution_order);
GO
