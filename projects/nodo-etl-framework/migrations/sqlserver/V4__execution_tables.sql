-- V4: Execution tracking tables (SQL Server)

-- -------------------------------------------------------
-- etl_process_execution
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_process_execution (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    process_id      INT             NOT NULL REFERENCES nodo_etl.etl_process(id),
    environment     VARCHAR(50)     NOT NULL,
    status          VARCHAR(50)     NOT NULL DEFAULT 'pending',
    triggered_by    VARCHAR(50)     NOT NULL,
    start_time      DATETIME2       NULL,
    end_time        DATETIME2       NULL,
    parameters      NVARCHAR(MAX)   NULL,
    error_message   NVARCHAR(MAX)   NULL,
    total_jobs      INT             NULL,
    completed_jobs  INT             NULL,
    failed_jobs     INT             NULL,
    created_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by      VARCHAR(150)    NULL,
    updated_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by      VARCHAR(150)    NULL
);
GO

CREATE INDEX ix_etl_process_execution_process_id
    ON nodo_etl.etl_process_execution (process_id);
GO

CREATE INDEX ix_etl_process_execution_status
    ON nodo_etl.etl_process_execution (status);
GO

CREATE INDEX ix_etl_process_execution_environment
    ON nodo_etl.etl_process_execution (environment);
GO

CREATE INDEX ix_etl_process_execution_start_time
    ON nodo_etl.etl_process_execution (start_time);
GO

CREATE INDEX ix_etl_process_execution_composite
    ON nodo_etl.etl_process_execution (process_id, environment, status, start_time);
GO

-- -------------------------------------------------------
-- etl_job_execution
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_job_execution (
    id                     INT IDENTITY(1,1) PRIMARY KEY,
    process_execution_id   INT             NOT NULL REFERENCES nodo_etl.etl_process_execution(id),
    job_id                 INT             NOT NULL REFERENCES nodo_etl.etl_job(id),
    status                 VARCHAR(50)     NOT NULL DEFAULT 'pending',
    start_time             DATETIME2       NULL,
    end_time               DATETIME2       NULL,
    error_message          NVARCHAR(MAX)   NULL,
    total_datasets         INT             NULL,
    completed_datasets     INT             NULL,
    failed_datasets        INT             NULL,
    created_at             DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by             VARCHAR(150)    NULL,
    updated_at             DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by             VARCHAR(150)    NULL
);
GO

CREATE INDEX ix_etl_job_execution_process_exec
    ON nodo_etl.etl_job_execution (process_execution_id);
GO

CREATE INDEX ix_etl_job_execution_job_id
    ON nodo_etl.etl_job_execution (job_id);
GO

CREATE INDEX ix_etl_job_execution_status
    ON nodo_etl.etl_job_execution (status);
GO

-- -------------------------------------------------------
-- etl_dataset_execution
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_execution (
    id                          INT IDENTITY(1,1) PRIMARY KEY,
    job_execution_id            INT             NOT NULL REFERENCES nodo_etl.etl_job_execution(id),
    dataset_id                  INT             NOT NULL REFERENCES nodo_etl.etl_dataset(id),
    status                      VARCHAR(50)     NOT NULL DEFAULT 'pending',
    start_time                  DATETIME2       NULL,
    end_time                    DATETIME2       NULL,
    rows_read                   BIGINT          NULL,
    rows_written                BIGINT          NULL,
    rows_errored                BIGINT          NULL,
    bytes_processed             BIGINT          NULL,
    error_message               NVARCHAR(MAX)   NULL,
    retry_count                 INT             NOT NULL DEFAULT 0,
    execution_duration_seconds  INT             NULL,
    created_at                  DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by                  VARCHAR(150)    NULL,
    updated_at                  DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by                  VARCHAR(150)    NULL
);
GO

CREATE INDEX ix_etl_dataset_execution_job_exec
    ON nodo_etl.etl_dataset_execution (job_execution_id);
GO

CREATE INDEX ix_etl_dataset_execution_dataset_id
    ON nodo_etl.etl_dataset_execution (dataset_id);
GO

CREATE INDEX ix_etl_dataset_execution_status
    ON nodo_etl.etl_dataset_execution (status);
GO

-- -------------------------------------------------------
-- etl_watermark
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_watermark (
    id                            INT IDENTITY(1,1) PRIMARY KEY,
    dataset_id                    INT             NOT NULL REFERENCES nodo_etl.etl_dataset(id),
    environment                   VARCHAR(50)     NOT NULL,
    watermark_column              VARCHAR(200)    NOT NULL,
    last_watermark_value          VARCHAR(500)    NOT NULL,
    last_successful_execution_id  INT             NULL REFERENCES nodo_etl.etl_dataset_execution(id),
    created_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by                    VARCHAR(150)    NULL,
    updated_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by                    VARCHAR(150)    NULL
);
GO

CREATE UNIQUE INDEX uq_etl_watermark_dataset_env
    ON nodo_etl.etl_watermark (dataset_id, environment);
GO
