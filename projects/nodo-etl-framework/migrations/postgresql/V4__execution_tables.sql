-- V4: Execution tracking tables
-- Tables: etl_process_execution, etl_job_execution, etl_dataset_execution, etl_watermark

-- -------------------------------------------------------
-- etl_process_execution
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_process_execution (
    id              SERIAL PRIMARY KEY,
    process_id      INT             NOT NULL REFERENCES nodo_etl.etl_process(id),
    environment     VARCHAR(50)     NOT NULL,
    status          VARCHAR(50)     NOT NULL DEFAULT 'pending',
    triggered_by    VARCHAR(50)     NOT NULL,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    parameters      JSONB,
    error_message   TEXT,
    total_jobs      INT,
    completed_jobs  INT,
    failed_jobs     INT,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by      VARCHAR(150),
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(150)
);

CREATE INDEX ix_etl_process_execution_process_id
    ON nodo_etl.etl_process_execution (process_id);

CREATE INDEX ix_etl_process_execution_status
    ON nodo_etl.etl_process_execution (status);

CREATE INDEX ix_etl_process_execution_environment
    ON nodo_etl.etl_process_execution (environment);

CREATE INDEX ix_etl_process_execution_start_time
    ON nodo_etl.etl_process_execution (start_time);

CREATE INDEX ix_etl_process_execution_composite
    ON nodo_etl.etl_process_execution (process_id, environment, status, start_time);

-- -------------------------------------------------------
-- etl_job_execution
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_job_execution (
    id                     SERIAL PRIMARY KEY,
    process_execution_id   INT             NOT NULL REFERENCES nodo_etl.etl_process_execution(id),
    job_id                 INT             NOT NULL REFERENCES nodo_etl.etl_job(id),
    status                 VARCHAR(50)     NOT NULL DEFAULT 'pending',
    start_time             TIMESTAMP,
    end_time               TIMESTAMP,
    error_message          TEXT,
    total_datasets         INT,
    completed_datasets     INT,
    failed_datasets        INT,
    created_at             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by             VARCHAR(150),
    updated_at             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by             VARCHAR(150)
);

CREATE INDEX ix_etl_job_execution_process_exec
    ON nodo_etl.etl_job_execution (process_execution_id);

CREATE INDEX ix_etl_job_execution_job_id
    ON nodo_etl.etl_job_execution (job_id);

CREATE INDEX ix_etl_job_execution_status
    ON nodo_etl.etl_job_execution (status);

-- -------------------------------------------------------
-- etl_dataset_execution
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_execution (
    id                          SERIAL PRIMARY KEY,
    job_execution_id            INT             NOT NULL REFERENCES nodo_etl.etl_job_execution(id),
    dataset_id                  INT             NOT NULL REFERENCES nodo_etl.etl_dataset(id),
    status                      VARCHAR(50)     NOT NULL DEFAULT 'pending',
    start_time                  TIMESTAMP,
    end_time                    TIMESTAMP,
    rows_read                   BIGINT,
    rows_written                BIGINT,
    rows_errored                BIGINT,
    bytes_processed             BIGINT,
    error_message               TEXT,
    retry_count                 INT             NOT NULL DEFAULT 0,
    execution_duration_seconds  INT,
    created_at                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by                  VARCHAR(150),
    updated_at                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by                  VARCHAR(150)
);

CREATE INDEX ix_etl_dataset_execution_job_exec
    ON nodo_etl.etl_dataset_execution (job_execution_id);

CREATE INDEX ix_etl_dataset_execution_dataset_id
    ON nodo_etl.etl_dataset_execution (dataset_id);

CREATE INDEX ix_etl_dataset_execution_status
    ON nodo_etl.etl_dataset_execution (status);

-- -------------------------------------------------------
-- etl_watermark
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_watermark (
    id                            SERIAL PRIMARY KEY,
    dataset_id                    INT             NOT NULL REFERENCES nodo_etl.etl_dataset(id),
    environment                   VARCHAR(50)     NOT NULL,
    watermark_column              VARCHAR(200)    NOT NULL,
    last_watermark_value          VARCHAR(500)    NOT NULL,
    last_successful_execution_id  INT             REFERENCES nodo_etl.etl_dataset_execution(id),
    created_at                    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by                    VARCHAR(150),
    updated_at                    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by                    VARCHAR(150)
);

CREATE UNIQUE INDEX uq_etl_watermark_dataset_env
    ON nodo_etl.etl_watermark (dataset_id, environment);
