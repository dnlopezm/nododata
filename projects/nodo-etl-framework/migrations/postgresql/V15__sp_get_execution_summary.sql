-- V15: sp_get_execution_summary
-- The "big query" — joins all execution and metadata tables

CREATE OR REPLACE FUNCTION nodo_etl.sp_get_execution_summary(
    p_process_id    INT DEFAULT NULL,
    p_environment   VARCHAR(50) DEFAULT NULL,
    p_status        VARCHAR(50) DEFAULT NULL,
    p_start_date    TIMESTAMP DEFAULT NULL,
    p_end_date      TIMESTAMP DEFAULT NULL
)
RETURNS TABLE (
    process_execution_id    INT,
    process_name            VARCHAR(200),
    process_status          VARCHAR(50),
    process_triggered_by    VARCHAR(50),
    process_start_time      TIMESTAMP,
    process_end_time        TIMESTAMP,
    process_environment     VARCHAR(50),
    process_total_jobs      INT,
    process_completed_jobs  INT,
    process_failed_jobs     INT,
    job_execution_id        INT,
    job_name                VARCHAR(200),
    job_execution_order     INT,
    job_status              VARCHAR(50),
    job_start_time          TIMESTAMP,
    job_end_time            TIMESTAMP,
    job_total_datasets      INT,
    job_completed_datasets  INT,
    job_failed_datasets     INT,
    dataset_execution_id    INT,
    dataset_name            VARCHAR(200),
    dataset_source_type     VARCHAR(50),
    dataset_layer           VARCHAR(20),
    dataset_load_strategy   VARCHAR(50),
    dataset_execution_order INT,
    dataset_status          VARCHAR(50),
    dataset_start_time      TIMESTAMP,
    dataset_end_time        TIMESTAMP,
    dataset_rows_read       BIGINT,
    dataset_rows_written    BIGINT,
    dataset_rows_errored    BIGINT,
    dataset_bytes_processed BIGINT,
    dataset_error_message   TEXT,
    dataset_retry_count     INT,
    dataset_duration_seconds INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        pe.id AS process_execution_id,
        p.process_name,
        pe.status AS process_status,
        pe.triggered_by AS process_triggered_by,
        pe.start_time AS process_start_time,
        pe.end_time AS process_end_time,
        pe.environment AS process_environment,
        pe.total_jobs AS process_total_jobs,
        pe.completed_jobs AS process_completed_jobs,
        pe.failed_jobs AS process_failed_jobs,
        je.id AS job_execution_id,
        j.job_name,
        j.execution_order AS job_execution_order,
        je.status AS job_status,
        je.start_time AS job_start_time,
        je.end_time AS job_end_time,
        je.total_datasets AS job_total_datasets,
        je.completed_datasets AS job_completed_datasets,
        je.failed_datasets AS job_failed_datasets,
        de.id AS dataset_execution_id,
        d.dataset_name,
        d.source_type AS dataset_source_type,
        d.layer AS dataset_layer,
        d.load_strategy AS dataset_load_strategy,
        d.execution_order AS dataset_execution_order,
        de.status AS dataset_status,
        de.start_time AS dataset_start_time,
        de.end_time AS dataset_end_time,
        de.rows_read AS dataset_rows_read,
        de.rows_written AS dataset_rows_written,
        de.rows_errored AS dataset_rows_errored,
        de.bytes_processed AS dataset_bytes_processed,
        de.error_message AS dataset_error_message,
        de.retry_count AS dataset_retry_count,
        de.execution_duration_seconds AS dataset_duration_seconds
    FROM nodo_etl.etl_process_execution pe
    JOIN nodo_etl.etl_process p ON pe.process_id = p.id
    LEFT JOIN nodo_etl.etl_job_execution je ON pe.id = je.process_execution_id
    LEFT JOIN nodo_etl.etl_job j ON je.job_id = j.id
    LEFT JOIN nodo_etl.etl_dataset_execution de ON je.id = de.job_execution_id
    LEFT JOIN nodo_etl.etl_dataset d ON de.dataset_id = d.id
    WHERE (p_process_id IS NULL OR pe.process_id = p_process_id)
      AND (p_environment IS NULL OR pe.environment = p_environment)
      AND (p_status IS NULL OR pe.status = p_status)
      AND (p_start_date IS NULL OR pe.start_time >= p_start_date)
      AND (p_end_date IS NULL OR pe.start_time <= p_end_date)
    ORDER BY pe.start_time DESC, j.execution_order, d.execution_order;
END;
$$;
