-- V16: sp_retry_failed_datasets (SQL Server)
-- Finds failed datasets under max_retries and resets them for re-execution

CREATE OR ALTER PROCEDURE nodo_etl.sp_retry_failed_datasets
    @job_execution_id INT,
    @updated_by VARCHAR(150) = 'system'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @default_retries INT;

    -- Get system default max_retries
    SELECT @default_retries = CAST(config_value AS INT)
    FROM nodo_etl.etl_system_config
    WHERE config_key = 'default_max_retries';

    -- Return retryable datasets
    SELECT
        de.id AS dataset_execution_id,
        d.id AS dataset_id,
        d.dataset_name,
        de.retry_count + 1 AS retry_count
    FROM nodo_etl.etl_dataset_execution de
    JOIN nodo_etl.etl_dataset d ON de.dataset_id = d.id
    JOIN nodo_etl.etl_job_execution je ON de.job_execution_id = je.id
    JOIN nodo_etl.etl_job j ON je.job_id = j.id
    JOIN nodo_etl.etl_process p ON j.process_id = p.id
    WHERE de.job_execution_id = @job_execution_id
      AND de.status = 'failed'
      AND de.retry_count < COALESCE(d.max_retries, j.max_retries, p.max_retries, @default_retries);

    -- Update the failed datasets to pending with incremented retry_count
    UPDATE de
    SET de.status = 'pending',
        de.retry_count = de.retry_count + 1,
        de.error_message = NULL,
        de.start_time = NULL,
        de.end_time = NULL,
        de.rows_read = NULL,
        de.rows_written = NULL,
        de.rows_errored = NULL,
        de.bytes_processed = NULL,
        de.execution_duration_seconds = NULL,
        de.updated_at = GETUTCDATE(),
        de.updated_by = @updated_by
    FROM nodo_etl.etl_dataset_execution de
    JOIN nodo_etl.etl_dataset d ON de.dataset_id = d.id
    JOIN nodo_etl.etl_job_execution je ON de.job_execution_id = je.id
    JOIN nodo_etl.etl_job j ON je.job_id = j.id
    JOIN nodo_etl.etl_process p ON j.process_id = p.id
    WHERE de.job_execution_id = @job_execution_id
      AND de.status = 'failed'
      AND de.retry_count < COALESCE(d.max_retries, j.max_retries, p.max_retries, @default_retries);
END
GO
