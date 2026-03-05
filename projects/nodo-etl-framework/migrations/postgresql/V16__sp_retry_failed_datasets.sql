-- V16: sp_retry_failed_datasets
-- Finds failed datasets under max_retries and resets them for re-execution

CREATE OR REPLACE FUNCTION nodo_etl.sp_retry_failed_datasets(
    p_job_execution_id INT,
    p_updated_by VARCHAR(150) DEFAULT 'system'
)
RETURNS TABLE (
    dataset_execution_id INT,
    dataset_id           INT,
    dataset_name         VARCHAR(200),
    retry_count          INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_default_retries INT;
BEGIN
    -- Get system default max_retries
    SELECT config_value::INT
    INTO v_default_retries
    FROM nodo_etl.etl_system_config
    WHERE config_key = 'default_max_retries';

    -- Find and reset failed datasets that haven't exhausted retries
    -- Priority: dataset.max_retries > job.max_retries > process.max_retries > system default
    RETURN QUERY
    WITH retryable AS (
        SELECT
            de.id AS dataset_execution_id,
            d.id AS dataset_id,
            d.dataset_name,
            de.retry_count,
            COALESCE(
                d.max_retries,
                j.max_retries,
                p.max_retries,
                v_default_retries
            ) AS effective_max_retries
        FROM nodo_etl.etl_dataset_execution de
        JOIN nodo_etl.etl_dataset d ON de.dataset_id = d.id
        JOIN nodo_etl.etl_job_execution je ON de.job_execution_id = je.id
        JOIN nodo_etl.etl_job j ON je.job_id = j.id
        JOIN nodo_etl.etl_process p ON j.process_id = p.id
        WHERE de.job_execution_id = p_job_execution_id
          AND de.status = 'failed'
    )
    SELECT
        r.dataset_execution_id,
        r.dataset_id,
        r.dataset_name,
        r.retry_count + 1 AS retry_count
    FROM retryable r
    WHERE r.retry_count < r.effective_max_retries;

    -- Update the failed datasets to pending with incremented retry_count
    UPDATE nodo_etl.etl_dataset_execution de
    SET status = 'pending',
        retry_count = de.retry_count + 1,
        error_message = NULL,
        start_time = NULL,
        end_time = NULL,
        rows_read = NULL,
        rows_written = NULL,
        rows_errored = NULL,
        bytes_processed = NULL,
        execution_duration_seconds = NULL,
        updated_at = CURRENT_TIMESTAMP,
        updated_by = p_updated_by
    FROM nodo_etl.etl_dataset d
    JOIN nodo_etl.etl_job_execution je ON de.job_execution_id = je.id
    JOIN nodo_etl.etl_job j ON je.job_id = j.id
    JOIN nodo_etl.etl_process p ON j.process_id = p.id
    WHERE de.dataset_id = d.id
      AND de.job_execution_id = p_job_execution_id
      AND de.status = 'failed'
      AND de.retry_count < COALESCE(d.max_retries, j.max_retries, p.max_retries, v_default_retries);
END;
$$;
