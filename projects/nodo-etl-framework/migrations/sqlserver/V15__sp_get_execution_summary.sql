-- V15: sp_get_execution_summary (SQL Server)
-- The "big query" — joins all execution and metadata tables

CREATE OR ALTER PROCEDURE nodo_etl.sp_get_execution_summary
    @process_id     INT = NULL,
    @environment    VARCHAR(50) = NULL,
    @status         VARCHAR(50) = NULL,
    @start_date     DATETIME2 = NULL,
    @end_date       DATETIME2 = NULL
AS
BEGIN
    SET NOCOUNT ON;

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
    WHERE (@process_id IS NULL OR pe.process_id = @process_id)
      AND (@environment IS NULL OR pe.environment = @environment)
      AND (@status IS NULL OR pe.status = @status)
      AND (@start_date IS NULL OR pe.start_time >= @start_date)
      AND (@end_date IS NULL OR pe.start_time <= @end_date)
    ORDER BY pe.start_time DESC, j.execution_order, d.execution_order;
END
GO
