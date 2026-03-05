-- V14: sp_get_datasets_to_execute (SQL Server)
-- Returns pending datasets for a job execution, respecting order and parallelism

CREATE OR ALTER PROCEDURE nodo_etl.sp_get_datasets_to_execute
    @job_execution_id INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @max_parallelism INT;
    DECLARE @min_pending_order INT;

    -- Get job-level max_parallelism, fall back to process, then system default
    SELECT @max_parallelism = COALESCE(
        j.max_parallelism,
        p.max_parallelism,
        (SELECT CAST(config_value AS INT) FROM nodo_etl.etl_system_config WHERE config_key = 'default_parallelism')
    )
    FROM nodo_etl.etl_job_execution je
    JOIN nodo_etl.etl_job j ON je.job_id = j.id
    JOIN nodo_etl.etl_process p ON j.process_id = p.id
    WHERE je.id = @job_execution_id;

    -- Find the minimum execution_order among pending datasets
    SELECT @min_pending_order = MIN(d.execution_order)
    FROM nodo_etl.etl_dataset_execution de
    JOIN nodo_etl.etl_dataset d ON de.dataset_id = d.id
    WHERE de.job_execution_id = @job_execution_id
      AND de.status = 'pending';

    IF @min_pending_order IS NULL
        RETURN;

    SELECT TOP (@max_parallelism)
        de.id AS dataset_execution_id,
        d.id AS dataset_id,
        d.dataset_name,
        d.source_type,
        d.layer,
        d.load_strategy,
        d.execution_order
    FROM nodo_etl.etl_dataset_execution de
    JOIN nodo_etl.etl_dataset d ON de.dataset_id = d.id
    WHERE de.job_execution_id = @job_execution_id
      AND de.status = 'pending'
      AND d.execution_order = @min_pending_order
    ORDER BY d.execution_order, d.id;
END
GO
