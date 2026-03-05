-- V13: sp_get_jobs_to_execute (SQL Server)
-- Returns pending jobs for a process execution, respecting order and parallelism

CREATE OR ALTER PROCEDURE nodo_etl.sp_get_jobs_to_execute
    @process_execution_id INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @max_parallelism INT;
    DECLARE @min_pending_order INT;

    -- Get process-level max_parallelism
    SELECT @max_parallelism = COALESCE(p.max_parallelism,
        (SELECT CAST(config_value AS INT) FROM nodo_etl.etl_system_config WHERE config_key = 'default_parallelism'))
    FROM nodo_etl.etl_process_execution pe
    JOIN nodo_etl.etl_process p ON pe.process_id = p.id
    WHERE pe.id = @process_execution_id;

    -- Find the minimum execution_order among pending jobs
    SELECT @min_pending_order = MIN(j.execution_order)
    FROM nodo_etl.etl_job_execution je
    JOIN nodo_etl.etl_job j ON je.job_id = j.id
    WHERE je.process_execution_id = @process_execution_id
      AND je.status = 'pending';

    IF @min_pending_order IS NULL
        RETURN;

    SELECT TOP (@max_parallelism)
        je.id AS job_execution_id,
        j.id AS job_id,
        j.job_name,
        j.execution_order,
        j.max_parallelism
    FROM nodo_etl.etl_job_execution je
    JOIN nodo_etl.etl_job j ON je.job_id = j.id
    WHERE je.process_execution_id = @process_execution_id
      AND je.status = 'pending'
      AND j.execution_order = @min_pending_order
    ORDER BY j.execution_order, j.id;
END
GO
