-- V13: sp_get_jobs_to_execute
-- Returns pending jobs for a process execution, respecting order and parallelism

CREATE OR REPLACE FUNCTION nodo_etl.sp_get_jobs_to_execute(
    p_process_execution_id INT
)
RETURNS TABLE (
    job_execution_id INT,
    job_id           INT,
    job_name         VARCHAR(200),
    execution_order  INT,
    max_parallelism  INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_max_parallelism INT;
    v_min_pending_order INT;
BEGIN
    -- Get process-level max_parallelism
    SELECT COALESCE(p.max_parallelism,
        (SELECT config_value::INT FROM nodo_etl.etl_system_config WHERE config_key = 'default_parallelism'))
    INTO v_max_parallelism
    FROM nodo_etl.etl_process_execution pe
    JOIN nodo_etl.etl_process p ON pe.process_id = p.id
    WHERE pe.id = p_process_execution_id;

    -- Find the minimum execution_order among pending jobs
    SELECT MIN(j.execution_order)
    INTO v_min_pending_order
    FROM nodo_etl.etl_job_execution je
    JOIN nodo_etl.etl_job j ON je.job_id = j.id
    WHERE je.process_execution_id = p_process_execution_id
      AND je.status = 'pending';

    IF v_min_pending_order IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT
        je.id AS job_execution_id,
        j.id AS job_id,
        j.job_name,
        j.execution_order,
        j.max_parallelism
    FROM nodo_etl.etl_job_execution je
    JOIN nodo_etl.etl_job j ON je.job_id = j.id
    WHERE je.process_execution_id = p_process_execution_id
      AND je.status = 'pending'
      AND j.execution_order = v_min_pending_order
    ORDER BY j.execution_order, j.id
    LIMIT v_max_parallelism;
END;
$$;
