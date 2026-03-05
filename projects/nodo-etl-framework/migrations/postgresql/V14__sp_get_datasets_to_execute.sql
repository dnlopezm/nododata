-- V14: sp_get_datasets_to_execute
-- Returns pending datasets for a job execution, respecting order and parallelism

CREATE OR REPLACE FUNCTION nodo_etl.sp_get_datasets_to_execute(
    p_job_execution_id INT
)
RETURNS TABLE (
    dataset_execution_id INT,
    dataset_id           INT,
    dataset_name         VARCHAR(200),
    source_type          VARCHAR(50),
    layer                VARCHAR(20),
    load_strategy        VARCHAR(50),
    execution_order      INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_max_parallelism INT;
    v_min_pending_order INT;
BEGIN
    -- Get job-level max_parallelism, fall back to process, then system default
    SELECT COALESCE(
        j.max_parallelism,
        p.max_parallelism,
        (SELECT config_value::INT FROM nodo_etl.etl_system_config WHERE config_key = 'default_parallelism')
    )
    INTO v_max_parallelism
    FROM nodo_etl.etl_job_execution je
    JOIN nodo_etl.etl_job j ON je.job_id = j.id
    JOIN nodo_etl.etl_process p ON j.process_id = p.id
    WHERE je.id = p_job_execution_id;

    -- Find the minimum execution_order among pending datasets
    SELECT MIN(d.execution_order)
    INTO v_min_pending_order
    FROM nodo_etl.etl_dataset_execution de
    JOIN nodo_etl.etl_dataset d ON de.dataset_id = d.id
    WHERE de.job_execution_id = p_job_execution_id
      AND de.status = 'pending';

    IF v_min_pending_order IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT
        de.id AS dataset_execution_id,
        d.id AS dataset_id,
        d.dataset_name,
        d.source_type,
        d.layer,
        d.load_strategy,
        d.execution_order
    FROM nodo_etl.etl_dataset_execution de
    JOIN nodo_etl.etl_dataset d ON de.dataset_id = d.id
    WHERE de.job_execution_id = p_job_execution_id
      AND de.status = 'pending'
      AND d.execution_order = v_min_pending_order
    ORDER BY d.execution_order, d.id
    LIMIT v_max_parallelism;
END;
$$;
