-- V6: sp_start_process_execution
-- Creates process_execution, job_execution, and dataset_execution records

CREATE OR REPLACE FUNCTION nodo_etl.sp_start_process_execution(
    p_process_id    INT,
    p_environment   VARCHAR(50),
    p_triggered_by  VARCHAR(50),
    p_parameters    JSONB DEFAULT NULL,
    p_created_by    VARCHAR(150) DEFAULT 'system'
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_process_execution_id INT;
    v_job RECORD;
    v_job_execution_id INT;
    v_dataset RECORD;
BEGIN
    -- Validate process exists and is enabled
    IF NOT EXISTS (
        SELECT 1 FROM nodo_etl.etl_process
        WHERE id = p_process_id AND is_enabled = TRUE AND is_deleted = FALSE
    ) THEN
        RAISE EXCEPTION 'Process % does not exist, is disabled, or is deleted', p_process_id;
    END IF;

    -- Create process execution record
    INSERT INTO nodo_etl.etl_process_execution (
        process_id, environment, status, triggered_by,
        start_time, parameters, created_by, updated_by
    )
    VALUES (
        p_process_id, p_environment, 'running', p_triggered_by,
        CURRENT_TIMESTAMP, p_parameters, p_created_by, p_created_by
    )
    RETURNING id INTO v_process_execution_id;

    -- Create job execution records for all enabled, non-deleted jobs
    FOR v_job IN
        SELECT id FROM nodo_etl.etl_job
        WHERE process_id = p_process_id
          AND is_enabled = TRUE
          AND is_deleted = FALSE
        ORDER BY execution_order
    LOOP
        INSERT INTO nodo_etl.etl_job_execution (
            process_execution_id, job_id, status, created_by, updated_by
        )
        VALUES (
            v_process_execution_id, v_job.id, 'pending', p_created_by, p_created_by
        )
        RETURNING id INTO v_job_execution_id;

        -- Create dataset execution records for all enabled, non-deleted datasets
        FOR v_dataset IN
            SELECT id FROM nodo_etl.etl_dataset
            WHERE job_id = v_job.id
              AND is_enabled = TRUE
              AND is_deleted = FALSE
            ORDER BY execution_order
        LOOP
            INSERT INTO nodo_etl.etl_dataset_execution (
                job_execution_id, dataset_id, status, retry_count,
                created_by, updated_by
            )
            VALUES (
                v_job_execution_id, v_dataset.id, 'pending', 0,
                p_created_by, p_created_by
            );
        END LOOP;
    END LOOP;

    RETURN v_process_execution_id;
END;
$$;
