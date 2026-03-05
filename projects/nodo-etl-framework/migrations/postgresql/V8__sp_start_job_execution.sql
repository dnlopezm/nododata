-- V8: sp_start_job_execution
-- Marks a job execution as running

CREATE OR REPLACE FUNCTION nodo_etl.sp_start_job_execution(
    p_job_execution_id INT,
    p_updated_by VARCHAR(150) DEFAULT 'system'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE nodo_etl.etl_job_execution
    SET status = 'running',
        start_time = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP,
        updated_by = p_updated_by
    WHERE id = p_job_execution_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job execution % not found', p_job_execution_id;
    END IF;
END;
$$;
