-- V7: sp_complete_process_execution
-- Computes totals from job results and sets final process status

CREATE OR REPLACE FUNCTION nodo_etl.sp_complete_process_execution(
    p_process_execution_id INT,
    p_updated_by VARCHAR(150) DEFAULT 'system'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_total INT;
    v_completed INT;
    v_failed INT;
    v_status VARCHAR(50);
BEGIN
    -- Compute job execution totals
    SELECT
        COUNT(*),
        COUNT(*) FILTER (WHERE status = 'success'),
        COUNT(*) FILTER (WHERE status = 'failed')
    INTO v_total, v_completed, v_failed
    FROM nodo_etl.etl_job_execution
    WHERE process_execution_id = p_process_execution_id;

    -- Determine process status
    IF v_failed > 0 THEN
        v_status := 'failed';
    ELSIF v_completed = v_total AND v_total > 0 THEN
        v_status := 'success';
    ELSIF v_total = 0 THEN
        v_status := 'success';
    ELSE
        v_status := 'cancelled';
    END IF;

    -- Update process execution
    UPDATE nodo_etl.etl_process_execution
    SET status = v_status,
        end_time = CURRENT_TIMESTAMP,
        total_jobs = v_total,
        completed_jobs = v_completed,
        failed_jobs = v_failed,
        updated_at = CURRENT_TIMESTAMP,
        updated_by = p_updated_by
    WHERE id = p_process_execution_id;
END;
$$;
