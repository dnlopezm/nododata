-- V9: sp_complete_job_execution
-- Computes totals from dataset results and sets final job status

CREATE OR REPLACE FUNCTION nodo_etl.sp_complete_job_execution(
    p_job_execution_id INT,
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
    SELECT
        COUNT(*),
        COUNT(*) FILTER (WHERE status = 'success' OR status = 'skipped'),
        COUNT(*) FILTER (WHERE status = 'failed')
    INTO v_total, v_completed, v_failed
    FROM nodo_etl.etl_dataset_execution
    WHERE job_execution_id = p_job_execution_id;

    IF v_failed > 0 THEN
        v_status := 'failed';
    ELSIF v_completed = v_total AND v_total > 0 THEN
        v_status := 'success';
    ELSIF v_total = 0 THEN
        v_status := 'success';
    ELSE
        v_status := 'cancelled';
    END IF;

    UPDATE nodo_etl.etl_job_execution
    SET status = v_status,
        end_time = CURRENT_TIMESTAMP,
        total_datasets = v_total,
        completed_datasets = v_completed,
        failed_datasets = v_failed,
        updated_at = CURRENT_TIMESTAMP,
        updated_by = p_updated_by
    WHERE id = p_job_execution_id;
END;
$$;
