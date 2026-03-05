-- V10: sp_start_dataset_execution
-- Marks a dataset execution as running

CREATE OR REPLACE FUNCTION nodo_etl.sp_start_dataset_execution(
    p_dataset_execution_id INT,
    p_updated_by VARCHAR(150) DEFAULT 'system'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE nodo_etl.etl_dataset_execution
    SET status = 'running',
        start_time = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP,
        updated_by = p_updated_by
    WHERE id = p_dataset_execution_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Dataset execution % not found', p_dataset_execution_id;
    END IF;
END;
$$;
