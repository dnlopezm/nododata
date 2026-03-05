-- V11: sp_complete_dataset_execution
-- Updates dataset execution with results, optionally updates watermark

CREATE OR REPLACE FUNCTION nodo_etl.sp_complete_dataset_execution(
    p_dataset_execution_id  INT,
    p_status                VARCHAR(50),
    p_rows_read             BIGINT DEFAULT NULL,
    p_rows_written          BIGINT DEFAULT NULL,
    p_rows_errored          BIGINT DEFAULT NULL,
    p_bytes_processed       BIGINT DEFAULT NULL,
    p_error_message         TEXT DEFAULT NULL,
    p_updated_by            VARCHAR(150) DEFAULT 'system'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_duration INT;
    v_dataset_id INT;
    v_load_strategy VARCHAR(50);
    v_watermark_column VARCHAR(200);
    v_environment VARCHAR(50);
    v_watermark_value VARCHAR(500);
BEGIN
    -- Get start_time and dataset info
    SELECT de.start_time, de.dataset_id
    INTO v_start_time, v_dataset_id
    FROM nodo_etl.etl_dataset_execution de
    WHERE de.id = p_dataset_execution_id;

    IF v_start_time IS NULL THEN
        v_duration := NULL;
    ELSE
        v_duration := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::INT;
    END IF;

    -- Update dataset execution
    UPDATE nodo_etl.etl_dataset_execution
    SET status = p_status,
        end_time = CURRENT_TIMESTAMP,
        rows_read = p_rows_read,
        rows_written = p_rows_written,
        rows_errored = p_rows_errored,
        bytes_processed = p_bytes_processed,
        error_message = p_error_message,
        execution_duration_seconds = v_duration,
        updated_at = CURRENT_TIMESTAMP,
        updated_by = p_updated_by
    WHERE id = p_dataset_execution_id;

    -- If successful and incremental, update watermark
    IF p_status = 'success' THEN
        SELECT d.load_strategy
        INTO v_load_strategy
        FROM nodo_etl.etl_dataset d
        WHERE d.id = v_dataset_id;

        IF v_load_strategy = 'incremental' THEN
            -- Get watermark column and environment
            SELECT dbc.watermark_column
            INTO v_watermark_column
            FROM nodo_etl.etl_dataset_db_config dbc
            WHERE dbc.dataset_id = v_dataset_id;

            SELECT pe.environment
            INTO v_environment
            FROM nodo_etl.etl_dataset_execution de
            JOIN nodo_etl.etl_job_execution je ON de.job_execution_id = je.id
            JOIN nodo_etl.etl_process_execution pe ON je.process_execution_id = pe.id
            WHERE de.id = p_dataset_execution_id;

            IF v_watermark_column IS NOT NULL THEN
                -- Use current timestamp as watermark value
                v_watermark_value := TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS');

                -- Upsert watermark
                INSERT INTO nodo_etl.etl_watermark (
                    dataset_id, environment, watermark_column,
                    last_watermark_value, last_successful_execution_id,
                    created_by, updated_by
                )
                VALUES (
                    v_dataset_id, v_environment, v_watermark_column,
                    v_watermark_value, p_dataset_execution_id,
                    p_updated_by, p_updated_by
                )
                ON CONFLICT (dataset_id, environment)
                DO UPDATE SET
                    last_watermark_value = EXCLUDED.last_watermark_value,
                    last_successful_execution_id = EXCLUDED.last_successful_execution_id,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = EXCLUDED.updated_by;
            END IF;
        END IF;
    END IF;
END;
$$;
