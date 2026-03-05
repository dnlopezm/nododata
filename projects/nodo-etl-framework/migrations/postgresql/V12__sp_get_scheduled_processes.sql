-- V12: sp_get_scheduled_processes
-- Returns enabled processes with active schedules
-- Note: Cron matching is done in the application layer (Python/Airflow)
-- This SP returns all enabled processes with at least one active schedule

CREATE OR REPLACE FUNCTION nodo_etl.sp_get_scheduled_processes()
RETURNS TABLE (
    process_id      INT,
    process_name    VARCHAR(200),
    execution_order INT,
    max_parallelism INT,
    schedule_id     INT,
    schedule_name   VARCHAR(200),
    cron_expression VARCHAR(100)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.id AS process_id,
        p.process_name,
        p.execution_order,
        p.max_parallelism,
        s.id AS schedule_id,
        s.schedule_name,
        s.cron_expression
    FROM nodo_etl.etl_process p
    INNER JOIN nodo_etl.etl_schedule s ON p.id = s.process_id
    WHERE p.is_enabled = TRUE
      AND p.is_deleted = FALSE
      AND s.is_enabled = TRUE
      AND s.is_deleted = FALSE
    ORDER BY p.execution_order, p.id;
END;
$$;
