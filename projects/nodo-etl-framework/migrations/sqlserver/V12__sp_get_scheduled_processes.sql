-- V12: sp_get_scheduled_processes (SQL Server)
-- Returns enabled processes with active schedules

CREATE OR ALTER PROCEDURE nodo_etl.sp_get_scheduled_processes
AS
BEGIN
    SET NOCOUNT ON;

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
    WHERE p.is_enabled = 1
      AND p.is_deleted = 0
      AND s.is_enabled = 1
      AND s.is_deleted = 0
    ORDER BY p.execution_order, p.id;
END
GO
