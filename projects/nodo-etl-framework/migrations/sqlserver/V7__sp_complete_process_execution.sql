-- V7: sp_complete_process_execution (SQL Server)

CREATE OR ALTER PROCEDURE nodo_etl.sp_complete_process_execution
    @process_execution_id INT,
    @updated_by VARCHAR(150) = 'system'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @total INT, @completed INT, @failed INT, @status VARCHAR(50);

    SELECT
        @total = COUNT(*),
        @completed = SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END),
        @failed = SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)
    FROM nodo_etl.etl_job_execution
    WHERE process_execution_id = @process_execution_id;

    IF @failed > 0
        SET @status = 'failed';
    ELSE IF @completed = @total AND @total > 0
        SET @status = 'success';
    ELSE IF @total = 0
        SET @status = 'success';
    ELSE
        SET @status = 'cancelled';

    UPDATE nodo_etl.etl_process_execution
    SET status = @status,
        end_time = GETUTCDATE(),
        total_jobs = @total,
        completed_jobs = @completed,
        failed_jobs = @failed,
        updated_at = GETUTCDATE(),
        updated_by = @updated_by
    WHERE id = @process_execution_id;
END
GO
