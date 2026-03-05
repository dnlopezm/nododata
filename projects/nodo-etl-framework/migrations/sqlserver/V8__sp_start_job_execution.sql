-- V8: sp_start_job_execution (SQL Server)

CREATE OR ALTER PROCEDURE nodo_etl.sp_start_job_execution
    @job_execution_id INT,
    @updated_by VARCHAR(150) = 'system'
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE nodo_etl.etl_job_execution
    SET status = 'running',
        start_time = GETUTCDATE(),
        updated_at = GETUTCDATE(),
        updated_by = @updated_by
    WHERE id = @job_execution_id;

    IF @@ROWCOUNT = 0
        RAISERROR('Job execution %d not found', 16, 1, @job_execution_id);
END
GO
