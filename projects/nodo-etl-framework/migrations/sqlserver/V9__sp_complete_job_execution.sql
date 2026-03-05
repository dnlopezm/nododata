-- V9: sp_complete_job_execution (SQL Server)

CREATE OR ALTER PROCEDURE nodo_etl.sp_complete_job_execution
    @job_execution_id INT,
    @updated_by VARCHAR(150) = 'system'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @total INT, @completed INT, @failed INT, @status VARCHAR(50);

    SELECT
        @total = COUNT(*),
        @completed = SUM(CASE WHEN status IN ('success', 'skipped') THEN 1 ELSE 0 END),
        @failed = SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)
    FROM nodo_etl.etl_dataset_execution
    WHERE job_execution_id = @job_execution_id;

    IF @failed > 0
        SET @status = 'failed';
    ELSE IF @completed = @total AND @total > 0
        SET @status = 'success';
    ELSE IF @total = 0
        SET @status = 'success';
    ELSE
        SET @status = 'cancelled';

    UPDATE nodo_etl.etl_job_execution
    SET status = @status,
        end_time = GETUTCDATE(),
        total_datasets = @total,
        completed_datasets = @completed,
        failed_datasets = @failed,
        updated_at = GETUTCDATE(),
        updated_by = @updated_by
    WHERE id = @job_execution_id;
END
GO
