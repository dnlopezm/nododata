-- V10: sp_start_dataset_execution (SQL Server)

CREATE OR ALTER PROCEDURE nodo_etl.sp_start_dataset_execution
    @dataset_execution_id INT,
    @updated_by VARCHAR(150) = 'system'
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE nodo_etl.etl_dataset_execution
    SET status = 'running',
        start_time = GETUTCDATE(),
        updated_at = GETUTCDATE(),
        updated_by = @updated_by
    WHERE id = @dataset_execution_id;

    IF @@ROWCOUNT = 0
        RAISERROR('Dataset execution %d not found', 16, 1, @dataset_execution_id);
END
GO
