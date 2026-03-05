-- V6: sp_start_process_execution (SQL Server)

CREATE OR ALTER PROCEDURE nodo_etl.sp_start_process_execution
    @process_id     INT,
    @environment    VARCHAR(50),
    @triggered_by   VARCHAR(50),
    @parameters     NVARCHAR(MAX) = NULL,
    @created_by     VARCHAR(150) = 'system',
    @process_execution_id INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate process exists and is enabled
    IF NOT EXISTS (
        SELECT 1 FROM nodo_etl.etl_process
        WHERE id = @process_id AND is_enabled = 1 AND is_deleted = 0
    )
    BEGIN
        RAISERROR('Process %d does not exist, is disabled, or is deleted', 16, 1, @process_id);
        RETURN;
    END

    -- Create process execution record
    INSERT INTO nodo_etl.etl_process_execution (
        process_id, environment, status, triggered_by,
        start_time, parameters, created_by, updated_by
    )
    VALUES (
        @process_id, @environment, 'running', @triggered_by,
        GETUTCDATE(), @parameters, @created_by, @created_by
    );

    SET @process_execution_id = SCOPE_IDENTITY();

    -- Create job execution records for all enabled, non-deleted jobs
    DECLARE @job_id INT;
    DECLARE @job_execution_id INT;

    DECLARE job_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT id FROM nodo_etl.etl_job
        WHERE process_id = @process_id
          AND is_enabled = 1
          AND is_deleted = 0
        ORDER BY execution_order;

    OPEN job_cursor;
    FETCH NEXT FROM job_cursor INTO @job_id;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        INSERT INTO nodo_etl.etl_job_execution (
            process_execution_id, job_id, status, created_by, updated_by
        )
        VALUES (
            @process_execution_id, @job_id, 'pending', @created_by, @created_by
        );

        SET @job_execution_id = SCOPE_IDENTITY();

        -- Create dataset execution records
        INSERT INTO nodo_etl.etl_dataset_execution (
            job_execution_id, dataset_id, status, retry_count,
            created_by, updated_by
        )
        SELECT
            @job_execution_id, id, 'pending', 0,
            @created_by, @created_by
        FROM nodo_etl.etl_dataset
        WHERE job_id = @job_id
          AND is_enabled = 1
          AND is_deleted = 0
        ORDER BY execution_order;

        FETCH NEXT FROM job_cursor INTO @job_id;
    END

    CLOSE job_cursor;
    DEALLOCATE job_cursor;
END
GO
