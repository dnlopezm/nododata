-- V11: sp_complete_dataset_execution (SQL Server)

CREATE OR ALTER PROCEDURE nodo_etl.sp_complete_dataset_execution
    @dataset_execution_id   INT,
    @status                 VARCHAR(50),
    @rows_read              BIGINT = NULL,
    @rows_written           BIGINT = NULL,
    @rows_errored           BIGINT = NULL,
    @bytes_processed        BIGINT = NULL,
    @error_message          NVARCHAR(MAX) = NULL,
    @updated_by             VARCHAR(150) = 'system'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME2;
    DECLARE @duration INT;
    DECLARE @dataset_id INT;
    DECLARE @load_strategy VARCHAR(50);
    DECLARE @watermark_column VARCHAR(200);
    DECLARE @environment VARCHAR(50);
    DECLARE @watermark_value VARCHAR(500);

    -- Get start_time and dataset info
    SELECT @start_time = start_time, @dataset_id = dataset_id
    FROM nodo_etl.etl_dataset_execution
    WHERE id = @dataset_execution_id;

    IF @start_time IS NOT NULL
        SET @duration = DATEDIFF(SECOND, @start_time, GETUTCDATE());

    -- Update dataset execution
    UPDATE nodo_etl.etl_dataset_execution
    SET status = @status,
        end_time = GETUTCDATE(),
        rows_read = @rows_read,
        rows_written = @rows_written,
        rows_errored = @rows_errored,
        bytes_processed = @bytes_processed,
        error_message = @error_message,
        execution_duration_seconds = @duration,
        updated_at = GETUTCDATE(),
        updated_by = @updated_by
    WHERE id = @dataset_execution_id;

    -- If successful and incremental, update watermark
    IF @status = 'success'
    BEGIN
        SELECT @load_strategy = load_strategy
        FROM nodo_etl.etl_dataset
        WHERE id = @dataset_id;

        IF @load_strategy = 'incremental'
        BEGIN
            SELECT @watermark_column = watermark_column
            FROM nodo_etl.etl_dataset_db_config
            WHERE dataset_id = @dataset_id;

            SELECT @environment = pe.environment
            FROM nodo_etl.etl_dataset_execution de
            JOIN nodo_etl.etl_job_execution je ON de.job_execution_id = je.id
            JOIN nodo_etl.etl_process_execution pe ON je.process_execution_id = pe.id
            WHERE de.id = @dataset_execution_id;

            IF @watermark_column IS NOT NULL
            BEGIN
                SET @watermark_value = CONVERT(VARCHAR(50), GETUTCDATE(), 120);

                -- Upsert watermark using MERGE
                MERGE nodo_etl.etl_watermark AS target
                USING (SELECT @dataset_id AS dataset_id, @environment AS environment) AS source
                ON target.dataset_id = source.dataset_id AND target.environment = source.environment
                WHEN MATCHED THEN
                    UPDATE SET
                        last_watermark_value = @watermark_value,
                        last_successful_execution_id = @dataset_execution_id,
                        updated_at = GETUTCDATE(),
                        updated_by = @updated_by
                WHEN NOT MATCHED THEN
                    INSERT (dataset_id, environment, watermark_column,
                            last_watermark_value, last_successful_execution_id,
                            created_by, updated_by)
                    VALUES (@dataset_id, @environment, @watermark_column,
                            @watermark_value, @dataset_execution_id,
                            @updated_by, @updated_by);
            END
        END
    END
END
GO
