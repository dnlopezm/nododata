-- V1: Create the ETL metadata schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'nodo_etl')
BEGIN
    EXEC('CREATE SCHEMA nodo_etl');
END
GO
