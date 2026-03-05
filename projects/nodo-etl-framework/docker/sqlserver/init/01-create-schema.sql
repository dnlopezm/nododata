-- Create the ETL metadata database and schema for SQL Server
-- This script runs after SQL Server starts

-- Create database if not exists
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'nodo_etl_db')
BEGIN
    CREATE DATABASE nodo_etl_db;
END
GO

USE nodo_etl_db;
GO

-- Create schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'nodo_etl')
BEGIN
    EXEC('CREATE SCHEMA nodo_etl');
END
GO
