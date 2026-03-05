-- Create the configurable schema for ETL metadata
-- Schema name comes from NODO_ETL_SCHEMA env var (default: nodo_etl)
DO $$
DECLARE
    schema_name TEXT := COALESCE(current_setting('nodo_etl.schema', true), 'nodo_etl');
BEGIN
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema_name);
    RAISE NOTICE 'Created schema: %', schema_name;
END $$;

-- Fallback: always create nodo_etl schema
CREATE SCHEMA IF NOT EXISTS nodo_etl;
