-- V5.1: Seed default system configuration values (SQL Server)

INSERT INTO nodo_etl.etl_system_config (config_key, config_value, description, created_by)
VALUES
    ('default_max_retries', '3', 'Default retry count when not set on entity', 'system'),
    ('default_retry_delay_seconds', '60', 'Default delay between retries in seconds', 'system'),
    ('default_timeout_seconds', '3600', 'Default timeout per dataset execution (1 hour)', 'system'),
    ('default_parallelism', '5', 'Default max parallel datasets/jobs when not set', 'system'),
    ('schema_version', '1.0.0', 'Current metadata schema version', 'system'),
    ('framework_version', '1.0.0', 'Nodo ETL Framework version', 'system'),
    ('secret_provider', 'env', 'Default secret provider type (env, keyvault, aws_sm, airflow)', 'system'),
    ('timezone', 'America/Mexico_City', 'Timezone for schedule evaluation and logging', 'system'),
    ('log_retention_days', '90', 'Number of days to keep execution history', 'system');
GO
