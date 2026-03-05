-- V17: Seed sample data — Finance ETL Pipeline
-- Creates a complete example: process, connections, jobs, datasets, hooks, tags, lineage

-- Connections
INSERT INTO nodo_etl.etl_connection (connection_name, connection_type, host, port, database_name, username, secret_reference, secret_provider, environment, created_by, updated_by)
VALUES
    ('finance_source_db', 'postgresql', 'source-db.example.com', 5432, 'finance_db', 'etl_reader', 'FINANCE_DB_PASSWORD', 'env', 'dev', 'seed', 'seed'),
    ('finance_target_storage', 'postgresql', 'target-db.example.com', 5432, 'warehouse_db', 'etl_writer', 'WAREHOUSE_DB_PASSWORD', 'env', 'dev', 'seed', 'seed'),
    ('exchange_rate_api', 'http', 'api.exchangeratesapi.io', 443, NULL, NULL, 'EXCHANGE_API_KEY', 'env', 'dev', 'seed', 'seed');

-- Process
INSERT INTO nodo_etl.etl_process (process_name, description, execution_order, max_parallelism, max_retries, is_enabled, created_by, updated_by)
VALUES ('finance_etl', 'Finance data pipeline: transactions, accounts, and exchange rates', 1, 3, 2, TRUE, 'seed', 'seed');

-- Schedule (daily at 6am UTC)
INSERT INTO nodo_etl.etl_schedule (process_id, schedule_name, cron_expression, is_enabled, created_by, updated_by)
VALUES (
    (SELECT id FROM nodo_etl.etl_process WHERE process_name = 'finance_etl'),
    'daily_6am', '0 6 * * *', TRUE, 'seed', 'seed'
);

-- Job 1: Bronze Ingestion (order=1, parallelism=3)
INSERT INTO nodo_etl.etl_job (process_id, job_name, description, execution_order, max_parallelism, max_retries, is_enabled, created_by, updated_by)
VALUES (
    (SELECT id FROM nodo_etl.etl_process WHERE process_name = 'finance_etl'),
    'bronze_ingestion', 'Ingest raw data from sources into bronze layer', 1, 3, 3, TRUE, 'seed', 'seed'
);

-- Job 2: Silver Transformation (order=2, parallelism=2)
INSERT INTO nodo_etl.etl_job (process_id, job_name, description, execution_order, max_parallelism, max_retries, is_enabled, created_by, updated_by)
VALUES (
    (SELECT id FROM nodo_etl.etl_process WHERE process_name = 'finance_etl'),
    'silver_transformation', 'Clean and transform data into silver layer', 2, 2, 2, TRUE, 'seed', 'seed'
);

-- Job 3: Gold Aggregation (order=3, parallelism=1)
INSERT INTO nodo_etl.etl_job (process_id, job_name, description, execution_order, max_parallelism, max_retries, is_enabled, created_by, updated_by)
VALUES (
    (SELECT id FROM nodo_etl.etl_process WHERE process_name = 'finance_etl'),
    'gold_aggregation', 'Aggregate data into gold layer for reporting', 3, 1, 1, TRUE, 'seed', 'seed'
);

-- Bronze Datasets
INSERT INTO nodo_etl.etl_dataset (job_id, dataset_name, source_type, layer, load_strategy, execution_order, connection_id, max_retries, is_enabled, created_by, updated_by)
VALUES
    ((SELECT id FROM nodo_etl.etl_job WHERE job_name = 'bronze_ingestion'), 'transactions_bronze', 'database', 'bronze', 'incremental', 1,
     (SELECT id FROM nodo_etl.etl_connection WHERE connection_name = 'finance_source_db'), 3, TRUE, 'seed', 'seed'),
    ((SELECT id FROM nodo_etl.etl_job WHERE job_name = 'bronze_ingestion'), 'accounts_bronze', 'database', 'bronze', 'full', 1,
     (SELECT id FROM nodo_etl.etl_connection WHERE connection_name = 'finance_source_db'), 2, TRUE, 'seed', 'seed'),
    ((SELECT id FROM nodo_etl.etl_job WHERE job_name = 'bronze_ingestion'), 'exchange_rates_bronze', 'api', 'bronze', 'full', 1,
     (SELECT id FROM nodo_etl.etl_connection WHERE connection_name = 'exchange_rate_api'), 2, TRUE, 'seed', 'seed');

-- Silver Datasets
INSERT INTO nodo_etl.etl_dataset (job_id, dataset_name, source_type, layer, load_strategy, execution_order, connection_id, is_enabled, created_by, updated_by)
VALUES
    ((SELECT id FROM nodo_etl.etl_job WHERE job_name = 'silver_transformation'), 'transactions_silver', 'database', 'silver', 'incremental', 1,
     (SELECT id FROM nodo_etl.etl_connection WHERE connection_name = 'finance_target_storage'), TRUE, 'seed', 'seed'),
    ((SELECT id FROM nodo_etl.etl_job WHERE job_name = 'silver_transformation'), 'accounts_silver', 'database', 'silver', 'full', 1,
     (SELECT id FROM nodo_etl.etl_connection WHERE connection_name = 'finance_target_storage'), TRUE, 'seed', 'seed');

-- Gold Datasets
INSERT INTO nodo_etl.etl_dataset (job_id, dataset_name, source_type, layer, load_strategy, execution_order, connection_id, is_enabled, created_by, updated_by)
VALUES
    ((SELECT id FROM nodo_etl.etl_job WHERE job_name = 'gold_aggregation'), 'daily_revenue_gold', 'database', 'gold', 'full', 1,
     (SELECT id FROM nodo_etl.etl_connection WHERE connection_name = 'finance_target_storage'), TRUE, 'seed', 'seed');

-- DB Config for database datasets
INSERT INTO nodo_etl.etl_dataset_db_config (dataset_id, source_schema, source_table, target_schema, target_table, watermark_column, created_by, updated_by)
VALUES
    ((SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'transactions_bronze'), 'finance', 'transactions', 'bronze', 'transactions', 'updated_at', 'seed', 'seed'),
    ((SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'accounts_bronze'), 'finance', 'accounts', 'bronze', 'accounts', NULL, 'seed', 'seed'),
    ((SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'transactions_silver'), 'bronze', 'transactions', 'silver', 'transactions_clean', NULL, 'seed', 'seed'),
    ((SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'accounts_silver'), 'bronze', 'accounts', 'silver', 'accounts_clean', NULL, 'seed', 'seed'),
    ((SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'daily_revenue_gold'), 'silver', 'transactions_clean', 'gold', 'daily_revenue', NULL, 'seed', 'seed');

-- API Config for exchange rates
INSERT INTO nodo_etl.etl_dataset_api_config (dataset_id, api_url, http_method, auth_method, created_by, updated_by)
VALUES
    ((SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'exchange_rates_bronze'), 'https://api.exchangeratesapi.io/latest', 'GET', 'api_key', 'seed', 'seed');

-- Hooks
INSERT INTO nodo_etl.etl_hook (entity_type, entity_id, hook_type, action_type, action_config, execution_order, is_enabled, created_by, updated_by)
VALUES
    ('job', (SELECT id FROM nodo_etl.etl_job WHERE job_name = 'bronze_ingestion'), 'pre', 'sql',
     '{"query": "TRUNCATE TABLE bronze.staging_transactions"}', 1, TRUE, 'seed', 'seed'),
    ('process', (SELECT id FROM nodo_etl.etl_process WHERE process_name = 'finance_etl'), 'post', 'command',
     '{"command": "echo Finance ETL completed at $(date)"}', 1, TRUE, 'seed', 'seed');

-- Tags
INSERT INTO nodo_etl.etl_tag (entity_type, entity_id, tag_key, tag_value, created_by, updated_by)
VALUES
    ('process', (SELECT id FROM nodo_etl.etl_process WHERE process_name = 'finance_etl'), 'domain', 'finance', 'seed', 'seed'),
    ('process', (SELECT id FROM nodo_etl.etl_process WHERE process_name = 'finance_etl'), 'priority', 'high', 'seed', 'seed');

-- Lineage: bronze → silver → gold
INSERT INTO nodo_etl.etl_dataset_lineage (source_dataset_id, target_dataset_id, created_by, updated_by)
VALUES
    ((SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'transactions_bronze'),
     (SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'transactions_silver'), 'seed', 'seed'),
    ((SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'transactions_silver'),
     (SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'daily_revenue_gold'), 'seed', 'seed'),
    ((SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'accounts_bronze'),
     (SELECT id FROM nodo_etl.etl_dataset WHERE dataset_name = 'accounts_silver'), 'seed', 'seed');
