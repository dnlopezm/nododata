-- V17: Seed sample data — Finance ETL Pipeline (SQL Server)

-- Connections
INSERT INTO nodo_etl.etl_connection (connection_name, connection_type, host, port, database_name, username, secret_reference, secret_provider, environment, created_by, updated_by)
VALUES
    ('finance_source_db', 'postgresql', 'source-db.example.com', 5432, 'finance_db', 'etl_reader', 'FINANCE_DB_PASSWORD', 'env', 'dev', 'seed', 'seed'),
    ('finance_target_storage', 'postgresql', 'target-db.example.com', 5432, 'warehouse_db', 'etl_writer', 'WAREHOUSE_DB_PASSWORD', 'env', 'dev', 'seed', 'seed'),
    ('exchange_rate_api', 'http', 'api.exchangeratesapi.io', 443, NULL, NULL, 'EXCHANGE_API_KEY', 'env', 'dev', 'seed', 'seed');

DECLARE @process_id INT, @job_bronze_id INT, @job_silver_id INT, @job_gold_id INT;
DECLARE @conn_source INT, @conn_target INT, @conn_api INT;
DECLARE @ds_txn_bronze INT, @ds_acct_bronze INT, @ds_fx_bronze INT;
DECLARE @ds_txn_silver INT, @ds_acct_silver INT, @ds_rev_gold INT;

SELECT @conn_source = id FROM nodo_etl.etl_connection WHERE connection_name = 'finance_source_db';
SELECT @conn_target = id FROM nodo_etl.etl_connection WHERE connection_name = 'finance_target_storage';
SELECT @conn_api = id FROM nodo_etl.etl_connection WHERE connection_name = 'exchange_rate_api';

-- Process
INSERT INTO nodo_etl.etl_process (process_name, description, execution_order, max_parallelism, max_retries, is_enabled, created_by, updated_by)
VALUES ('finance_etl', 'Finance data pipeline: transactions, accounts, and exchange rates', 1, 3, 2, 1, 'seed', 'seed');
SET @process_id = SCOPE_IDENTITY();

-- Schedule
INSERT INTO nodo_etl.etl_schedule (process_id, schedule_name, cron_expression, is_enabled, created_by, updated_by)
VALUES (@process_id, 'daily_6am', '0 6 * * *', 1, 'seed', 'seed');

-- Job 1: Bronze
INSERT INTO nodo_etl.etl_job (process_id, job_name, description, execution_order, max_parallelism, max_retries, is_enabled, created_by, updated_by)
VALUES (@process_id, 'bronze_ingestion', 'Ingest raw data from sources into bronze layer', 1, 3, 3, 1, 'seed', 'seed');
SET @job_bronze_id = SCOPE_IDENTITY();

-- Job 2: Silver
INSERT INTO nodo_etl.etl_job (process_id, job_name, description, execution_order, max_parallelism, max_retries, is_enabled, created_by, updated_by)
VALUES (@process_id, 'silver_transformation', 'Clean and transform data into silver layer', 2, 2, 2, 1, 'seed', 'seed');
SET @job_silver_id = SCOPE_IDENTITY();

-- Job 3: Gold
INSERT INTO nodo_etl.etl_job (process_id, job_name, description, execution_order, max_parallelism, max_retries, is_enabled, created_by, updated_by)
VALUES (@process_id, 'gold_aggregation', 'Aggregate data into gold layer for reporting', 3, 1, 1, 1, 'seed', 'seed');
SET @job_gold_id = SCOPE_IDENTITY();

-- Bronze Datasets
INSERT INTO nodo_etl.etl_dataset (job_id, dataset_name, source_type, layer, load_strategy, execution_order, connection_id, max_retries, is_enabled, created_by, updated_by)
VALUES (@job_bronze_id, 'transactions_bronze', 'database', 'bronze', 'incremental', 1, @conn_source, 3, 1, 'seed', 'seed');
SET @ds_txn_bronze = SCOPE_IDENTITY();

INSERT INTO nodo_etl.etl_dataset (job_id, dataset_name, source_type, layer, load_strategy, execution_order, connection_id, max_retries, is_enabled, created_by, updated_by)
VALUES (@job_bronze_id, 'accounts_bronze', 'database', 'bronze', 'full', 1, @conn_source, 2, 1, 'seed', 'seed');
SET @ds_acct_bronze = SCOPE_IDENTITY();

INSERT INTO nodo_etl.etl_dataset (job_id, dataset_name, source_type, layer, load_strategy, execution_order, connection_id, max_retries, is_enabled, created_by, updated_by)
VALUES (@job_bronze_id, 'exchange_rates_bronze', 'api', 'bronze', 'full', 1, @conn_api, 2, 1, 'seed', 'seed');
SET @ds_fx_bronze = SCOPE_IDENTITY();

-- Silver Datasets
INSERT INTO nodo_etl.etl_dataset (job_id, dataset_name, source_type, layer, load_strategy, execution_order, connection_id, is_enabled, created_by, updated_by)
VALUES (@job_silver_id, 'transactions_silver', 'database', 'silver', 'incremental', 1, @conn_target, 1, 'seed', 'seed');
SET @ds_txn_silver = SCOPE_IDENTITY();

INSERT INTO nodo_etl.etl_dataset (job_id, dataset_name, source_type, layer, load_strategy, execution_order, connection_id, is_enabled, created_by, updated_by)
VALUES (@job_silver_id, 'accounts_silver', 'database', 'silver', 'full', 1, @conn_target, 1, 'seed', 'seed');
SET @ds_acct_silver = SCOPE_IDENTITY();

-- Gold Datasets
INSERT INTO nodo_etl.etl_dataset (job_id, dataset_name, source_type, layer, load_strategy, execution_order, connection_id, is_enabled, created_by, updated_by)
VALUES (@job_gold_id, 'daily_revenue_gold', 'database', 'gold', 'full', 1, @conn_target, 1, 'seed', 'seed');
SET @ds_rev_gold = SCOPE_IDENTITY();

-- DB Configs
INSERT INTO nodo_etl.etl_dataset_db_config (dataset_id, source_schema, source_table, target_schema, target_table, watermark_column, created_by, updated_by)
VALUES
    (@ds_txn_bronze, 'finance', 'transactions', 'bronze', 'transactions', 'updated_at', 'seed', 'seed'),
    (@ds_acct_bronze, 'finance', 'accounts', 'bronze', 'accounts', NULL, 'seed', 'seed'),
    (@ds_txn_silver, 'bronze', 'transactions', 'silver', 'transactions_clean', NULL, 'seed', 'seed'),
    (@ds_acct_silver, 'bronze', 'accounts', 'silver', 'accounts_clean', NULL, 'seed', 'seed'),
    (@ds_rev_gold, 'silver', 'transactions_clean', 'gold', 'daily_revenue', NULL, 'seed', 'seed');

-- API Config
INSERT INTO nodo_etl.etl_dataset_api_config (dataset_id, api_url, http_method, auth_method, created_by, updated_by)
VALUES (@ds_fx_bronze, 'https://api.exchangeratesapi.io/latest', 'GET', 'api_key', 'seed', 'seed');

-- Hooks
INSERT INTO nodo_etl.etl_hook (entity_type, entity_id, hook_type, action_type, action_config, execution_order, is_enabled, created_by, updated_by)
VALUES
    ('job', @job_bronze_id, 'pre', 'sql', '{"query": "TRUNCATE TABLE bronze.staging_transactions"}', 1, 1, 'seed', 'seed'),
    ('process', @process_id, 'post', 'command', '{"command": "echo Finance ETL completed"}', 1, 1, 'seed', 'seed');

-- Tags
INSERT INTO nodo_etl.etl_tag (entity_type, entity_id, tag_key, tag_value, created_by, updated_by)
VALUES
    ('process', @process_id, 'domain', 'finance', 'seed', 'seed'),
    ('process', @process_id, 'priority', 'high', 'seed', 'seed');

-- Lineage
INSERT INTO nodo_etl.etl_dataset_lineage (source_dataset_id, target_dataset_id, created_by, updated_by)
VALUES
    (@ds_txn_bronze, @ds_txn_silver, 'seed', 'seed'),
    (@ds_txn_silver, @ds_rev_gold, 'seed', 'seed'),
    (@ds_acct_bronze, @ds_acct_silver, 'seed', 'seed');
GO
