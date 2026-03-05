-- V3: Source configuration tables (SQL Server)

-- -------------------------------------------------------
-- etl_dataset_db_config
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_db_config (
    id                  INT IDENTITY(1,1) PRIMARY KEY,
    dataset_id          INT             NOT NULL UNIQUE REFERENCES nodo_etl.etl_dataset(id),
    source_schema       VARCHAR(200)    NULL,
    source_table        VARCHAR(200)    NULL,
    extraction_query    NVARCHAR(MAX)   NULL,
    watermark_column    VARCHAR(200)    NULL,
    primary_key_columns VARCHAR(500)    NULL,
    created_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by          VARCHAR(150)    NULL,
    updated_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by          VARCHAR(150)    NULL
);
GO

-- -------------------------------------------------------
-- etl_dataset_file_config
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_file_config (
    id                INT IDENTITY(1,1) PRIMARY KEY,
    dataset_id        INT             NOT NULL UNIQUE REFERENCES nodo_etl.etl_dataset(id),
    file_path_pattern VARCHAR(1000)   NOT NULL,
    file_format       VARCHAR(50)     NOT NULL,
    delimiter         VARCHAR(10)     NULL,
    has_header        BIT             DEFAULT 1,
    encoding          VARCHAR(50)     DEFAULT 'utf-8',
    compression       VARCHAR(50)     NULL,
    created_at        DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by        VARCHAR(150)    NULL,
    updated_at        DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by        VARCHAR(150)    NULL
);
GO

-- -------------------------------------------------------
-- etl_dataset_api_config
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_api_config (
    id                    INT IDENTITY(1,1) PRIMARY KEY,
    dataset_id            INT             NOT NULL UNIQUE REFERENCES nodo_etl.etl_dataset(id),
    api_url               VARCHAR(2000)   NOT NULL,
    api_method            VARCHAR(10)     NOT NULL,
    api_headers           NVARCHAR(MAX)   NULL,
    api_body_template     NVARCHAR(MAX)   NULL,
    pagination_strategy   VARCHAR(50)     NULL,
    pagination_config     NVARCHAR(MAX)   NULL,
    rate_limit_per_second INT             NULL,
    auth_method           VARCHAR(50)     NULL,
    response_path         VARCHAR(500)    NULL,
    created_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by            VARCHAR(150)    NULL,
    updated_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by            VARCHAR(150)    NULL
);
GO

-- -------------------------------------------------------
-- etl_dataset_stream_config
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_stream_config (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    dataset_id      INT             NOT NULL UNIQUE REFERENCES nodo_etl.etl_dataset(id),
    topic           VARCHAR(500)    NOT NULL,
    consumer_group  VARCHAR(200)    NULL,
    offset_strategy VARCHAR(50)     NOT NULL,
    specific_offset VARCHAR(200)    NULL,
    batch_size      INT             NULL,
    created_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by      VARCHAR(150)    NULL,
    updated_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by      VARCHAR(150)    NULL
);
GO
