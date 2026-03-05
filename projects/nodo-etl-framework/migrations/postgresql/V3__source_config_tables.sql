-- V3: Source configuration tables
-- One config table per source type, linked 1:1 to etl_dataset

-- -------------------------------------------------------
-- etl_dataset_db_config
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_db_config (
    id                  SERIAL PRIMARY KEY,
    dataset_id          INT             NOT NULL UNIQUE REFERENCES nodo_etl.etl_dataset(id),
    source_schema       VARCHAR(200),
    source_table        VARCHAR(200),
    extraction_query    TEXT,
    watermark_column    VARCHAR(200),
    primary_key_columns VARCHAR(500),
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by          VARCHAR(150),
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by          VARCHAR(150)
);

-- -------------------------------------------------------
-- etl_dataset_file_config
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_file_config (
    id                SERIAL PRIMARY KEY,
    dataset_id        INT             NOT NULL UNIQUE REFERENCES nodo_etl.etl_dataset(id),
    file_path_pattern VARCHAR(1000)   NOT NULL,
    file_format       VARCHAR(50)     NOT NULL,
    delimiter         VARCHAR(10),
    has_header        BOOLEAN         DEFAULT TRUE,
    encoding          VARCHAR(50)     DEFAULT 'utf-8',
    compression       VARCHAR(50),
    created_at        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by        VARCHAR(150),
    updated_at        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by        VARCHAR(150)
);

-- -------------------------------------------------------
-- etl_dataset_api_config
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_api_config (
    id                    SERIAL PRIMARY KEY,
    dataset_id            INT             NOT NULL UNIQUE REFERENCES nodo_etl.etl_dataset(id),
    api_url               VARCHAR(2000)   NOT NULL,
    api_method            VARCHAR(10)     NOT NULL,
    api_headers           JSONB,
    api_body_template     JSONB,
    pagination_strategy   VARCHAR(50),
    pagination_config     JSONB,
    rate_limit_per_second INT,
    auth_method           VARCHAR(50),
    response_path         VARCHAR(500),
    created_at            TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by            VARCHAR(150),
    updated_at            TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by            VARCHAR(150)
);

-- -------------------------------------------------------
-- etl_dataset_stream_config
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_stream_config (
    id              SERIAL PRIMARY KEY,
    dataset_id      INT             NOT NULL UNIQUE REFERENCES nodo_etl.etl_dataset(id),
    topic           VARCHAR(500)    NOT NULL,
    consumer_group  VARCHAR(200),
    offset_strategy VARCHAR(50)     NOT NULL,
    specific_offset VARCHAR(200),
    batch_size      INT,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by      VARCHAR(150),
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(150)
);
