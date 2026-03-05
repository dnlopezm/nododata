-- V5: Supporting tables (SQL Server)

-- -------------------------------------------------------
-- etl_hook
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_hook (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    entity_type     VARCHAR(50)     NOT NULL,
    entity_id       INT             NOT NULL,
    hook_type       VARCHAR(10)     NOT NULL,
    execution_order INT             NOT NULL DEFAULT 1,
    action_type     VARCHAR(50)     NOT NULL,
    action_config   NVARCHAR(MAX)   NOT NULL,
    on_status       VARCHAR(50)     DEFAULT 'any',
    is_enabled      BIT             NOT NULL DEFAULT 1,
    is_deleted      BIT             NOT NULL DEFAULT 0,
    created_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by      VARCHAR(150)    NULL,
    updated_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by      VARCHAR(150)    NULL
);
GO

CREATE INDEX ix_etl_hook_entity
    ON nodo_etl.etl_hook (entity_type, entity_id);
GO

CREATE INDEX ix_etl_hook_type
    ON nodo_etl.etl_hook (hook_type);
GO

-- -------------------------------------------------------
-- etl_tag
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_tag (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    entity_type VARCHAR(50)     NOT NULL,
    entity_id   INT             NOT NULL,
    tag_key     VARCHAR(100)    NOT NULL,
    tag_value   VARCHAR(500)    NOT NULL,
    created_at  DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by  VARCHAR(150)    NULL,
    updated_at  DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by  VARCHAR(150)    NULL
);
GO

CREATE UNIQUE INDEX uq_etl_tag_entity_key
    ON nodo_etl.etl_tag (entity_type, entity_id, tag_key);
GO

CREATE INDEX ix_etl_tag_entity
    ON nodo_etl.etl_tag (entity_type, entity_id);
GO

-- -------------------------------------------------------
-- etl_dataset_lineage
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_lineage (
    id                  INT IDENTITY(1,1) PRIMARY KEY,
    source_dataset_id   INT             NOT NULL REFERENCES nodo_etl.etl_dataset(id),
    target_dataset_id   INT             NOT NULL REFERENCES nodo_etl.etl_dataset(id),
    description         VARCHAR(1000)   NULL,
    created_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by          VARCHAR(150)    NULL,
    updated_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by          VARCHAR(150)    NULL
);
GO

CREATE UNIQUE INDEX uq_etl_lineage_source_target
    ON nodo_etl.etl_dataset_lineage (source_dataset_id, target_dataset_id);
GO

CREATE INDEX ix_etl_lineage_source
    ON nodo_etl.etl_dataset_lineage (source_dataset_id);
GO

CREATE INDEX ix_etl_lineage_target
    ON nodo_etl.etl_dataset_lineage (target_dataset_id);
GO

-- -------------------------------------------------------
-- etl_system_config
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_system_config (
    id           INT IDENTITY(1,1) PRIMARY KEY,
    config_key   VARCHAR(200)    NOT NULL,
    config_value VARCHAR(2000)   NOT NULL,
    description  VARCHAR(1000)   NULL,
    created_at   DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by   VARCHAR(150)    NULL,
    updated_at   DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_by   VARCHAR(150)    NULL
);
GO

CREATE UNIQUE INDEX uq_etl_system_config_key
    ON nodo_etl.etl_system_config (config_key);
GO
