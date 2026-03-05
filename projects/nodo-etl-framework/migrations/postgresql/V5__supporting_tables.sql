-- V5: Supporting tables
-- Tables: etl_hook, etl_tag, etl_dataset_lineage, etl_system_config

-- -------------------------------------------------------
-- etl_hook
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_hook (
    id              SERIAL PRIMARY KEY,
    entity_type     VARCHAR(50)     NOT NULL,
    entity_id       INT             NOT NULL,
    hook_type       VARCHAR(10)     NOT NULL,
    execution_order INT             NOT NULL DEFAULT 1,
    action_type     VARCHAR(50)     NOT NULL,
    action_config   JSONB           NOT NULL,
    on_status       VARCHAR(50)     DEFAULT 'any',
    is_enabled      BOOLEAN         NOT NULL DEFAULT TRUE,
    is_deleted      BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by      VARCHAR(150),
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(150)
);

CREATE INDEX ix_etl_hook_entity
    ON nodo_etl.etl_hook (entity_type, entity_id);

CREATE INDEX ix_etl_hook_type
    ON nodo_etl.etl_hook (hook_type);

-- -------------------------------------------------------
-- etl_tag
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_tag (
    id          SERIAL PRIMARY KEY,
    entity_type VARCHAR(50)     NOT NULL,
    entity_id   INT             NOT NULL,
    tag_key     VARCHAR(100)    NOT NULL,
    tag_value   VARCHAR(500)    NOT NULL,
    created_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by  VARCHAR(150),
    updated_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by  VARCHAR(150)
);

CREATE UNIQUE INDEX uq_etl_tag_entity_key
    ON nodo_etl.etl_tag (entity_type, entity_id, tag_key);

CREATE INDEX ix_etl_tag_entity
    ON nodo_etl.etl_tag (entity_type, entity_id);

-- -------------------------------------------------------
-- etl_dataset_lineage
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_dataset_lineage (
    id                  SERIAL PRIMARY KEY,
    source_dataset_id   INT             NOT NULL REFERENCES nodo_etl.etl_dataset(id),
    target_dataset_id   INT             NOT NULL REFERENCES nodo_etl.etl_dataset(id),
    description         VARCHAR(1000),
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by          VARCHAR(150),
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by          VARCHAR(150)
);

CREATE UNIQUE INDEX uq_etl_lineage_source_target
    ON nodo_etl.etl_dataset_lineage (source_dataset_id, target_dataset_id);

CREATE INDEX ix_etl_lineage_source
    ON nodo_etl.etl_dataset_lineage (source_dataset_id);

CREATE INDEX ix_etl_lineage_target
    ON nodo_etl.etl_dataset_lineage (target_dataset_id);

-- -------------------------------------------------------
-- etl_system_config
-- -------------------------------------------------------
CREATE TABLE nodo_etl.etl_system_config (
    id           SERIAL PRIMARY KEY,
    config_key   VARCHAR(200)    NOT NULL,
    config_value VARCHAR(2000)   NOT NULL,
    description  VARCHAR(1000),
    created_at   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by   VARCHAR(150),
    updated_at   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by   VARCHAR(150)
);

CREATE UNIQUE INDEX uq_etl_system_config_key
    ON nodo_etl.etl_system_config (config_key);
