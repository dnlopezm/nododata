# Nodo Data - Master Backlog

Overall progress tracker for all Nodo Data projects.

**Last Updated:** March 4, 2026

---

## Projects Overview

| Project | Status | Progress | Backlog |
|---------|--------|----------|---------|
| Nodo ETL Framework | In Progress | 0/7 phases | [BACKLOG_ETL.md](BACKLOG_ETL.md) |

---

## Nodo ETL Framework — Phase Summary

Metadata-driven ETL framework for orchestrating data pipelines (bronze → silver → gold).

| Phase | Name | Status | Description |
|-------|------|--------|-------------|
| E1 | Foundation & Docker Setup | Not Started | Python project, Docker (SQL Server + PostgreSQL + Airflow), config |
| E2 | Metadata Database Migrations | Not Started | Flyway migrations for all tables (both DB engines) |
| E3 | Stored Procedures | Not Started | Execution lifecycle SPs (both DB engines) |
| E4 | Python Core & Secret Provider | Not Started | DB connection layer, repositories, secret abstraction |
| E5 | Python CLI | Not Started | CRUD commands for all metadata entities |
| E6 | Airflow Orchestration | Not Started | DAGs reading metadata, triggering pipelines, tracking execution |
| E7 | Integration Testing & Docs | Not Started | End-to-end tests, sample data, documentation |

### Future Phases (Deferred)

| Phase | Name | Description |
|-------|------|-------------|
| E8 | Target Config & Column Mapping | Target-specific loading, column transformations |
| E9 | DDL Generation | Auto-generate bronze/silver/gold DDLs from source metadata |
| E10 | Dynamic Transformation Code | Generate SQL/Spark for data movement between layers |
| E11 | ADF & Databricks Orchestration | Azure Data Factory + Databricks integration |
| E12 | dbt Integration | Generate dbt models from metadata |
| E13 | Notifications & Alerting | Email, Slack, webhook alerts |
| E14 | Data Quality Checks | Validation rules between layers |
| E15 | Schema Drift Detection | Track and alert on source schema changes |
| E16 | Additional Secret Providers | Key Vault, AWS SM, Airflow, Vault |
| E17 | Audit History | Full change log for metadata modifications |
| E18 | CDC & Streaming | Kafka, Debezium, custom CDC |

---

## Future Projects (Ideas)

- **Nodo Data Catalog** — Metadata management and data discovery
- **Nodo Data Quality** — Data validation and quality monitoring
- **Nodo Analytics** — Analytics and reporting tools
