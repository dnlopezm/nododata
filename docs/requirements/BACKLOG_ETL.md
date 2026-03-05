# Nodo ETL Framework - Backlog

Detailed backlog for the Nodo ETL Framework project.

**Last Updated:** March 4, 2026

---

## Phase E1: Foundation & Core Architecture
**Status:** Not Started

### Tasks:
- [ ] Define project structure (src, tests, configs)
- [ ] Set up Python project with pyproject.toml
- [ ] Define core abstractions (Extractor, Transformer, Loader interfaces)
- [ ] Create Pipeline class (orchestrates E -> T -> L)
- [ ] Create DataRecord / DataBatch base models
- [ ] Set up logging framework
- [ ] Set up testing framework (pytest)
- [ ] Write initial unit tests for core classes

---

## Phase E2: Extractors
**Status:** Not Started

### Tasks:
- [ ] CSV Extractor - Read from CSV files
- [ ] JSON Extractor - Read from JSON files
- [ ] Database Extractor - Read from SQL databases (SQLAlchemy)
- [ ] API Extractor - Read from REST APIs
- [ ] Extractor factory / registry pattern
- [ ] Unit tests for each extractor

---

## Phase E3: Transformers
**Status:** Not Started

### Tasks:
- [ ] Filter transformer - Filter rows by condition
- [ ] Map transformer - Transform/rename fields
- [ ] Aggregate transformer - Group by and aggregate
- [ ] Join transformer - Join multiple data sources
- [ ] Custom transformer - User-defined transformation functions
- [ ] Transformer chaining (compose multiple transforms)
- [ ] Unit tests for each transformer

---

## Phase E4: Loaders
**Status:** Not Started

### Tasks:
- [ ] CSV Loader - Write to CSV files
- [ ] JSON Loader - Write to JSON files
- [ ] Database Loader - Write to SQL databases (upsert support)
- [ ] Console Loader - Print to stdout (debugging)
- [ ] Loader factory / registry pattern
- [ ] Unit tests for each loader

---

## Phase E5: Pipeline Orchestration
**Status:** Not Started

### Tasks:
- [ ] Pipeline configuration via YAML/TOML
- [ ] Pipeline validation (check connections, schemas)
- [ ] Sequential pipeline execution
- [ ] Parallel pipeline execution (optional)
- [ ] Pipeline error handling and retry logic
- [ ] Pipeline state management (checkpoints)
- [ ] Unit and integration tests

---

## Phase E6: Monitoring & Logging
**Status:** Not Started

### Tasks:
- [ ] Structured logging (JSON format)
- [ ] Pipeline execution metrics (rows processed, duration, errors)
- [ ] Pipeline execution history
- [ ] Alerting hooks (email, webhook)
- [ ] Dashboard data export
- [ ] Tests for monitoring components

---

## Phase E7: CLI & Configuration
**Status:** Not Started

### Tasks:
- [ ] CLI tool for running pipelines (`nodo-etl run <pipeline>`)
- [ ] CLI for listing available pipelines
- [ ] CLI for validating pipeline configs
- [ ] Environment-based configuration (.env support)
- [ ] Configuration schema validation
- [ ] CLI tests

---

## Phase E8: Testing & Documentation
**Status:** Not Started

### Tasks:
- [ ] Integration tests with real data sources
- [ ] Performance benchmarks
- [ ] API documentation (auto-generated)
- [ ] User guide / getting started
- [ ] Example pipelines
- [ ] CI/CD setup (GitHub Actions)
