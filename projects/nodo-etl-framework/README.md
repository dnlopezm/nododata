# Nodo ETL Framework

Metadata-driven ETL framework for orchestrating data pipelines across bronze, silver, and gold layers.

## Features

- **Metadata-driven**: All pipeline configuration stored in a metadata database
- **Multi-database**: Supports SQL Server and PostgreSQL as metadata backends
- **Medallion architecture**: Bronze → Silver → Gold data layers
- **Multiple sources**: Database, file (CSV/Parquet/Delta/JSON), API, and streaming (Kafka/CDC)
- **Orchestration**: Airflow integration with schedule-based and manual execution
- **Execution tracking**: Full lifecycle tracking with retry support
- **Environment support**: Dev, staging, and production configurations
- **CLI**: Command-line interface for metadata management

## Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose

### Setup

```bash
# Clone and navigate
cd projects/nodo-etl-framework

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Start Docker services (SQL Server, PostgreSQL, Airflow)
cd docker && docker-compose up -d

# Configure environment
cp .env.example .env
# Edit .env with your settings

# Run tests
pytest
```

### CLI Usage

```bash
# Create a connection
nodo-etl connection create --name finance_db --type postgresql --host localhost --port 5432

# Create a process
nodo-etl process create --name finance_etl --description "Finance data pipeline"

# Create jobs and datasets
nodo-etl job create --process-id 1 --name bronze_ingestion --order 1
nodo-etl dataset create --job-id 1 --name transactions_bronze --source-type database

# Run a process
nodo-etl run process 1

# Check status
nodo-etl status 1
```

## Architecture

```
Process → Job → Dataset → Source Config
    │         │        │
    ▼         ▼        ▼
Process    Job     Dataset
Execution  Execution  Execution
```

See [BACKLOG_ETL.md](../../docs/requirements/BACKLOG_ETL.md) for full documentation.

## Project Structure

```
src/nodo_etl/
├── cli/        # CLI commands (Click)
├── core/       # Enums, Pydantic models
├── db/         # Database connection, repositories
├── secrets/    # Secret provider abstraction
└── config/     # Environment-based configuration
```

## License

MIT
