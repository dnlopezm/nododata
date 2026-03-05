FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies needed by DAGs
RUN pip install --no-cache-dir \
    sqlalchemy>=2.0 \
    psycopg2-binary>=2.9 \
    pyodbc>=5.0 \
    pydantic>=2.5 \
    pydantic-settings>=2.1 \
    python-dotenv>=1.0 \
    croniter>=1.4
