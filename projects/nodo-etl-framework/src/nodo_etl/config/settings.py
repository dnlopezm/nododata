"""Environment-based configuration for the Nodo ETL Framework."""

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from nodo_etl.core.enums import DatabaseType, Environment


class Settings(BaseSettings):
    """Application settings loaded from environment variables or .env file."""

    model_config = SettingsConfigDict(
        env_prefix="NODO_ETL_",
        env_file=".env",
        env_file_encoding="utf-8",
        frozen=True,
    )

    # -- Metadata Database --
    db_type: DatabaseType = DatabaseType.POSTGRESQL
    schema_name: str = Field("nodo_etl", alias="NODO_ETL_SCHEMA")
    environment: Environment = Environment.DEV

    # -- PostgreSQL --
    pg_host: str = "localhost"
    pg_port: int = 5433
    pg_db: str = "nodo_etl_db"
    pg_user: str = "nodo_etl"
    pg_password: str = ""

    # -- SQL Server --
    sqlserver_host: str = "localhost"
    sqlserver_port: int = 1433
    sqlserver_db: str = "nodo_etl_db"
    sqlserver_user: str = "sa"
    sqlserver_password: str = ""

    # -- Secret Provider --
    secret_provider: str = "env"

    @field_validator("db_type", mode="before")
    @classmethod
    def validate_db_type(cls, v: str) -> str:
        if isinstance(v, str):
            v = v.lower()
        return v

    @field_validator("environment", mode="before")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        if isinstance(v, str):
            v = v.lower()
        return v

    @property
    def db_host(self) -> str:
        if self.db_type == DatabaseType.POSTGRESQL:
            return self.pg_host
        return self.sqlserver_host

    @property
    def db_port(self) -> int:
        if self.db_type == DatabaseType.POSTGRESQL:
            return self.pg_port
        return self.sqlserver_port

    @property
    def db_name(self) -> str:
        if self.db_type == DatabaseType.POSTGRESQL:
            return self.pg_db
        return self.sqlserver_db

    @property
    def db_user(self) -> str:
        if self.db_type == DatabaseType.POSTGRESQL:
            return self.pg_user
        return self.sqlserver_user

    @property
    def db_password(self) -> str:
        if self.db_type == DatabaseType.POSTGRESQL:
            return self.pg_password
        return self.sqlserver_password

    @property
    def connection_string(self) -> str:
        if self.db_type == DatabaseType.POSTGRESQL:
            return (
                f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}"
                f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
            )
        return (
            f"mssql+pyodbc://{self.sqlserver_user}:{self.sqlserver_password}"
            f"@{self.sqlserver_host}:{self.sqlserver_port}/{self.sqlserver_db}"
            f"?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )

    def __repr__(self) -> str:
        return (
            f"Settings(db_type={self.db_type.value}, "
            f"env={self.environment.value}, "
            f"host={self.db_host}:{self.db_port}, "
            f"db={self.db_name})"
        )

    def __str__(self) -> str:
        return self.__repr__()


def load_settings(env_file: str | Path | None = None) -> Settings:
    """Load settings from environment variables and optional .env file."""
    if env_file:
        return Settings(_env_file=str(env_file))
    return Settings()
