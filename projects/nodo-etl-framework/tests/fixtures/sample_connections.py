"""Factory functions for creating test connection metadata."""

from nodo_etl.core.enums import ConnectionType, Environment, SecretProviderType
from nodo_etl.core.models import ConnectionModel


def make_connection(**overrides) -> ConnectionModel:
    """Create a ConnectionModel with sensible defaults."""
    defaults = {
        "connection_name": "finance_db_dev",
        "connection_type": ConnectionType.POSTGRESQL,
        "host": "localhost",
        "port": 5432,
        "database_name": "finance_db",
        "schema_name": "public",
        "secret_reference": "FINANCE_DB_PASSWORD",
        "secret_provider_type": SecretProviderType.ENV,
        "additional_params": {"sslmode": "require"},
        "environment": Environment.DEV,
        "is_enabled": True,
        "is_deleted": False,
    }
    defaults.update(overrides)
    return ConnectionModel(**defaults)
