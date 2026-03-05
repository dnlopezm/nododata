"""Factory for creating secret providers."""

from nodo_etl.secrets.base import SecretProvider
from nodo_etl.secrets.env_provider import EnvSecretProvider

_PROVIDER_CACHE: dict[str, SecretProvider] = {}

_NOT_IMPLEMENTED = {"keyvault", "aws_sm", "airflow"}


def get_provider(provider_type: str | None = None) -> SecretProvider:
    """Get a secret provider by type.

    Args:
        provider_type: The provider type. If None, defaults to "env".

    Returns:
        A SecretProvider instance (cached).

    Raises:
        NotImplementedError: For provider types not yet implemented.
        ValueError: For unknown provider types.
    """
    if provider_type is None:
        provider_type = "env"

    if provider_type in _PROVIDER_CACHE:
        return _PROVIDER_CACHE[provider_type]

    if provider_type == "env":
        provider = EnvSecretProvider()
    elif provider_type in _NOT_IMPLEMENTED:
        raise NotImplementedError(
            f"Secret provider '{provider_type}' is not yet implemented"
        )
    else:
        raise ValueError(f"Unknown secret provider type: '{provider_type}'")

    _PROVIDER_CACHE[provider_type] = provider
    return provider


def clear_cache() -> None:
    """Clear the provider cache. Useful for testing."""
    _PROVIDER_CACHE.clear()
