"""Tests for the secret provider factory."""

import pytest

from nodo_etl.secrets.env_provider import EnvSecretProvider
from nodo_etl.secrets.factory import clear_cache, get_provider


@pytest.fixture(autouse=True)
def clean_cache():
    """Clear the provider cache before each test."""
    clear_cache()
    yield
    clear_cache()


class TestSecretFactory:
    """Tests for the secret provider factory."""

    def test_get_provider_env(self):
        """Test get_provider('env') returns EnvSecretProvider instance."""
        provider = get_provider("env")
        assert isinstance(provider, EnvSecretProvider)

    def test_get_provider_keyvault_not_implemented(self):
        """Test get_provider('keyvault') raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="keyvault"):
            get_provider("keyvault")

    def test_get_provider_aws_sm_not_implemented(self):
        """Test get_provider('aws_sm') raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="aws_sm"):
            get_provider("aws_sm")

    def test_get_provider_airflow_not_implemented(self):
        """Test get_provider('airflow') raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="airflow"):
            get_provider("airflow")

    def test_get_provider_invalid(self):
        """Test get_provider('invalid') raises ValueError."""
        with pytest.raises(ValueError, match="Unknown secret provider type"):
            get_provider("invalid")

    def test_get_provider_none_defaults_to_env(self):
        """Test get_provider(None) uses default env provider."""
        provider = get_provider(None)
        assert isinstance(provider, EnvSecretProvider)

    def test_factory_caches_providers(self):
        """Test factory caches providers (returns same instance for same type)."""
        provider1 = get_provider("env")
        provider2 = get_provider("env")
        assert provider1 is provider2

    def test_cache_cleared(self):
        """Test clear_cache resets the cache."""
        provider1 = get_provider("env")
        clear_cache()
        provider2 = get_provider("env")
        assert provider1 is not provider2
