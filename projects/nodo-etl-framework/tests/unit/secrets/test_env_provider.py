"""Tests for the environment variable secret provider."""

import os

import pytest

from nodo_etl.secrets.base import SecretNotFoundError
from nodo_etl.secrets.env_provider import EnvSecretProvider


@pytest.fixture
def provider():
    return EnvSecretProvider()


class TestEnvSecretProvider:
    """Tests for EnvSecretProvider."""

    def test_get_secret_existing_env_var(self, provider, monkeypatch):
        """Test get_secret with existing env var returns value."""
        monkeypatch.setenv("TEST_SECRET_KEY", "my_secret_value")
        assert provider.get_secret("TEST_SECRET_KEY") == "my_secret_value"

    def test_get_secret_nonexistent_env_var(self, provider):
        """Test get_secret with non-existent env var raises SecretNotFoundError."""
        # Ensure the var doesn't exist
        os.environ.pop("NONEXISTENT_SECRET_VAR_12345", None)
        with pytest.raises(SecretNotFoundError) as exc_info:
            provider.get_secret("NONEXISTENT_SECRET_VAR_12345")
        assert "NONEXISTENT_SECRET_VAR_12345" in str(exc_info.value)
        assert exc_info.value.reference == "NONEXISTENT_SECRET_VAR_12345"

    def test_get_secret_empty_value(self, provider, monkeypatch):
        """Test get_secret with empty env var value returns empty string."""
        monkeypatch.setenv("TEST_EMPTY_SECRET", "")
        assert provider.get_secret("TEST_EMPTY_SECRET") == ""

    def test_get_secret_special_characters(self, provider, monkeypatch):
        """Test get_secret with special characters in value returns correctly."""
        special_value = "p@$$w0rd!#%^&*()"
        monkeypatch.setenv("TEST_SPECIAL_SECRET", special_value)
        assert provider.get_secret("TEST_SPECIAL_SECRET") == special_value

    def test_get_secret_case_sensitive(self, provider, monkeypatch):
        """Test get_secret reference name is case-sensitive."""
        monkeypatch.setenv("MY_SECRET", "value")
        os.environ.pop("my_secret", None)
        assert provider.get_secret("MY_SECRET") == "value"
        with pytest.raises(SecretNotFoundError):
            provider.get_secret("my_secret")

    def test_provider_type(self, provider):
        """Test provider type identifier returns 'env'."""
        assert provider.provider_type == "env"

    def test_secret_not_found_error_attributes(self):
        """Test SecretNotFoundError has correct attributes."""
        error = SecretNotFoundError("MY_VAR")
        assert error.reference == "MY_VAR"
        assert "MY_VAR" in str(error)
