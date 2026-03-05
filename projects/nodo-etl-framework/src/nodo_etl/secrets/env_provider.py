"""Environment variable-based secret provider."""

import os

from nodo_etl.secrets.base import SecretNotFoundError, SecretProvider


class EnvSecretProvider(SecretProvider):
    """Reads secrets from environment variables."""

    @property
    def provider_type(self) -> str:
        return "env"

    def get_secret(self, reference: str) -> str:
        """Get a secret from an environment variable.

        Args:
            reference: The environment variable name.

        Returns:
            The environment variable value.

        Raises:
            SecretNotFoundError: If the environment variable is not set.
        """
        value = os.environ.get(reference)
        if value is None:
            raise SecretNotFoundError(reference)
        return value
