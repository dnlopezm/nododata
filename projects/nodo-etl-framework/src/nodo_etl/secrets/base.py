"""Abstract base class for secret providers."""

from abc import ABC, abstractmethod


class SecretNotFoundError(Exception):
    """Raised when a secret cannot be found."""

    def __init__(self, reference: str) -> None:
        self.reference = reference
        super().__init__(f"Secret not found: {reference}")


class SecretProvider(ABC):
    """Abstract secret provider interface."""

    @property
    @abstractmethod
    def provider_type(self) -> str:
        """Return the provider type identifier."""

    @abstractmethod
    def get_secret(self, reference: str) -> str:
        """Retrieve a secret by its reference.

        Args:
            reference: The secret reference (e.g., env var name, vault path).

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If the secret cannot be found.
        """
