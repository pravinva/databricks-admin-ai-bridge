"""
Custom exception classes for Databricks Admin AI Bridge.
"""


class AdminBridgeError(Exception):
    """Base exception for all Admin AI Bridge errors."""
    pass


class ConfigurationError(AdminBridgeError):
    """Raised when there is a configuration issue."""
    pass


class AuthenticationError(AdminBridgeError):
    """Raised when authentication fails."""
    pass


class AuthorizationError(AdminBridgeError):
    """Raised when the user lacks permissions for an operation."""
    pass


class ResourceNotFoundError(AdminBridgeError):
    """Raised when a requested resource is not found."""
    pass


class ValidationError(AdminBridgeError):
    """Raised when input validation fails."""
    pass


class APIError(AdminBridgeError):
    """Raised when the Databricks API returns an error."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class RateLimitError(AdminBridgeError):
    """Raised when rate limits are exceeded."""
    pass


class TimeoutError(AdminBridgeError):
    """Raised when an operation times out."""
    pass
