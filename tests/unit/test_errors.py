"""
Unit tests for errors module.
"""

import pytest
from admin_ai_bridge.errors import (
    AdminBridgeError,
    ConfigurationError,
    AuthenticationError,
    AuthorizationError,
    ResourceNotFoundError,
    ValidationError,
    APIError,
    RateLimitError,
    TimeoutError,
)


class TestCustomExceptions:
    """Test custom exception classes."""

    def test_admin_bridge_error(self):
        """Test base AdminBridgeError."""
        with pytest.raises(AdminBridgeError):
            raise AdminBridgeError("Test error")

    def test_configuration_error(self):
        """Test ConfigurationError."""
        with pytest.raises(ConfigurationError):
            raise ConfigurationError("Config error")

    def test_authentication_error(self):
        """Test AuthenticationError."""
        with pytest.raises(AuthenticationError):
            raise AuthenticationError("Auth error")

    def test_authorization_error(self):
        """Test AuthorizationError."""
        with pytest.raises(AuthorizationError):
            raise AuthorizationError("Authz error")

    def test_resource_not_found_error(self):
        """Test ResourceNotFoundError."""
        with pytest.raises(ResourceNotFoundError):
            raise ResourceNotFoundError("Not found")

    def test_validation_error(self):
        """Test ValidationError."""
        with pytest.raises(ValidationError):
            raise ValidationError("Validation failed")

    def test_api_error(self):
        """Test APIError."""
        error = APIError("API failed", status_code=500)
        assert error.status_code == 500
        assert str(error) == "API failed"

    def test_api_error_no_status(self):
        """Test APIError without status code."""
        error = APIError("API failed")
        assert error.status_code is None

    def test_rate_limit_error(self):
        """Test RateLimitError."""
        with pytest.raises(RateLimitError):
            raise RateLimitError("Rate limit exceeded")

    def test_timeout_error(self):
        """Test TimeoutError."""
        with pytest.raises(TimeoutError):
            raise TimeoutError("Operation timed out")

    def test_inheritance(self):
        """Test that all errors inherit from AdminBridgeError."""
        assert issubclass(ConfigurationError, AdminBridgeError)
        assert issubclass(AuthenticationError, AdminBridgeError)
        assert issubclass(AuthorizationError, AdminBridgeError)
        assert issubclass(ResourceNotFoundError, AdminBridgeError)
        assert issubclass(ValidationError, AdminBridgeError)
        assert issubclass(APIError, AdminBridgeError)
        assert issubclass(RateLimitError, AdminBridgeError)
        assert issubclass(TimeoutError, AdminBridgeError)
