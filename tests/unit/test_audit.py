"""
Unit tests for AuditAdmin module.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from admin_ai_bridge.audit import AuditAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.errors import ValidationError, APIError
from admin_ai_bridge.schemas import AuditEvent


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient."""
    with patch('admin_ai_bridge.audit.get_workspace_client') as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        yield mock_client


@pytest.fixture
def audit_admin(mock_workspace_client):
    """Create an AuditAdmin instance with mocked client."""
    cfg = AdminBridgeConfig(profile="test")
    return AuditAdmin(cfg)


class TestAuditAdminInit:
    """Tests for AuditAdmin initialization."""

    def test_init_with_config(self, mock_workspace_client):
        """Test initialization with configuration."""
        cfg = AdminBridgeConfig(profile="test")
        admin = AuditAdmin(cfg)
        assert admin.ws == mock_workspace_client

    def test_init_without_config(self, mock_workspace_client):
        """Test initialization without configuration."""
        admin = AuditAdmin()
        assert admin.ws == mock_workspace_client


class TestFailedLogins:
    """Tests for failed_logins method."""

    def test_failed_logins_invalid_lookback_hours(self, audit_admin):
        """Test with invalid lookback_hours."""
        with pytest.raises(ValidationError, match="lookback_hours must be positive"):
            audit_admin.failed_logins(lookback_hours=0)

        with pytest.raises(ValidationError, match="lookback_hours must be positive"):
            audit_admin.failed_logins(lookback_hours=-1)

    def test_failed_logins_invalid_limit(self, audit_admin):
        """Test with invalid limit."""
        with pytest.raises(ValidationError, match="limit must be positive"):
            audit_admin.failed_logins(lookback_hours=24.0, limit=0)

        with pytest.raises(ValidationError, match="limit must be positive"):
            audit_admin.failed_logins(lookback_hours=24.0, limit=-1)

    def test_failed_logins_returns_empty_list(self, audit_admin, mock_workspace_client):
        """Test that the placeholder implementation returns empty list."""
        # This is a placeholder implementation until audit log querying is set up
        result = audit_admin.failed_logins(lookback_hours=24.0, limit=100)

        assert isinstance(result, list)
        assert len(result) == 0

    def test_failed_logins_with_custom_parameters(self, audit_admin, mock_workspace_client):
        """Test with custom lookback and limit parameters."""
        result = audit_admin.failed_logins(lookback_hours=48.0, limit=50)

        assert isinstance(result, list)
        # Placeholder returns empty
        assert len(result) == 0

    def test_failed_logins_log_info_when_table_missing(self, audit_admin, mock_workspace_client, caplog):
        """Test that info is logged when audit table is not available."""
        import logging
        caplog.set_level(logging.INFO)

        # Mock table check to return False (table doesn't exist)
        audit_admin._table_exists = lambda table: False

        audit_admin.failed_logins(lookback_hours=24.0)

        # Check that info message was logged about missing table
        assert any("not found" in record.message.lower() for record in caplog.records)


class TestRecentAdminChanges:
    """Tests for recent_admin_changes method."""

    def test_recent_admin_changes_invalid_lookback_hours(self, audit_admin):
        """Test with invalid lookback_hours."""
        with pytest.raises(ValidationError, match="lookback_hours must be positive"):
            audit_admin.recent_admin_changes(lookback_hours=0)

        with pytest.raises(ValidationError, match="lookback_hours must be positive"):
            audit_admin.recent_admin_changes(lookback_hours=-1)

    def test_recent_admin_changes_invalid_limit(self, audit_admin):
        """Test with invalid limit."""
        with pytest.raises(ValidationError, match="limit must be positive"):
            audit_admin.recent_admin_changes(lookback_hours=24.0, limit=0)

        with pytest.raises(ValidationError, match="limit must be positive"):
            audit_admin.recent_admin_changes(lookback_hours=24.0, limit=-1)

    def test_recent_admin_changes_returns_empty_list(self, audit_admin, mock_workspace_client):
        """Test that the placeholder implementation returns empty list."""
        # This is a placeholder implementation until audit log querying is set up
        result = audit_admin.recent_admin_changes(lookback_hours=24.0, limit=100)

        assert isinstance(result, list)
        assert len(result) == 0

    def test_recent_admin_changes_with_custom_parameters(self, audit_admin, mock_workspace_client):
        """Test with custom lookback and limit parameters."""
        result = audit_admin.recent_admin_changes(lookback_hours=168.0, limit=200)

        assert isinstance(result, list)
        # Placeholder returns empty
        assert len(result) == 0

    def test_recent_admin_changes_log_info_when_table_missing(self, audit_admin, mock_workspace_client, caplog):
        """Test that info is logged when audit table is not available."""
        import logging
        caplog.set_level(logging.INFO)

        # Mock table check to return False (table doesn't exist)
        audit_admin._table_exists = lambda table: False

        audit_admin.recent_admin_changes(lookback_hours=24.0)

        # Check that info message was logged about missing table
        assert any("not found" in record.message.lower() for record in caplog.records)

    def test_recent_admin_changes_different_time_ranges(self, audit_admin):
        """Test with various time ranges."""
        # Test 1 hour
        result = audit_admin.recent_admin_changes(lookback_hours=1.0)
        assert isinstance(result, list)

        # Test 7 days
        result = audit_admin.recent_admin_changes(lookback_hours=168.0)
        assert isinstance(result, list)

        # Test 30 days
        result = audit_admin.recent_admin_changes(lookback_hours=720.0)
        assert isinstance(result, list)


class TestAuditEventStructure:
    """Tests for AuditEvent data structure (for future implementation)."""

    def test_audit_event_creation(self):
        """Test creating an AuditEvent instance."""
        event = AuditEvent(
            event_time=datetime.now(timezone.utc),
            service_name="accounts",
            event_type="login",
            user_name="user@example.com",
            source_ip="192.168.1.1",
            details={"status": "failed", "reason": "invalid_password"}
        )

        assert event.service_name == "accounts"
        assert event.event_type == "login"
        assert event.user_name == "user@example.com"
        assert event.source_ip == "192.168.1.1"
        assert event.details["status"] == "failed"

    def test_audit_event_optional_fields(self):
        """Test AuditEvent with optional fields."""
        event = AuditEvent(
            event_time=datetime.now(timezone.utc),
            service_name="workspace",
            event_type="createCluster"
        )

        assert event.user_name is None
        assert event.source_ip is None
        assert event.details is None

    def test_audit_event_serialization(self):
        """Test that AuditEvent can be serialized."""
        event = AuditEvent(
            event_time=datetime.now(timezone.utc),
            service_name="accounts",
            event_type="login",
            user_name="user@example.com"
        )

        # Test Pydantic model_dump
        data = event.model_dump()
        assert data["service_name"] == "accounts"
        assert data["event_type"] == "login"


class TestAuditAdminLogging:
    """Tests for logging behavior."""

    def test_init_logs_message(self, mock_workspace_client, caplog):
        """Test that initialization logs a message."""
        import logging
        caplog.set_level(logging.INFO)

        audit_admin = AuditAdmin()

        assert any("AuditAdmin initialized" in record.message for record in caplog.records)

    def test_failed_logins_logs_query(self, audit_admin, caplog):
        """Test that failed_logins logs the query."""
        import logging
        caplog.set_level(logging.INFO)

        audit_admin.failed_logins(lookback_hours=24.0)

        assert any("failed logins" in record.message.lower() for record in caplog.records)

    def test_recent_admin_changes_logs_query(self, audit_admin, caplog):
        """Test that recent_admin_changes logs the query."""
        import logging
        caplog.set_level(logging.INFO)

        audit_admin.recent_admin_changes(lookback_hours=24.0)

        assert any("admin changes" in record.message.lower() for record in caplog.records)
