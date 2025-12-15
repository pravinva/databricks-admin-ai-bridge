"""
Unit tests for SecurityAdmin module.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from admin_ai_bridge.security import SecurityAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.errors import ValidationError, ResourceNotFoundError, APIError
from admin_ai_bridge.schemas import PermissionEntry

# Note: We mock the SDK responses, so we don't need to import specific types


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient."""
    with patch('admin_ai_bridge.security.get_workspace_client') as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        yield mock_client


@pytest.fixture
def security_admin(mock_workspace_client):
    """Create a SecurityAdmin instance with mocked client."""
    cfg = AdminBridgeConfig(profile="test")
    return SecurityAdmin(cfg)


class TestSecurityAdminInit:
    """Tests for SecurityAdmin initialization."""

    def test_init_with_config(self, mock_workspace_client):
        """Test initialization with configuration."""
        cfg = AdminBridgeConfig(profile="test")
        admin = SecurityAdmin(cfg)
        assert admin.ws == mock_workspace_client

    def test_init_without_config(self, mock_workspace_client):
        """Test initialization without configuration."""
        admin = SecurityAdmin()
        assert admin.ws == mock_workspace_client


class TestWhoCanManageJob:
    """Tests for who_can_manage_job method."""

    def test_who_can_manage_job_success(self, security_admin, mock_workspace_client):
        """Test successful query of job permissions."""
        # Mock permissions response
        mock_acl_entry = MagicMock()
        mock_acl_entry.user_name = "user@example.com"
        mock_acl_entry.group_name = None
        mock_acl_entry.service_principal_name = None

        mock_permission = MagicMock()
        mock_permission.permission_level = MagicMock()
        mock_permission.permission_level.value = "CAN_MANAGE"

        mock_acl_entry.all_permissions = [mock_permission]

        mock_permissions = MagicMock()
        mock_permissions.access_control_list = [mock_acl_entry]

        mock_workspace_client.permissions.get.return_value = mock_permissions

        # Call method
        result = security_admin.who_can_manage_job(job_id=123)

        # Verify
        assert len(result) == 1
        assert isinstance(result[0], PermissionEntry)
        assert result[0].object_type == "JOB"
        assert result[0].object_id == "123"
        assert result[0].principal == "user@example.com"
        assert result[0].permission_level == "CAN_MANAGE"

        mock_workspace_client.permissions.get.assert_called_once_with(
            request_object_type="jobs",
            request_object_id="123"
        )

    def test_who_can_manage_job_multiple_principals(self, security_admin, mock_workspace_client):
        """Test with multiple principals having CAN_MANAGE."""
        # Mock multiple ACL entries
        mock_acl_user = MagicMock()
        mock_acl_user.user_name = "user@example.com"
        mock_acl_user.group_name = None
        mock_acl_user.service_principal_name = None

        mock_acl_group = MagicMock()
        mock_acl_group.user_name = None
        mock_acl_group.group_name = "admins"
        mock_acl_group.service_principal_name = None

        mock_acl_sp = MagicMock()
        mock_acl_sp.user_name = None
        mock_acl_sp.group_name = None
        mock_acl_sp.service_principal_name = "app-sp"

        mock_permission = MagicMock()
        mock_permission.permission_level = MagicMock()
        mock_permission.permission_level.value = "CAN_MANAGE"

        mock_acl_user.all_permissions = [mock_permission]
        mock_acl_group.all_permissions = [mock_permission]
        mock_acl_sp.all_permissions = [mock_permission]

        mock_permissions = MagicMock()
        mock_permissions.access_control_list = [mock_acl_user, mock_acl_group, mock_acl_sp]

        mock_workspace_client.permissions.get.return_value = mock_permissions

        # Call method
        result = security_admin.who_can_manage_job(job_id=456)

        # Verify
        assert len(result) == 3
        principals = [entry.principal for entry in result]
        assert "user@example.com" in principals
        assert "admins" in principals
        assert "app-sp" in principals

    def test_who_can_manage_job_invalid_id(self, security_admin):
        """Test with invalid job ID."""
        with pytest.raises(ValidationError, match="job_id must be positive"):
            security_admin.who_can_manage_job(job_id=0)

        with pytest.raises(ValidationError, match="job_id must be positive"):
            security_admin.who_can_manage_job(job_id=-1)

    def test_who_can_manage_job_not_found(self, security_admin, mock_workspace_client):
        """Test when job is not found."""
        mock_workspace_client.permissions.get.return_value = None

        with pytest.raises(ResourceNotFoundError, match="Job 999 not found"):
            security_admin.who_can_manage_job(job_id=999)

    def test_who_can_manage_job_api_error(self, security_admin, mock_workspace_client):
        """Test API error handling."""
        mock_workspace_client.permissions.get.side_effect = Exception("API error")

        with pytest.raises(APIError, match="Failed to query permissions for job 123"):
            security_admin.who_can_manage_job(job_id=123)

    def test_who_can_manage_job_filters_non_manage(self, security_admin, mock_workspace_client):
        """Test that only CAN_MANAGE permissions are returned."""
        # Mock ACL with various permission levels
        mock_acl_manage = MagicMock()
        mock_acl_manage.user_name = "manager@example.com"
        mock_acl_manage.group_name = None
        mock_acl_manage.service_principal_name = None

        mock_perm_manage = MagicMock()
        mock_perm_manage.permission_level = MagicMock()
        mock_perm_manage.permission_level.value = "CAN_MANAGE"

        mock_acl_manage.all_permissions = [mock_perm_manage]

        mock_acl_view = MagicMock()
        mock_acl_view.user_name = "viewer@example.com"
        mock_acl_view.group_name = None
        mock_acl_view.service_principal_name = None

        mock_perm_view = MagicMock()
        mock_perm_view.permission_level = MagicMock()
        mock_perm_view.permission_level.value = "CAN_VIEW"

        mock_acl_view.all_permissions = [mock_perm_view]

        mock_permissions = MagicMock()
        mock_permissions.access_control_list = [mock_acl_manage, mock_acl_view]

        mock_workspace_client.permissions.get.return_value = mock_permissions

        # Call method
        result = security_admin.who_can_manage_job(job_id=789)

        # Verify only CAN_MANAGE is returned
        assert len(result) == 1
        assert result[0].principal == "manager@example.com"


class TestWhoCanUseCluster:
    """Tests for who_can_use_cluster method."""

    def test_who_can_use_cluster_success(self, security_admin, mock_workspace_client):
        """Test successful query of cluster permissions."""
        # Mock permissions response
        mock_acl_entry = MagicMock()
        mock_acl_entry.user_name = "user@example.com"
        mock_acl_entry.group_name = None
        mock_acl_entry.service_principal_name = None

        mock_permission = MagicMock()
        mock_permission.permission_level = MagicMock()
        mock_permission.permission_level.value = "CAN_ATTACH_TO"

        mock_acl_entry.all_permissions = [mock_permission]

        mock_permissions = MagicMock()
        mock_permissions.access_control_list = [mock_acl_entry]

        mock_workspace_client.permissions.get.return_value = mock_permissions

        # Call method
        result = security_admin.who_can_use_cluster(cluster_id="1234-567890-abc123")

        # Verify
        assert len(result) == 1
        assert isinstance(result[0], PermissionEntry)
        assert result[0].object_type == "CLUSTER"
        assert result[0].object_id == "1234-567890-abc123"
        assert result[0].principal == "user@example.com"
        assert result[0].permission_level == "CAN_ATTACH_TO"

        mock_workspace_client.permissions.get.assert_called_once_with(
            request_object_type="clusters",
            request_object_id="1234-567890-abc123"
        )

    def test_who_can_use_cluster_multiple_permissions(self, security_admin, mock_workspace_client):
        """Test with multiple permission levels (ATTACH, RESTART, MANAGE)."""
        # Mock ACL entries with different permission levels
        mock_acl_attach = MagicMock()
        mock_acl_attach.user_name = "user1@example.com"
        mock_acl_attach.group_name = None
        mock_acl_attach.service_principal_name = None

        mock_perm_attach = MagicMock()
        mock_perm_attach.permission_level = MagicMock()
        mock_perm_attach.permission_level.value = "CAN_ATTACH_TO"

        mock_acl_attach.all_permissions = [mock_perm_attach]

        mock_acl_restart = MagicMock()
        mock_acl_restart.user_name = "user2@example.com"
        mock_acl_restart.group_name = None
        mock_acl_restart.service_principal_name = None

        mock_perm_restart = MagicMock()
        mock_perm_restart.permission_level = MagicMock()
        mock_perm_restart.permission_level.value = "CAN_RESTART"

        mock_acl_restart.all_permissions = [mock_perm_restart]

        mock_acl_manage = MagicMock()
        mock_acl_manage.user_name = "admin@example.com"
        mock_acl_manage.group_name = None
        mock_acl_manage.service_principal_name = None

        mock_perm_manage = MagicMock()
        mock_perm_manage.permission_level = MagicMock()
        mock_perm_manage.permission_level.value = "CAN_MANAGE"

        mock_acl_manage.all_permissions = [mock_perm_manage]

        mock_permissions = MagicMock()
        mock_permissions.access_control_list = [mock_acl_attach, mock_acl_restart, mock_acl_manage]

        mock_workspace_client.permissions.get.return_value = mock_permissions

        # Call method
        result = security_admin.who_can_use_cluster(cluster_id="test-cluster")

        # Verify
        assert len(result) == 3
        principals = [entry.principal for entry in result]
        assert "user1@example.com" in principals
        assert "user2@example.com" in principals
        assert "admin@example.com" in principals

    def test_who_can_use_cluster_invalid_id(self, security_admin):
        """Test with invalid cluster ID."""
        with pytest.raises(ValidationError, match="cluster_id must be a non-empty string"):
            security_admin.who_can_use_cluster(cluster_id="")

        with pytest.raises(ValidationError, match="cluster_id must be a non-empty string"):
            security_admin.who_can_use_cluster(cluster_id="   ")

    def test_who_can_use_cluster_not_found(self, security_admin, mock_workspace_client):
        """Test when cluster is not found."""
        mock_workspace_client.permissions.get.return_value = None

        with pytest.raises(ResourceNotFoundError, match="Cluster invalid-cluster not found"):
            security_admin.who_can_use_cluster(cluster_id="invalid-cluster")

    def test_who_can_use_cluster_api_error(self, security_admin, mock_workspace_client):
        """Test API error handling."""
        mock_workspace_client.permissions.get.side_effect = Exception("API error")

        with pytest.raises(APIError, match="Failed to query permissions for cluster test-cluster"):
            security_admin.who_can_use_cluster(cluster_id="test-cluster")

    def test_who_can_use_cluster_filters_unrelated_permissions(self, security_admin, mock_workspace_client):
        """Test that only relevant permissions are returned."""
        # Mock ACL with various permission levels
        mock_acl_relevant = MagicMock()
        mock_acl_relevant.user_name = "user@example.com"
        mock_acl_relevant.group_name = None
        mock_acl_relevant.service_principal_name = None

        mock_perm_relevant = MagicMock()
        mock_perm_relevant.permission_level = MagicMock()
        mock_perm_relevant.permission_level.value = "CAN_ATTACH_TO"

        mock_acl_relevant.all_permissions = [mock_perm_relevant]

        mock_acl_view = MagicMock()
        mock_acl_view.user_name = "viewer@example.com"
        mock_acl_view.group_name = None
        mock_acl_view.service_principal_name = None

        mock_perm_view = MagicMock()
        mock_perm_view.permission_level = MagicMock()
        mock_perm_view.permission_level.value = "CAN_VIEW"

        mock_acl_view.all_permissions = [mock_perm_view]

        mock_permissions = MagicMock()
        mock_permissions.access_control_list = [mock_acl_relevant, mock_acl_view]

        mock_workspace_client.permissions.get.return_value = mock_permissions

        # Call method
        result = security_admin.who_can_use_cluster(cluster_id="test-cluster")

        # Verify only relevant permission is returned
        assert len(result) == 1
        assert result[0].principal == "user@example.com"
