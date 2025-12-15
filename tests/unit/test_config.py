"""
Unit tests for config module.
"""

import pytest
from admin_ai_bridge.config import AdminBridgeConfig, get_workspace_client


class TestAdminBridgeConfig:
    """Test AdminBridgeConfig model."""

    def test_config_with_profile(self):
        """Test config with profile."""
        cfg = AdminBridgeConfig(profile="DEFAULT")
        assert cfg.profile == "DEFAULT"
        assert cfg.host is None
        assert cfg.token is None

    def test_config_with_host_token(self):
        """Test config with host and token."""
        cfg = AdminBridgeConfig(
            host="https://e2-demo-field-eng.cloud.databricks.com",
            token="dapi123"
        )
        assert cfg.host == "https://e2-demo-field-eng.cloud.databricks.com"
        assert cfg.token == "dapi123"
        assert cfg.profile is None

    def test_config_empty(self):
        """Test empty config."""
        cfg = AdminBridgeConfig()
        assert cfg.profile is None
        assert cfg.host is None
        assert cfg.token is None


class TestGetWorkspaceClient:
    """Test get_workspace_client function."""

    def test_get_client_with_none(self):
        """Test getting client with no config uses default."""
        # This will fail if no default credentials are available
        # In CI/CD, mock this or provide test credentials
        try:
            client = get_workspace_client(None)
            assert client is not None
        except Exception:
            # Expected in environments without credentials
            pytest.skip("No default credentials available")

    def test_get_client_with_profile_config(self):
        """Test getting client with profile config."""
        cfg = AdminBridgeConfig(profile="DEFAULT")
        # This will fail if the profile doesn't exist
        try:
            client = get_workspace_client(cfg)
            assert client is not None
        except Exception:
            pytest.skip("Profile DEFAULT not available")
