"""
Configuration and client management for Databricks Admin AI Bridge.
"""

from databricks.sdk import WorkspaceClient
from pydantic import BaseModel, Field


class AdminBridgeConfig(BaseModel):
    """
    Configuration for the Admin AI Bridge.

    Attributes:
        profile: Databricks CLI profile name (preferred; must point to e2-demo-field-eng.cloud.databricks.com)
        host: Databricks workspace host URL
        token: Databricks personal access token
    """
    profile: str | None = Field(default=None, description="Databricks CLI profile name from ~/.databrickscfg")
    host: str | None = Field(default=None, description="Databricks workspace host URL")
    token: str | None = Field(default=None, description="Databricks personal access token")


def get_workspace_client(cfg: AdminBridgeConfig | None = None) -> WorkspaceClient:
    """
    Resolve a WorkspaceClient from configuration.

    Priority order:
    1. Profile (preferred; must be defined in ~/.databrickscfg and point to e2-demo-field-eng.cloud.databricks.com)
    2. Host + token
    3. Environment variables / default config

    Args:
        cfg: AdminBridgeConfig instance with credentials. If None, uses default config.

    Returns:
        WorkspaceClient configured with the appropriate credentials.

    Examples:
        >>> # Using profile (preferred)
        >>> cfg = AdminBridgeConfig(profile="DEFAULT")
        >>> client = get_workspace_client(cfg)

        >>> # Using host + token
        >>> cfg = AdminBridgeConfig(
        ...     host="https://e2-demo-field-eng.cloud.databricks.com",
        ...     token="dapi..."
        ... )
        >>> client = get_workspace_client(cfg)

        >>> # Using default config
        >>> client = get_workspace_client()
    """
    if cfg and cfg.profile:
        return WorkspaceClient(profile=cfg.profile)
    if cfg and cfg.host and cfg.token:
        return WorkspaceClient(host=cfg.host, token=cfg.token)
    # Fallback: rely on default env/config
    return WorkspaceClient()
