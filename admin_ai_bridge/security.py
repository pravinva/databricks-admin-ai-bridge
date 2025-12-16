"""
Security and permissions management for Databricks Admin AI Bridge.

This module provides read-only access to security information including:
- Job permissions
- Cluster permissions
- Identity and access control
"""

import logging
from typing import List

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ObjectPermissions

from .config import AdminBridgeConfig, get_workspace_client
from .errors import APIError, ResourceNotFoundError, ValidationError
from .schemas import PermissionEntry

logger = logging.getLogger(__name__)


class SecurityAdmin:
    """
    Admin interface for Databricks security and permissions.

    This class provides read-only methods to query permissions on various
    Databricks objects including jobs, clusters, notebooks, and more.

    All methods are safe and read-only - no destructive operations are performed.

    Attributes:
        ws: WorkspaceClient instance for API access
    """

    def __init__(self, cfg: AdminBridgeConfig | None = None):
        """
        Initialize SecurityAdmin with optional configuration.

        Args:
            cfg: AdminBridgeConfig instance. If None, uses default credentials.

        Examples:
            >>> # Using profile
            >>> cfg = AdminBridgeConfig(profile="DEFAULT")
            >>> security_admin = SecurityAdmin(cfg)

            >>> # Using default credentials
            >>> security_admin = SecurityAdmin()
        """
        self.ws = get_workspace_client(cfg)
        logger.info("SecurityAdmin initialized")

    def who_can_manage_job(self, job_id: int) -> List[PermissionEntry]:
        """
        Return principals with CAN_MANAGE permission on the specified job.

        This method identifies all users, groups, and service principals that have
        the ability to manage (modify, delete, or change permissions on) a job.

        Args:
            job_id: Unique identifier for the job

        Returns:
            List of PermissionEntry objects representing principals with CAN_MANAGE permission.

        Raises:
            ValidationError: If job_id is invalid
            ResourceNotFoundError: If the job does not exist
            APIError: If the Databricks API returns an error

        Examples:
            >>> security_admin = SecurityAdmin()
            >>> managers = security_admin.who_can_manage_job(job_id=12345)
            >>> for entry in managers:
            ...     print(f"{entry.principal}: {entry.permission_level}")
        """
        if job_id <= 0:
            raise ValidationError("job_id must be positive")

        logger.info(f"Querying permissions for job {job_id}")

        try:
            # Get job permissions
            permissions = self.ws.permissions.get(
                request_object_type="jobs",
                request_object_id=str(job_id)
            )

            if not permissions:
                raise ResourceNotFoundError(f"Job {job_id} not found or has no permissions")

            results = []

            # Process access control list
            if permissions.access_control_list:
                for acl in permissions.access_control_list:
                    # Check for CAN_MANAGE permission
                    if acl.all_permissions:
                        for perm in acl.all_permissions:
                            if perm.permission_level:
                                # Handle permission_level field (can be object or dict)
                                perm_level_str = None
                                if hasattr(perm.permission_level, 'value'):
                                    perm_level_str = perm.permission_level.value
                                elif isinstance(perm.permission_level, dict):
                                    perm_level_str = perm.permission_level.get('value') or str(perm.permission_level)
                                else:
                                    perm_level_str = str(perm.permission_level)

                                if perm_level_str and "MANAGE" in perm_level_str:
                                    # Determine the principal name
                                    principal = None
                                    if acl.user_name:
                                        principal = acl.user_name
                                    elif acl.group_name:
                                        principal = acl.group_name
                                    elif acl.service_principal_name:
                                        principal = acl.service_principal_name

                                    if principal:
                                        entry = PermissionEntry(
                                            object_type="JOB",
                                            object_id=str(job_id),
                                            principal=principal,
                                            permission_level=perm_level_str
                                        )
                                        results.append(entry)
                                        logger.debug(f"Found permission: {principal} - {perm_level_str}")

            logger.info(f"Found {len(results)} principals with CAN_MANAGE on job {job_id}")
            return results

        except ResourceNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error querying job permissions: {e}")
            raise APIError(f"Failed to query permissions for job {job_id}: {e}")

    def who_can_use_cluster(self, cluster_id: str) -> List[PermissionEntry]:
        """
        Return principals with permission to attach/use the specified cluster.

        This method identifies all users, groups, and service principals that have
        permission to use (attach to, restart, or execute code on) a cluster.

        Args:
            cluster_id: Unique identifier for the cluster

        Returns:
            List of PermissionEntry objects representing principals with cluster permissions.
            This includes CAN_ATTACH_TO, CAN_RESTART, and CAN_MANAGE permissions.

        Raises:
            ValidationError: If cluster_id is invalid
            ResourceNotFoundError: If the cluster does not exist
            APIError: If the Databricks API returns an error

        Examples:
            >>> security_admin = SecurityAdmin()
            >>> users = security_admin.who_can_use_cluster(cluster_id="1234-567890-abc123")
            >>> for entry in users:
            ...     print(f"{entry.principal}: {entry.permission_level}")
        """
        if not cluster_id or not cluster_id.strip():
            raise ValidationError("cluster_id must be a non-empty string")

        logger.info(f"Querying permissions for cluster {cluster_id}")

        try:
            # Get cluster permissions
            permissions = self.ws.permissions.get(
                request_object_type="clusters",
                request_object_id=cluster_id
            )

            if not permissions:
                raise ResourceNotFoundError(f"Cluster {cluster_id} not found or has no permissions")

            results = []

            # Process access control list
            if permissions.access_control_list:
                for acl in permissions.access_control_list:
                    if acl.all_permissions:
                        for perm in acl.all_permissions:
                            if perm.permission_level:
                                # Handle permission_level field (can be object or dict)
                                perm_level = None
                                if hasattr(perm.permission_level, 'value'):
                                    perm_level = perm.permission_level.value
                                elif isinstance(perm.permission_level, dict):
                                    perm_level = perm.permission_level.get('value') or str(perm.permission_level)
                                else:
                                    perm_level = str(perm.permission_level)

                                # Include CAN_ATTACH_TO, CAN_RESTART, and CAN_MANAGE
                                if perm_level and any(keyword in perm_level for keyword in ["ATTACH", "RESTART", "MANAGE"]):
                                    # Determine the principal name
                                    principal = None
                                    if acl.user_name:
                                        principal = acl.user_name
                                    elif acl.group_name:
                                        principal = acl.group_name
                                    elif acl.service_principal_name:
                                        principal = acl.service_principal_name

                                    if principal:
                                        entry = PermissionEntry(
                                            object_type="CLUSTER",
                                            object_id=cluster_id,
                                            principal=principal,
                                            permission_level=perm_level
                                        )
                                        results.append(entry)
                                        logger.debug(f"Found permission: {principal} - {perm_level}")

            logger.info(f"Found {len(results)} principals with cluster permissions on {cluster_id}")
            return results

        except ResourceNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error querying cluster permissions: {e}")
            raise APIError(f"Failed to query permissions for cluster {cluster_id}: {e}")
