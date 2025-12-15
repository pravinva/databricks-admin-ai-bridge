"""
Audit log monitoring for Databricks Admin AI Bridge.

This module provides read-only access to audit log information including:
- Failed login attempts
- Admin permission changes
- Security-relevant events
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List

from databricks.sdk import WorkspaceClient

from .config import AdminBridgeConfig, get_workspace_client
from .errors import APIError, ValidationError
from .schemas import AuditEvent

logger = logging.getLogger(__name__)


class AuditAdmin:
    """
    Admin interface for Databricks audit logs.

    This class provides read-only methods to query audit logs for security and
    compliance monitoring, including failed logins and administrative changes.

    All methods are safe and read-only - no destructive operations are performed.

    Note:
        This implementation provides a basic framework. In production environments,
        you would typically query system tables (system.access.audit) or integrate
        with log forwarding/SIEM systems for comprehensive audit log analysis.

    Attributes:
        ws: WorkspaceClient instance for API access
    """

    def __init__(self, cfg: AdminBridgeConfig | None = None):
        """
        Initialize AuditAdmin with optional configuration.

        Args:
            cfg: AdminBridgeConfig instance. If None, uses default credentials.

        Examples:
            >>> # Using profile
            >>> cfg = AdminBridgeConfig(profile="DEFAULT")
            >>> audit_admin = AuditAdmin(cfg)

            >>> # Using default credentials
            >>> audit_admin = AuditAdmin()
        """
        self.ws = get_workspace_client(cfg)
        logger.info("AuditAdmin initialized")

    def failed_logins(
        self,
        lookback_hours: float = 24.0,
        limit: int = 100,
    ) -> List[AuditEvent]:
        """
        Return failed login attempts from the audit logs.

        This method identifies unsuccessful authentication attempts, which can help
        detect potential security threats, brute force attacks, or user access issues.

        Note:
            This is a basic implementation. In production, you should query
            system.access.audit tables for complete audit log data. This example
            demonstrates the structure but may return limited data without proper
            audit log configuration.

        Args:
            lookback_hours: How far back to search for failed logins. Must be positive.
                Default: 24.0 hours.
            limit: Maximum number of results to return. Must be positive.
                Default: 100.

        Returns:
            List of AuditEvent objects for failed login attempts, sorted by time (newest first).

        Raises:
            ValidationError: If parameters are invalid (negative values, etc.)
            APIError: If the Databricks API returns an error

        Examples:
            >>> audit_admin = AuditAdmin()
            >>> # Find failed logins in the last 48 hours
            >>> failed = audit_admin.failed_logins(lookback_hours=48.0, limit=50)
            >>> for event in failed:
            ...     print(f"{event.event_time}: {event.user_name} from {event.source_ip}")
        """
        # Validate parameters
        if lookback_hours <= 0:
            raise ValidationError("lookback_hours must be positive")
        if limit <= 0:
            raise ValidationError("limit must be positive")

        logger.info(f"Querying failed logins for last {lookback_hours} hours")

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)

        # Note: This is a placeholder implementation
        # In production, you would query system.access.audit like:
        #
        # SELECT event_time, service_name, action_name, user_identity.email,
        #        request_params, source_ip_address
        # FROM system.access.audit
        # WHERE event_time >= '{start_time}'
        #   AND action_name = 'login'
        #   AND response.status_code = 401
        # ORDER BY event_time DESC
        # LIMIT {limit}

        logger.warning(
            "Audit log querying requires system.access.audit table access. "
            "This implementation returns a placeholder. Configure audit log delivery "
            "and query system.access.audit for production use."
        )

        # Return empty list as placeholder
        # In production, populate this from actual audit logs
        audit_events = []

        logger.info(f"Found {len(audit_events)} failed login events (placeholder implementation)")
        return audit_events[:limit]

    def recent_admin_changes(
        self,
        lookback_hours: float = 24.0,
        limit: int = 100,
    ) -> List[AuditEvent]:
        """
        Return recent administrative and permission change events from audit logs.

        This method identifies sensitive operations such as:
        - Admin group membership changes
        - Permission grants/revokes
        - Service principal creation/deletion
        - Workspace configuration changes

        Note:
            This is a basic implementation. In production, you should query
            system.access.audit tables for complete audit log data.

        Args:
            lookback_hours: How far back to search for admin changes. Must be positive.
                Default: 24.0 hours.
            limit: Maximum number of results to return. Must be positive.
                Default: 100.

        Returns:
            List of AuditEvent objects for administrative changes, sorted by time (newest first).

        Raises:
            ValidationError: If parameters are invalid (negative values, etc.)
            APIError: If the Databricks API returns an error

        Examples:
            >>> audit_admin = AuditAdmin()
            >>> # Find admin changes in the last 7 days
            >>> changes = audit_admin.recent_admin_changes(lookback_hours=168.0)
            >>> for event in changes:
            ...     print(f"{event.event_time}: {event.event_type} by {event.user_name}")
        """
        # Validate parameters
        if lookback_hours <= 0:
            raise ValidationError("lookback_hours must be positive")
        if limit <= 0:
            raise ValidationError("limit must be positive")

        logger.info(f"Querying admin changes for last {lookback_hours} hours")

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)

        # Note: This is a placeholder implementation
        # In production, you would query system.access.audit like:
        #
        # SELECT event_time, service_name, action_name, user_identity.email,
        #        request_params, response, source_ip_address
        # FROM system.access.audit
        # WHERE event_time >= '{start_time}'
        #   AND (
        #     action_name IN ('addPrincipalToGroup', 'removePrincipalFromGroup',
        #                     'createServicePrincipal', 'deleteServicePrincipal',
        #                     'changePermissions', 'updatePermissions')
        #     OR service_name = 'accounts'
        #     OR request_params.group_name LIKE '%admin%'
        #   )
        # ORDER BY event_time DESC
        # LIMIT {limit}

        logger.warning(
            "Audit log querying requires system.access.audit table access. "
            "This implementation returns a placeholder. Configure audit log delivery "
            "and query system.access.audit for production use."
        )

        # Return empty list as placeholder
        # In production, populate this from actual audit logs
        audit_events = []

        # Example of how you might construct events from real audit data:
        # for row in query_results:
        #     event = AuditEvent(
        #         event_time=row['event_time'],
        #         service_name=row['service_name'],
        #         event_type=row['action_name'],
        #         user_name=row.get('user_identity', {}).get('email'),
        #         source_ip=row.get('source_ip_address'),
        #         details={
        #             'request_params': row.get('request_params'),
        #             'response': row.get('response')
        #         }
        #     )
        #     audit_events.append(event)

        logger.info(f"Found {len(audit_events)} admin change events (placeholder implementation)")
        return audit_events[:limit]
