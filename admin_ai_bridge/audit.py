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
        self._audit_table = "system.access.audit"
        logger.info("AuditAdmin initialized")

    def _table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the workspace.

        Args:
            table_name: Fully qualified table name (e.g., "system.access.audit")

        Returns:
            True if table exists, False otherwise
        """
        try:
            # Try to query the table with LIMIT 0 to check existence
            parts = table_name.split(".")
            if len(parts) == 3:
                catalog, schema, table = parts
                # Use workspace client to check table existence
                tables = self.ws.tables.list(catalog_name=catalog, schema_name=schema)
                return any(t.name == table for t in tables)
            return False
        except Exception as e:
            logger.debug(f"Table {table_name} does not exist or is not accessible: {e}")
            return False

    def _get_default_warehouse_id(self) -> str | None:
        """
        Get the default SQL warehouse ID for executing queries.

        Returns:
            The ID of the first available SQL warehouse, or None if none available.
        """
        try:
            warehouses = list(self.ws.warehouses.list())
            if warehouses:
                return warehouses[0].id
            logger.warning("No SQL warehouses available for audit queries")
            return None
        except Exception as e:
            logger.warning(f"Error getting default warehouse: {e}")
            return None

    def failed_logins(
        self,
        lookback_hours: float = 24.0,
        limit: int = 100,
    ) -> List[AuditEvent]:
        """
        Return failed login attempts from the audit logs.

        This method identifies unsuccessful authentication attempts, which can help
        detect potential security threats, brute force attacks, or user access issues.

        Prerequisites:
            - system.access.audit table must be available (Unity Catalog audit logs enabled)
            - A SQL warehouse must be available for query execution

        Args:
            lookback_hours: How far back to search for failed logins. Must be positive.
                Default: 24.0 hours.
            limit: Maximum number of results to return. Must be positive.
                Default: 100.

        Returns:
            List of AuditEvent objects for failed login attempts, sorted by time (newest first).
            Returns empty list if system.access.audit table is not available.

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

        # Check if audit table exists
        if not self._table_exists(self._audit_table):
            logger.info(
                f"Table {self._audit_table} not found. Please enable Unity Catalog audit logs. "
                "Returning empty results."
            )
            return []

        # Get warehouse for query execution
        warehouse_id = self._get_default_warehouse_id()
        if not warehouse_id:
            logger.info("No SQL warehouse available for audit queries. Returning empty results.")
            return []

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

        # Build SQL query for failed logins
        sql = f"""
        SELECT
            event_time,
            service_name,
            action_name,
            user_identity.email as user_name,
            source_ip_address,
            request_params,
            response
        FROM {self._audit_table}
        WHERE event_time >= TIMESTAMP '{start_time_str}'
          AND action_name = 'login'
          AND response.status_code = 401
        ORDER BY event_time DESC
        LIMIT {limit}
        """

        try:
            logger.debug(f"Executing SQL query: {sql}")

            # Execute SQL query
            statement = self.ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout="50s"  # Maximum allowed by Databricks API
            )

            # Parse results into AuditEvent objects
            audit_events = []
            if statement.result and statement.result.data_array:
                for row in statement.result.data_array:
                    # row format: [event_time, service_name, action_name, user_name, source_ip, request_params, response]
                    event = AuditEvent(
                        event_time=datetime.fromisoformat(row[0].replace('Z', '+00:00')) if row[0] else now,
                        service_name=str(row[1]) if row[1] else "unknown",
                        event_type=str(row[2]) if row[2] else "login",
                        user_name=str(row[3]) if row[3] else None,
                        source_ip=str(row[4]) if row[4] else None,
                        details={
                            'request_params': row[5] if row[5] else {},
                            'response': row[6] if row[6] else {}
                        }
                    )
                    audit_events.append(event)

            logger.info(f"Found {len(audit_events)} failed login events")
            return audit_events

        except Exception as e:
            logger.error(f"Error querying failed logins: {e}")
            raise APIError(f"Failed to query audit logs: {e}")

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

        Prerequisites:
            - system.access.audit table must be available (Unity Catalog audit logs enabled)
            - A SQL warehouse must be available for query execution

        Args:
            lookback_hours: How far back to search for admin changes. Must be positive.
                Default: 24.0 hours.
            limit: Maximum number of results to return. Must be positive.
                Default: 100.

        Returns:
            List of AuditEvent objects for administrative changes, sorted by time (newest first).
            Returns empty list if system.access.audit table is not available.

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

        # Check if audit table exists
        if not self._table_exists(self._audit_table):
            logger.info(
                f"Table {self._audit_table} not found. Please enable Unity Catalog audit logs. "
                "Returning empty results."
            )
            return []

        # Get warehouse for query execution
        warehouse_id = self._get_default_warehouse_id()
        if not warehouse_id:
            logger.info("No SQL warehouse available for audit queries. Returning empty results.")
            return []

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

        # Admin-related action names to filter for
        admin_actions = [
            'addPrincipalToGroup',
            'removePrincipalFromGroup',
            'createServicePrincipal',
            'deleteServicePrincipal',
            'updateServicePrincipal',
            'createUser',
            'deleteUser',
            'updateUser',
            'changePermissions',
            'updatePermissions',
            'setPermissions',
            'createClusterPolicy',
            'updateClusterPolicy',
            'deleteClusterPolicy',
            'updateWorkspaceConf'
        ]

        # Build action list for SQL IN clause
        actions_sql = "', '".join(admin_actions)

        # Build SQL query for admin changes
        sql = f"""
        SELECT
            event_time,
            service_name,
            action_name,
            user_identity.email as user_name,
            source_ip_address,
            request_params,
            response
        FROM {self._audit_table}
        WHERE event_time >= TIMESTAMP '{start_time_str}'
          AND (
            action_name IN ('{actions_sql}')
            OR service_name = 'accounts'
            OR service_name = 'unityCatalog'
          )
        ORDER BY event_time DESC
        LIMIT {limit}
        """

        try:
            logger.debug(f"Executing SQL query: {sql}")

            # Execute SQL query
            statement = self.ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout="50s"  # Maximum allowed by Databricks API
            )

            # Parse results into AuditEvent objects
            audit_events = []
            if statement.result and statement.result.data_array:
                for row in statement.result.data_array:
                    # row format: [event_time, service_name, action_name, user_name, source_ip, request_params, response]
                    event = AuditEvent(
                        event_time=datetime.fromisoformat(row[0].replace('Z', '+00:00')) if row[0] else now,
                        service_name=str(row[1]) if row[1] else "unknown",
                        event_type=str(row[2]) if row[2] else "unknown",
                        user_name=str(row[3]) if row[3] else None,
                        source_ip=str(row[4]) if row[4] else None,
                        details={
                            'request_params': row[5] if row[5] else {},
                            'response': row[6] if row[6] else {}
                        }
                    )
                    audit_events.append(event)

            logger.info(f"Found {len(audit_events)} admin change events")
            return audit_events

        except Exception as e:
            logger.error(f"Error querying admin changes: {e}")
            raise APIError(f"Failed to query audit logs: {e}")
