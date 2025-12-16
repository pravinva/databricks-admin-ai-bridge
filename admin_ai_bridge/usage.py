"""
Usage and cost monitoring for Databricks Admin AI Bridge.

This module provides read-only access to usage and cost information including:
- Top cost centers by cluster, job, warehouse, or workspace
- DBU consumption tracking
- Resource utilization analysis
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List

from databricks.sdk import WorkspaceClient

from .config import AdminBridgeConfig, get_workspace_client
from .errors import APIError, ValidationError
from .schemas import UsageEntry, BudgetStatus

logger = logging.getLogger(__name__)


class UsageAdmin:
    """
    Admin interface for Databricks usage and cost monitoring.

    This class provides read-only methods to query usage metrics, cost data,
    and resource consumption patterns across the workspace.

    All methods are safe and read-only - no destructive operations are performed.

    Note:
        This implementation provides a basic framework. In production environments,
        you would typically query system tables (system.billing.usage) or integrate
        with external billing APIs for more detailed cost data.

    Attributes:
        ws: WorkspaceClient instance for API access
        usage_table: Name of the usage events table in the lakehouse
        budget_table: Name of the budget table in the lakehouse
        warehouse_id: SQL warehouse ID for executing queries (optional)
    """

    def __init__(
        self,
        cfg: AdminBridgeConfig | None = None,
        usage_table: str = "billing.usage_events",
        budget_table: str = "billing.budgets",
        warehouse_id: str | None = None,
    ):
        """
        Initialize UsageAdmin with optional configuration.

        Args:
            cfg: AdminBridgeConfig instance. If None, uses default credentials.
            usage_table: Fully qualified name of the usage events table.
                Default: "billing.usage_events"
            budget_table: Fully qualified name of the budget table.
                Default: "billing.budgets"
            warehouse_id: SQL warehouse ID for executing queries.
                If None, uses the default warehouse.

        Examples:
            >>> # Using profile
            >>> cfg = AdminBridgeConfig(profile="DEFAULT")
            >>> usage_admin = UsageAdmin(cfg)

            >>> # Using default credentials with custom table names
            >>> usage_admin = UsageAdmin(
            ...     usage_table="custom_schema.usage_data",
            ...     budget_table="custom_schema.budgets"
            ... )

            >>> # With specific warehouse
            >>> usage_admin = UsageAdmin(warehouse_id="abc123def456")
        """
        self.ws = get_workspace_client(cfg)
        self.usage_table = usage_table
        self.budget_table = budget_table
        self.warehouse_id = warehouse_id
        logger.info(
            f"UsageAdmin initialized with usage_table={usage_table}, "
            f"budget_table={budget_table}, warehouse_id={warehouse_id}"
        )

    def _table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the workspace.

        Args:
            table_name: Fully qualified table name (e.g., "billing.usage_events")

        Returns:
            True if table exists, False otherwise
        """
        try:
            # Parse table name
            parts = table_name.split(".")
            if len(parts) == 3:
                catalog, schema, table = parts
            elif len(parts) == 2:
                # Assume default catalog if not specified
                schema, table = parts
                catalog = "hive_metastore"
            else:
                logger.warning(f"Invalid table name format: {table_name}")
                return False

            # Use workspace client to check table existence
            tables = self.ws.tables.list(catalog_name=catalog, schema_name=schema)
            return any(t.name == table for t in tables)
        except Exception as e:
            logger.debug(f"Table {table_name} does not exist or is not accessible: {e}")
            return False

    def top_cost_centers(
        self,
        lookback_days: int = 7,
        limit: int = 20,
        warehouse_id: str | None = None,
    ) -> List[UsageEntry]:
        """
        Return the top cost contributors over the specified time window.

        This method identifies the clusters, jobs, warehouses, and workspaces that
        are consuming the most resources (in terms of DBUs or cost).

        Note:
            If system.billing.usage table is available and warehouse_id is provided,
            this method will use actual billing data. Otherwise, it falls back to
            estimation based on cluster runtime.

        Args:
            lookback_days: Number of days to look back for usage data. Must be positive.
                Default: 7 days.
            limit: Maximum number of results to return. Must be positive.
                Default: 20.
            warehouse_id: Optional SQL warehouse ID for faster system table queries.
                If provided, uses system.billing.usage. Otherwise estimates via API.

        Returns:
            List of UsageEntry objects sorted by estimated cost/usage (highest first).
            Note: Cost and DBU values may be None if billing data is not available.

        Raises:
            ValidationError: If parameters are invalid (negative values, etc.)
            APIError: If the Databricks API returns an error

        Examples:
            >>> usage_admin = UsageAdmin()
            >>> # Find top 10 cost centers in the last 30 days
            >>> top_costs = usage_admin.top_cost_centers(
            ...     lookback_days=30,
            ...     limit=10
            ... )
            >>> for entry in top_costs:
            ...     print(f"{entry.scope}: {entry.name} - ${entry.cost:.2f}" if entry.cost else f"{entry.scope}: {entry.name}")
        """
        # Validate parameters
        if lookback_days <= 0:
            raise ValidationError("lookback_days must be positive")
        if limit <= 0:
            raise ValidationError("limit must be positive")

        logger.info(f"Querying top cost centers for last {lookback_days} days")

        # Try SQL first if warehouse available
        if warehouse_id or self.warehouse_id:
            wh_id = warehouse_id or self.warehouse_id
            try:
                logger.info(f"Using system.billing.usage table (warehouse: {wh_id})")
                return self._top_cost_centers_sql(lookback_days, limit, wh_id)
            except Exception as e:
                logger.warning(f"System table query failed, falling back to estimation: {e}")

        # Fall back to API estimation
        logger.info("Using API estimation method")
        return self._top_cost_centers_api(lookback_days, limit)

    def _top_cost_centers_sql(
        self,
        lookback_days: int,
        limit: int,
        warehouse_id: str,
    ) -> List[UsageEntry]:
        """Query top cost centers from system.billing.usage (fast)."""

        now = datetime.now(timezone.utc)
        start_time = now - timedelta(days=lookback_days)
        start_date_str = start_time.strftime("%Y-%m-%d")

        sql = f"""
        SELECT
            sku_name as scope,
            usage_metadata.cluster_id as name,
            MIN(usage_date) as start_time,
            MAX(usage_date) as end_time,
            SUM(usage_quantity) as total_dbus,
            SUM(usage_quantity * list_price) as total_cost
        FROM system.billing.usage
        WHERE usage_date >= '{start_date_str}'
        GROUP BY sku_name, usage_metadata.cluster_id
        ORDER BY total_cost DESC
        LIMIT {limit}
        """

        try:
            logger.debug(f"Executing SQL query: {sql}")
            statement = self.ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout="50s"  # Maximum allowed by Databricks API
            )

            usage_entries = []
            if statement.result and statement.result.data_array:
                for row in statement.result.data_array:
                    scope = str(row[0]) if row[0] is not None else "unknown"
                    name = str(row[1]) if row[1] is not None else "unknown"
                    start_time_val = datetime.fromisoformat(row[2]) if row[2] else start_time
                    end_time_val = datetime.fromisoformat(row[3]) if row[3] else now
                    total_dbus = float(row[4]) if row[4] is not None else 0
                    total_cost = float(row[5]) if row[5] is not None else 0

                    entry = UsageEntry(
                        scope=scope,
                        name=name,
                        start_time=start_time_val,
                        end_time=end_time_val,
                        cost=total_cost,
                        dbus=total_dbus,
                    )
                    usage_entries.append(entry)

            logger.info(f"Found {len(usage_entries)} cost centers via SQL")
            return usage_entries

        except Exception as e:
            logger.error(f"Error executing SQL query: {e}")
            raise APIError(f"Failed to query cost centers from system tables: {e}")

    def _top_cost_centers_api(
        self,
        lookback_days: int,
        limit: int,
    ) -> List[UsageEntry]:
        """Estimate top cost centers using API calls (slower)."""

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(days=lookback_days)

        usage_entries = []

        try:
            # Query cluster usage
            # Note: In production, you would query system.billing.usage or similar tables
            clusters = list(self.ws.clusters.list())
            logger.debug(f"Found {len(clusters)} clusters")

            for cluster in clusters:
                if not cluster.cluster_id:
                    continue

                try:
                    # Get cluster events to estimate usage
                    events = self.ws.clusters.events(
                        cluster_id=cluster.cluster_id,
                        start_time=int(start_time.timestamp() * 1000),
                        end_time=int(now.timestamp() * 1000),
                        max_items=100
                    )

                    # Calculate approximate runtime
                    # This is a simplified estimation - in production use billing tables
                    cluster_start = None
                    cluster_end = None
                    total_runtime_hours = 0.0

                    for event in events:
                        if event.type:
                            # Handle type field (can be object or dict)
                            type_str = None
                            if hasattr(event.type, 'value'):
                                type_str = event.type.value
                            elif isinstance(event.type, dict):
                                type_str = event.type.get('value') or str(event.type)
                            else:
                                type_str = str(event.type)

                            if type_str and "STARTING" in type_str:
                                cluster_start = event.timestamp
                            elif type_str and ("TERMINATING" in type_str or "TERMINATED" in type_str):
                                if cluster_start and event.timestamp:
                                    runtime_ms = event.timestamp - cluster_start
                                    total_runtime_hours += runtime_ms / (1000.0 * 3600.0)
                                    cluster_start = None

                    # If still running, add time until now
                    if cluster_start:
                        runtime_ms = int(now.timestamp() * 1000) - cluster_start
                        total_runtime_hours += runtime_ms / (1000.0 * 3600.0)

                    if total_runtime_hours > 0:
                        # Estimate DBUs (very rough approximation)
                        # In production, query actual billing data
                        num_workers = cluster.num_workers if cluster.num_workers else 1
                        estimated_dbus = total_runtime_hours * (1 + num_workers) * 2.0  # Rough estimate

                        entry = UsageEntry(
                            scope="cluster",
                            name=cluster.cluster_name or f"Cluster {cluster.cluster_id}",
                            start_time=start_time,
                            end_time=now,
                            cost=None,  # Would come from billing data
                            dbus=estimated_dbus
                        )
                        usage_entries.append(entry)
                        logger.debug(f"Cluster {entry.name}: ~{estimated_dbus:.1f} DBUs over {total_runtime_hours:.1f}h")

                except Exception as e:
                    logger.warning(f"Error processing cluster {cluster.cluster_id}: {e}")
                    continue

            # Query SQL warehouse usage
            try:
                warehouses = list(self.ws.warehouses.list())
                logger.debug(f"Found {len(warehouses)} SQL warehouses")

                for warehouse in warehouses:
                    if not warehouse.id:
                        continue

                    try:
                        # Get warehouse info for state
                        wh_info = self.ws.warehouses.get(id=warehouse.id)

                        # Handle state field (can be object or dict)
                        state_str = None
                        if wh_info.state:
                            if hasattr(wh_info.state, 'value'):
                                state_str = wh_info.state.value
                            elif isinstance(wh_info.state, dict):
                                state_str = wh_info.state.get('value') or str(wh_info.state)
                            else:
                                state_str = str(wh_info.state)

                        # Estimate usage based on cluster size and state
                        # In production, query system.billing.usage for actual data
                        if state_str and "RUNNING" in state_str:
                            cluster_size = wh_info.cluster_size or "2X-Small"

                            # Very rough estimation
                            size_multiplier = {
                                "2X-Small": 1, "X-Small": 2, "Small": 4,
                                "Medium": 8, "Large": 16, "X-Large": 32, "2X-Large": 64
                            }.get(cluster_size, 4)

                            estimated_dbus = lookback_days * 24 * size_multiplier * 0.5  # Rough estimate

                            entry = UsageEntry(
                                scope="warehouse",
                                name=wh_info.name or f"Warehouse {wh_info.id}",
                                start_time=start_time,
                                end_time=now,
                                cost=None,  # Would come from billing data
                                dbus=estimated_dbus
                            )
                            usage_entries.append(entry)
                            logger.debug(f"Warehouse {entry.name}: ~{estimated_dbus:.1f} DBUs (estimated)")

                    except Exception as e:
                        logger.warning(f"Error processing warehouse {warehouse.id}: {e}")
                        continue

            except Exception as e:
                logger.warning(f"Error listing warehouses: {e}")

            # Sort by estimated DBUs (highest first)
            usage_entries.sort(key=lambda x: x.dbus if x.dbus else 0, reverse=True)

            logger.info(f"Found {len(usage_entries)} usage entries via API estimation")
            return usage_entries[:limit]

        except Exception as e:
            logger.error(f"Error querying usage data: {e}")
            raise APIError(f"Failed to query usage data: {e}")

    def cost_by_dimension(
        self,
        dimension: str,
        lookback_days: int = 30,
        limit: int = 100,
    ) -> List[UsageEntry]:
        """
        Aggregate cost and DBUs by a given dimension for chargeback analysis.

        This method queries the usage table and groups costs by the specified dimension
        (workspace, cluster, job, warehouse, or tag). This is useful for implementing
        chargeback models and understanding which teams/projects are consuming resources.

        Args:
            dimension: Dimension to aggregate by. Supported values:
                - "workspace": Group by workspace_id
                - "cluster": Group by cluster_id
                - "job": Group by job_id
                - "warehouse": Group by warehouse_id
                - "tag:KEY": Group by tag value (e.g., "tag:project", "tag:team")
            lookback_days: Number of days to look back for usage data. Must be positive.
                Default: 30 days.
            limit: Maximum number of results to return. Must be positive.
                Default: 100.

        Returns:
            List of UsageEntry objects sorted by cost (highest first).
            Each entry contains aggregated cost and DBU data for the dimension value.

        Raises:
            ValidationError: If parameters are invalid (negative values, unsupported dimension)
            APIError: If the SQL query or Databricks API returns an error

        Examples:
            >>> usage_admin = UsageAdmin()

            >>> # Get cost by workspace
            >>> workspace_costs = usage_admin.cost_by_dimension(
            ...     dimension="workspace",
            ...     lookback_days=30
            ... )
            >>> for entry in workspace_costs:
            ...     print(f"{entry.name}: ${entry.cost:.2f}")

            >>> # Get cost by project tag
            >>> project_costs = usage_admin.cost_by_dimension(
            ...     dimension="tag:project",
            ...     lookback_days=7,
            ...     limit=20
            ... )

            >>> # Get cost by cluster
            >>> cluster_costs = usage_admin.cost_by_dimension(
            ...     dimension="cluster",
            ...     lookback_days=30
            ... )

        Note:
            This method requires a usage table with the following schema:
            - timestamp: Event timestamp
            - workspace_id, cluster_id, job_id, warehouse_id: Resource identifiers
            - cost: Cost in currency units
            - dbu_consumed: DBUs consumed
            - tags: Map or struct containing tag key-value pairs (for tag: dimensions)

        Prerequisites:
            - Usage table (default: billing.usage_events) must exist
            - Table must be configured with billing data export from Databricks account console
        """
        # Validate parameters
        if lookback_days <= 0:
            raise ValidationError("lookback_days must be positive")
        if limit <= 0:
            raise ValidationError("limit must be positive")

        logger.info(
            f"Querying cost by dimension '{dimension}' for last {lookback_days} days"
        )

        # Check if usage table exists
        if not self._table_exists(self.usage_table):
            logger.info(
                f"Table {self.usage_table} not found. Please configure billing data export "
                "in the Databricks account console. Returning empty results."
            )
            return []

        # Validate and parse dimension
        dimension_lower = dimension.lower()
        is_tag_dimension = dimension_lower.startswith("tag:")

        if is_tag_dimension:
            # Extract tag key
            tag_key = dimension[4:]  # Remove "tag:" prefix
            if not tag_key:
                raise ValidationError("Tag dimension must specify a key (e.g., 'tag:project')")
            group_column = f"tags['{tag_key}']"
            dimension_type = "tag"
            logger.debug(f"Using tag dimension: {tag_key}")
        elif dimension_lower in ["workspace", "cluster", "job", "warehouse"]:
            group_column = f"{dimension_lower}_id"
            dimension_type = dimension_lower
            logger.debug(f"Using standard dimension: {dimension_lower}")
        else:
            raise ValidationError(
                f"Unsupported dimension '{dimension}'. "
                "Supported: workspace, cluster, job, warehouse, or tag:KEY"
            )

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(days=lookback_days)
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = now.strftime("%Y-%m-%d %H:%M:%S")

        # Build SQL query
        sql = f"""
        SELECT
            {group_column} as dimension_value,
            SUM(cost) as total_cost,
            SUM(dbu_consumed) as total_dbus,
            MIN(timestamp) as start_time,
            MAX(timestamp) as end_time
        FROM {self.usage_table}
        WHERE timestamp >= '{start_time_str}'
          AND timestamp < '{end_time_str}'
          AND {group_column} IS NOT NULL
        GROUP BY {group_column}
        ORDER BY total_cost DESC
        LIMIT {limit}
        """

        try:
            logger.debug(f"Executing SQL query: {sql}")

            # Execute SQL query using statement execution API
            # Note: In production, you'd use the SQL Statement Execution API
            # For now, we'll use a simplified approach with the workspace client
            statement = self.ws.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id or self._get_default_warehouse_id(),
                statement=sql,
                wait_timeout="50s"  # Maximum allowed by Databricks API
            )

            # Parse results
            usage_entries = []
            if statement.result and statement.result.data_array:
                for row in statement.result.data_array:
                    # row format: [dimension_value, total_cost, total_dbus, start_time, end_time]
                    dimension_value = str(row[0]) if row[0] is not None else "unknown"
                    total_cost = float(row[1]) if row[1] is not None else 0.0
                    total_dbus = float(row[2]) if row[2] is not None else 0.0
                    row_start_time = datetime.fromisoformat(row[3]) if row[3] else start_time
                    row_end_time = datetime.fromisoformat(row[4]) if row[4] else now

                    entry = UsageEntry(
                        scope=dimension_type,
                        name=dimension_value,
                        start_time=row_start_time,
                        end_time=row_end_time,
                        cost=total_cost,
                        dbus=total_dbus,
                    )
                    usage_entries.append(entry)
                    logger.debug(
                        f"{dimension_type} {dimension_value}: "
                        f"${total_cost:.2f}, {total_dbus:.1f} DBUs"
                    )

            logger.info(f"Found {len(usage_entries)} entries for dimension '{dimension}'")
            return usage_entries

        except Exception as e:
            logger.error(f"Error querying cost by dimension: {e}")
            raise APIError(f"Failed to query cost by dimension: {e}")

    def budget_status(
        self,
        dimension: str,
        period_days: int = 30,
        warn_threshold: float = 0.8,
    ) -> List[dict]:
        """
        Get budget vs actuals status for each entity in a dimension.

        This method compares actual costs against allocated budgets and returns
        the status for each entity. This is useful for budget monitoring and
        detecting overspending.

        Args:
            dimension: Dimension to check budgets for. Supported values:
                - "workspace": Check workspace budgets
                - "project": Check project budgets (typically from tags)
                - "team": Check team budgets (typically from tags)
                - Any custom dimension that exists in the budget table
            period_days: Number of days in the budget period. Must be positive.
                Default: 30 days (monthly).
            warn_threshold: Threshold for warning status (0.0 to 1.0).
                Default: 0.8 (80% utilization triggers warning).

        Returns:
            List of dictionaries, each containing:
                - dimension_value (str): The entity identifier
                - actual_cost (float): Actual cost incurred during the period
                - budget_amount (float): Allocated budget amount
                - utilization_pct (float): Budget utilization percentage (0-100+)
                - status (str): "within_budget", "warning", or "breached"

        Raises:
            ValidationError: If parameters are invalid (negative values, invalid threshold)
            APIError: If the SQL query or Databricks API returns an error

        Examples:
            >>> usage_admin = UsageAdmin()

            >>> # Check workspace budgets for current month
            >>> workspace_status = usage_admin.budget_status(
            ...     dimension="workspace",
            ...     period_days=30
            ... )
            >>> for status in workspace_status:
            ...     if status["status"] == "breached":
            ...         print(f"ALERT: {status['dimension_value']} is over budget!")
            ...         print(f"  Budget: ${status['budget_amount']:.2f}")
            ...         print(f"  Actual: ${status['actual_cost']:.2f}")

            >>> # Check project budgets with custom warning threshold (90%)
            >>> project_status = usage_admin.budget_status(
            ...     dimension="project",
            ...     period_days=30,
            ...     warn_threshold=0.9
            ... )

            >>> # Find all entities with warnings or breaches
            >>> alerts = [
            ...     s for s in project_status
            ...     if s["status"] in ["warning", "breached"]
            ... ]

        Note:
            This method requires:
            - A usage table with timestamp, cost, and dimension columns
            - A budget table with columns:
              - dimension_type: The dimension being budgeted (e.g., "workspace", "project")
              - dimension_value: The specific entity (e.g., workspace ID, project name)
              - budget_amount: The allocated budget amount
              - period: Optional period identifier (e.g., "2024-01")

        Prerequisites:
            - Usage table (default: billing.usage_events) must exist
            - Budget table (default: billing.budgets) must exist
            - Tables must be configured with billing data export from Databricks account console
        """
        # Validate parameters
        if period_days <= 0:
            raise ValidationError("period_days must be positive")
        if not 0 <= warn_threshold <= 1:
            raise ValidationError("warn_threshold must be between 0 and 1")

        logger.info(
            f"Checking budget status for dimension '{dimension}' "
            f"(period={period_days} days, warn_threshold={warn_threshold})"
        )

        # Check if required tables exist
        if not self._table_exists(self.usage_table):
            logger.info(
                f"Table {self.usage_table} not found. Please configure billing data export "
                "in the Databricks account console. Returning empty results."
            )
            return []

        if not self._table_exists(self.budget_table):
            logger.info(
                f"Table {self.budget_table} not found. Please create a budget table "
                "or configure billing data export. Returning empty results."
            )
            return []

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(days=period_days)
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = now.strftime("%Y-%m-%d %H:%M:%S")

        # Determine the column to use for grouping based on dimension
        dimension_lower = dimension.lower()
        if dimension_lower in ["workspace", "cluster", "job", "warehouse"]:
            usage_column = f"{dimension_lower}_id"
        else:
            # For custom dimensions like "project" or "team", assume they're in tags
            usage_column = f"tags['{dimension_lower}']"

        # Build SQL query that joins usage with budget data
        sql = f"""
        WITH actual_costs AS (
            SELECT
                {usage_column} as dimension_value,
                SUM(cost) as actual_cost
            FROM {self.usage_table}
            WHERE timestamp >= '{start_time_str}'
              AND timestamp < '{end_time_str}'
              AND {usage_column} IS NOT NULL
            GROUP BY {usage_column}
        ),
        budgets AS (
            SELECT
                dimension_value,
                budget_amount
            FROM {self.budget_table}
            WHERE dimension_type = '{dimension_lower}'
        )
        SELECT
            COALESCE(b.dimension_value, a.dimension_value) as dimension_value,
            COALESCE(a.actual_cost, 0.0) as actual_cost,
            COALESCE(b.budget_amount, 0.0) as budget_amount
        FROM budgets b
        FULL OUTER JOIN actual_costs a
            ON b.dimension_value = a.dimension_value
        WHERE b.budget_amount IS NOT NULL OR a.actual_cost IS NOT NULL
        ORDER BY actual_cost DESC
        """

        try:
            logger.debug(f"Executing budget status query: {sql}")

            # Execute SQL query
            statement = self.ws.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id or self._get_default_warehouse_id(),
                statement=sql,
                wait_timeout="50s"  # Maximum allowed by Databricks API
            )

            # Parse results and calculate status
            budget_statuses = []
            if statement.result and statement.result.data_array:
                for row in statement.result.data_array:
                    # row format: [dimension_value, actual_cost, budget_amount]
                    dimension_value = str(row[0]) if row[0] is not None else "unknown"
                    actual_cost = float(row[1]) if row[1] is not None else 0.0
                    budget_amount = float(row[2]) if row[2] is not None else 0.0

                    # Calculate utilization percentage
                    if budget_amount > 0:
                        utilization_pct = actual_cost / budget_amount
                    else:
                        # No budget defined - consider it breached if there's any cost
                        utilization_pct = float('inf') if actual_cost > 0 else 0.0

                    # Determine status
                    if utilization_pct >= 1.0:
                        status = "breached"
                    elif utilization_pct >= warn_threshold:
                        status = "warning"
                    else:
                        status = "within_budget"

                    budget_status_dict = {
                        "dimension_value": dimension_value,
                        "actual_cost": actual_cost,
                        "budget_amount": budget_amount,
                        "utilization_pct": utilization_pct * 100,  # Convert to percentage
                        "status": status,
                    }
                    budget_statuses.append(budget_status_dict)

                    logger.debug(
                        f"{dimension_value}: ${actual_cost:.2f} / ${budget_amount:.2f} "
                        f"({utilization_pct*100:.1f}%) - {status}"
                    )

            logger.info(
                f"Found {len(budget_statuses)} budget entries for dimension '{dimension}'"
            )
            return budget_statuses

        except Exception as e:
            logger.error(f"Error querying budget status: {e}")
            raise APIError(f"Failed to query budget status: {e}")

    def _get_default_warehouse_id(self) -> str:
        """
        Get the default SQL warehouse ID.

        Returns:
            The ID of the first available SQL warehouse.

        Raises:
            APIError: If no warehouse is available.
        """
        try:
            warehouses = list(self.ws.warehouses.list())
            if not warehouses:
                raise APIError("No SQL warehouses available")
            return warehouses[0].id
        except Exception as e:
            logger.error(f"Error getting default warehouse: {e}")
            raise APIError(f"Failed to get default warehouse: {e}")
