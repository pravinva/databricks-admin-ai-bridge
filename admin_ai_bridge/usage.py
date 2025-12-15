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
from .schemas import UsageEntry

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
    """

    def __init__(self, cfg: AdminBridgeConfig | None = None):
        """
        Initialize UsageAdmin with optional configuration.

        Args:
            cfg: AdminBridgeConfig instance. If None, uses default credentials.

        Examples:
            >>> # Using profile
            >>> cfg = AdminBridgeConfig(profile="DEFAULT")
            >>> usage_admin = UsageAdmin(cfg)

            >>> # Using default credentials
            >>> usage_admin = UsageAdmin()
        """
        self.ws = get_workspace_client(cfg)
        logger.info("UsageAdmin initialized")

    def top_cost_centers(
        self,
        lookback_days: int = 7,
        limit: int = 20,
    ) -> List[UsageEntry]:
        """
        Return the top cost contributors over the specified time window.

        This method identifies the clusters, jobs, warehouses, and workspaces that
        are consuming the most resources (in terms of DBUs or cost).

        Note:
            This is a simplified implementation that estimates usage based on cluster
            runtime. For production use with actual billing data, you should query
            system.billing.usage tables or integrate with your billing export.

        Args:
            lookback_days: Number of days to look back for usage data. Must be positive.
                Default: 7 days.
            limit: Maximum number of results to return. Must be positive.
                Default: 20.

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
                        if event.type and "STARTING" in event.type.value:
                            cluster_start = event.timestamp
                        elif event.type and ("TERMINATING" in event.type.value or "TERMINATED" in event.type.value):
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

                        # Estimate usage based on cluster size and state
                        # In production, query system.billing.usage for actual data
                        if wh_info.state and "RUNNING" in wh_info.state.value:
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

            logger.info(f"Found {len(usage_entries)} usage entries")
            return usage_entries[:limit]

        except Exception as e:
            logger.error(f"Error querying usage data: {e}")
            raise APIError(f"Failed to query usage data: {e}")
