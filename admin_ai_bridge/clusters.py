"""
Clusters and utilization monitoring for Databricks Admin AI Bridge.

This module provides read-only access to cluster information including:
- Long-running clusters
- Idle clusters
- Cluster utilization metrics
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State

from .config import AdminBridgeConfig, get_workspace_client
from .errors import APIError, ValidationError
from .schemas import ClusterSummary

logger = logging.getLogger(__name__)


class ClustersAdmin:
    """
    Admin interface for Databricks clusters and utilization.

    This class provides read-only methods to monitor cluster usage, identify
    long-running or idle clusters, and analyze compute resource utilization.

    All methods are safe and read-only - no destructive operations are performed.

    Attributes:
        ws: WorkspaceClient instance for API access
    """

    def __init__(self, cfg: AdminBridgeConfig | None = None):
        """
        Initialize ClustersAdmin with optional configuration.

        Args:
            cfg: AdminBridgeConfig instance. If None, uses default credentials.

        Examples:
            >>> # Using profile
            >>> cfg = AdminBridgeConfig(profile="DEFAULT")
            >>> clusters_admin = ClustersAdmin(cfg)

            >>> # Using default credentials
            >>> clusters_admin = ClustersAdmin()
        """
        self.ws = get_workspace_client(cfg)
        logger.info("ClustersAdmin initialized")

    def list_long_running_clusters(
        self,
        min_duration_hours: float = 8.0,
        lookback_hours: float = 24.0,
        limit: int = 100,
    ) -> List[ClusterSummary]:
        """
        List clusters that have been running longer than the specified threshold.

        This method identifies clusters with extended runtime, which may indicate:
        - Clusters left running unnecessarily (cost optimization opportunity)
        - Long-running workloads that might benefit from optimization
        - Clusters that should be reviewed for auto-termination settings

        Args:
            min_duration_hours: Minimum runtime in hours to be considered long-running.
                Must be positive. Default: 8.0 hours.
            lookback_hours: How far back to consider cluster start times. Must be positive.
                Default: 24.0 hours. (This filters for clusters that started within the window
                and have been running longer than min_duration_hours)
            limit: Maximum number of results to return. Must be positive.
                Default: 100.

        Returns:
            List of ClusterSummary objects sorted by runtime duration (longest first).

        Raises:
            ValidationError: If parameters are invalid (negative values, etc.)
            APIError: If the Databricks API returns an error

        Examples:
            >>> clusters_admin = ClustersAdmin()
            >>> # Find clusters running over 12 hours
            >>> long_clusters = clusters_admin.list_long_running_clusters(
            ...     min_duration_hours=12.0,
            ...     lookback_hours=48.0,
            ...     limit=50
            ... )
            >>> for cluster in long_clusters:
            ...     runtime_hours = (datetime.now(timezone.utc) - cluster.start_time).total_seconds() / 3600
            ...     print(f"{cluster.cluster_name}: {runtime_hours:.1f} hours")
        """
        # Validate parameters
        if min_duration_hours <= 0:
            raise ValidationError("min_duration_hours must be positive")
        if lookback_hours <= 0:
            raise ValidationError("lookback_hours must be positive")
        if limit <= 0:
            raise ValidationError("limit must be positive")

        logger.info(
            f"Searching for clusters running > {min_duration_hours}h in last {lookback_hours}h"
        )

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)
        min_duration_seconds = min_duration_hours * 3600

        long_running_clusters = []

        try:
            # List all clusters
            clusters = list(self.ws.clusters.list())
            logger.debug(f"Found {len(clusters)} total clusters")

            for cluster in clusters:
                if not cluster.cluster_id:
                    continue

                # Get detailed cluster info
                try:
                    cluster_info = self.ws.clusters.get(cluster_id=cluster.cluster_id)

                    # Only consider running clusters or recently terminated ones
                    if cluster_info.state not in (
                        State.RUNNING,
                        State.RESIZING,
                        State.RESTARTING,
                    ):
                        continue

                    # Check if cluster has a start time
                    start_time_ms = cluster_info.start_time
                    if start_time_ms is None:
                        continue

                    cluster_start_time = datetime.fromtimestamp(start_time_ms / 1000, tz=timezone.utc)

                    # Skip if cluster started before our lookback window
                    if cluster_start_time < start_time:
                        continue

                    # Calculate how long the cluster has been running
                    runtime_seconds = (now - cluster_start_time).total_seconds()

                    # Check if it meets the duration threshold
                    if runtime_seconds >= min_duration_seconds:
                        # Determine last activity time
                        last_activity = None
                        if hasattr(cluster_info, 'last_activity_time') and cluster_info.last_activity_time:
                            try:
                                if isinstance(cluster_info.last_activity_time, (int, float)):
                                    last_activity = datetime.fromtimestamp(
                                        cluster_info.last_activity_time / 1000,
                                        tz=timezone.utc
                                    )
                            except (TypeError, ValueError):
                                pass

                        # Safely extract optional string fields
                        driver_node_type = None
                        if hasattr(cluster_info, 'driver_node_type_id'):
                            val = cluster_info.driver_node_type_id
                            driver_node_type = val if isinstance(val, (str, type(None))) else None

                        node_type = None
                        if hasattr(cluster_info, 'node_type_id'):
                            val = cluster_info.node_type_id
                            node_type = val if isinstance(val, (str, type(None))) else None

                        policy_id = None
                        if hasattr(cluster_info, 'policy_id'):
                            val = cluster_info.policy_id
                            policy_id = val if isinstance(val, (str, type(None))) else None

                        creator = None
                        if hasattr(cluster_info, 'creator_user_name'):
                            val = cluster_info.creator_user_name
                            creator = val if isinstance(val, (str, type(None))) else None

                        # Handle state field (can be object or dict)
                        state_str = None
                        if cluster_info.state:
                            if hasattr(cluster_info.state, 'value'):
                                state_str = cluster_info.state.value
                            elif isinstance(cluster_info.state, dict):
                                state_str = cluster_info.state.get('value') or str(cluster_info.state)
                            else:
                                state_str = str(cluster_info.state)

                        cluster_summary = ClusterSummary(
                            cluster_id=cluster.cluster_id,
                            cluster_name=cluster_info.cluster_name or f"Cluster {cluster.cluster_id}",
                            state=state_str,
                            creator=creator,
                            start_time=cluster_start_time,
                            driver_node_type=driver_node_type,
                            node_type=node_type,
                            cluster_policy_id=policy_id,
                            last_activity_time=last_activity,
                            is_long_running=True,
                        )
                        long_running_clusters.append(cluster_summary)
                        logger.debug(
                            f"Found long-running cluster: {cluster_summary.cluster_name}, "
                            f"runtime: {runtime_seconds / 3600:.2f}h"
                        )

                        # Stop if we've reached the limit
                        if len(long_running_clusters) >= limit:
                            break

                except Exception as e:
                    logger.warning(f"Error processing cluster {cluster.cluster_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error listing long-running clusters: {e}")
            raise APIError(f"Failed to list long-running clusters: {e}")

        # Sort by start time (oldest/longest running first)
        long_running_clusters.sort(
            key=lambda x: x.start_time if x.start_time else datetime.max.replace(tzinfo=timezone.utc)
        )

        logger.info(f"Found {len(long_running_clusters)} long-running clusters")
        return long_running_clusters[:limit]

    def list_idle_clusters(
        self,
        idle_hours: float = 2.0,
        limit: int = 100,
    ) -> List[ClusterSummary]:
        """
        List clusters with no activity in the last N hours.

        This method identifies running clusters that have been idle, which are
        candidates for termination to reduce costs. Idle clusters consume compute
        resources without performing useful work.

        Args:
            idle_hours: Number of hours of inactivity to be considered idle.
                Must be positive. Default: 2.0 hours.
            limit: Maximum number of results to return. Must be positive.
                Default: 100.

        Returns:
            List of ClusterSummary objects sorted by last activity time (least recent first).

        Raises:
            ValidationError: If parameters are invalid (negative values, etc.)
            APIError: If the Databricks API returns an error

        Examples:
            >>> clusters_admin = ClustersAdmin()
            >>> # Find clusters idle for more than 4 hours
            >>> idle = clusters_admin.list_idle_clusters(idle_hours=4.0)
            >>> print(f"Found {len(idle)} idle clusters")
            >>> for cluster in idle:
            ...     if cluster.last_activity_time:
            ...         idle_duration = (datetime.now(timezone.utc) - cluster.last_activity_time).total_seconds() / 3600
            ...         print(f"{cluster.cluster_name}: idle {idle_duration:.1f} hours")
        """
        # Validate parameters
        if idle_hours <= 0:
            raise ValidationError("idle_hours must be positive")
        if limit <= 0:
            raise ValidationError("limit must be positive")

        logger.info(f"Searching for clusters idle > {idle_hours}h")

        # Calculate idle threshold
        now = datetime.now(timezone.utc)
        idle_threshold = now - timedelta(hours=idle_hours)

        idle_clusters = []

        try:
            # List all clusters
            clusters = list(self.ws.clusters.list())
            logger.debug(f"Found {len(clusters)} total clusters")

            for cluster in clusters:
                if not cluster.cluster_id:
                    continue

                # Get detailed cluster info
                try:
                    cluster_info = self.ws.clusters.get(cluster_id=cluster.cluster_id)

                    # Only consider running clusters
                    if cluster_info.state != State.RUNNING:
                        continue

                    # Check last activity time
                    last_activity = None
                    if hasattr(cluster_info, 'last_activity_time') and cluster_info.last_activity_time:
                        try:
                            if isinstance(cluster_info.last_activity_time, (int, float)):
                                last_activity = datetime.fromtimestamp(
                                    cluster_info.last_activity_time / 1000,
                                    tz=timezone.utc
                                )
                        except (TypeError, ValueError):
                            pass

                    if last_activity is None and cluster_info.start_time:
                        # If no activity time, use start time as fallback
                        try:
                            if isinstance(cluster_info.start_time, (int, float)):
                                last_activity = datetime.fromtimestamp(
                                    cluster_info.start_time / 1000,
                                    tz=timezone.utc
                                )
                        except (TypeError, ValueError):
                            pass

                    # Check if cluster has been idle
                    if last_activity and last_activity < idle_threshold:
                        idle_duration_hours = (now - last_activity).total_seconds() / 3600

                        # Safely extract optional string fields
                        driver_node_type = None
                        if hasattr(cluster_info, 'driver_node_type_id'):
                            val = cluster_info.driver_node_type_id
                            driver_node_type = val if isinstance(val, (str, type(None))) else None

                        node_type = None
                        if hasattr(cluster_info, 'node_type_id'):
                            val = cluster_info.node_type_id
                            node_type = val if isinstance(val, (str, type(None))) else None

                        policy_id = None
                        if hasattr(cluster_info, 'policy_id'):
                            val = cluster_info.policy_id
                            policy_id = val if isinstance(val, (str, type(None))) else None

                        creator = None
                        if hasattr(cluster_info, 'creator_user_name'):
                            val = cluster_info.creator_user_name
                            creator = val if isinstance(val, (str, type(None))) else None

                        # Calculate start_time
                        start_time = None
                        if cluster_info.start_time:
                            try:
                                if isinstance(cluster_info.start_time, (int, float)):
                                    start_time = datetime.fromtimestamp(
                                        cluster_info.start_time / 1000,
                                        tz=timezone.utc
                                    )
                            except (TypeError, ValueError):
                                pass

                        # Handle state field (can be object or dict)
                        state_str = None
                        if cluster_info.state:
                            if hasattr(cluster_info.state, 'value'):
                                state_str = cluster_info.state.value
                            elif isinstance(cluster_info.state, dict):
                                state_str = cluster_info.state.get('value') or str(cluster_info.state)
                            else:
                                state_str = str(cluster_info.state)

                        cluster_summary = ClusterSummary(
                            cluster_id=cluster.cluster_id,
                            cluster_name=cluster_info.cluster_name or f"Cluster {cluster.cluster_id}",
                            state=state_str,
                            creator=creator,
                            start_time=start_time,
                            driver_node_type=driver_node_type,
                            node_type=node_type,
                            cluster_policy_id=policy_id,
                            last_activity_time=last_activity,
                            is_long_running=None,
                        )
                        idle_clusters.append(cluster_summary)
                        logger.debug(
                            f"Found idle cluster: {cluster_summary.cluster_name}, "
                            f"idle: {idle_duration_hours:.2f}h"
                        )

                        # Stop if we've reached the limit
                        if len(idle_clusters) >= limit:
                            break

                except Exception as e:
                    logger.warning(f"Error processing cluster {cluster.cluster_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error listing idle clusters: {e}")
            raise APIError(f"Failed to list idle clusters: {e}")

        # Sort by last activity time (least recent first)
        idle_clusters.sort(
            key=lambda x: x.last_activity_time if x.last_activity_time else datetime.min.replace(tzinfo=timezone.utc)
        )

        logger.info(f"Found {len(idle_clusters)} idle clusters")
        return idle_clusters[:limit]
