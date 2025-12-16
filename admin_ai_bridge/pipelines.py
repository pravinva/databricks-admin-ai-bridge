"""
Pipeline and streaming job monitoring for Databricks Admin AI Bridge.

This module provides read-only access to pipeline status information including:
- Lagging pipelines (DLT/Lakeflow)
- Failed pipelines
- Streaming job observability
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineState

from .config import AdminBridgeConfig, get_workspace_client
from .errors import APIError, ValidationError
from .schemas import PipelineStatus

logger = logging.getLogger(__name__)


class PipelinesAdmin:
    """
    Admin interface for Databricks pipelines and streaming jobs.

    This class provides read-only methods to monitor Delta Live Tables (DLT)
    pipelines, Lakeflow jobs, and streaming workloads for lag and failures.

    All methods are safe and read-only - no destructive operations are performed.

    Attributes:
        ws: WorkspaceClient instance for API access
    """

    def __init__(self, cfg: AdminBridgeConfig | None = None):
        """
        Initialize PipelinesAdmin with optional configuration.

        Args:
            cfg: AdminBridgeConfig instance. If None, uses default credentials.

        Examples:
            >>> # Using profile
            >>> cfg = AdminBridgeConfig(profile="DEFAULT")
            >>> pipelines_admin = PipelinesAdmin(cfg)

            >>> # Using default credentials
            >>> pipelines_admin = PipelinesAdmin()
        """
        self.ws = get_workspace_client(cfg)
        logger.info("PipelinesAdmin initialized")

    def list_lagging_pipelines(
        self,
        max_lag_seconds: float = 600.0,
        limit: int = 50,
    ) -> List[PipelineStatus]:
        """
        List streaming/Lakeflow pipelines whose lag exceeds the specified threshold.

        This method identifies pipelines that are falling behind in processing their
        input data, which may indicate performance issues, insufficient resources,
        or data quality problems.

        Args:
            max_lag_seconds: Maximum acceptable lag in seconds. Pipelines with lag
                exceeding this value are included. Must be positive.
                Default: 600.0 seconds (10 minutes).
            limit: Maximum number of results to return. Must be positive.
                Default: 50.

        Returns:
            List of PipelineStatus objects for lagging pipelines, sorted by lag (highest first).

        Raises:
            ValidationError: If parameters are invalid (negative values, etc.)
            APIError: If the Databricks API returns an error

        Examples:
            >>> pipelines_admin = PipelinesAdmin()
            >>> # Find pipelines lagging more than 30 minutes
            >>> lagging = pipelines_admin.list_lagging_pipelines(
            ...     max_lag_seconds=1800.0,
            ...     limit=20
            ... )
            >>> for pipeline in lagging:
            ...     lag_min = pipeline.lag_seconds / 60 if pipeline.lag_seconds else 0
            ...     print(f"{pipeline.name}: {lag_min:.1f} minutes behind")
        """
        # Validate parameters
        if max_lag_seconds <= 0:
            raise ValidationError("max_lag_seconds must be positive")
        if limit <= 0:
            raise ValidationError("limit must be positive")

        logger.info(f"Querying pipelines with lag > {max_lag_seconds}s")

        lagging_pipelines = []

        try:
            # List all pipelines
            pipelines = list(self.ws.pipelines.list_pipelines())
            logger.debug(f"Found {len(pipelines)} total pipelines")

            for pipeline in pipelines:
                if not pipeline.pipeline_id:
                    continue

                try:
                    # Get detailed pipeline info
                    details = self.ws.pipelines.get(pipeline_id=pipeline.pipeline_id)

                    if not details:
                        continue

                    # Check if pipeline is a streaming pipeline with lag information
                    # Note: Lag information may be in latest_updates or state
                    lag_seconds = None

                    # Try to get lag from the latest update
                    if details.latest_updates:
                        latest = details.latest_updates[0] if details.latest_updates else None
                        if latest:
                            # Check for streaming information
                            # Note: The exact field for lag depends on the pipeline type
                            # This is a simplified check
                            if latest.state and latest.state == PipelineState.RUNNING:
                                # In a real implementation, you would extract lag from
                                # monitoring metrics or observability APIs
                                # For now, we'll use creation time as a proxy
                                if latest.creation_time:
                                    try:
                                        # Calculate time since last update
                                        now = datetime.now(timezone.utc)
                                        # Convert creation_time to int if it's a string
                                        creation_time_ms = int(latest.creation_time) if isinstance(latest.creation_time, str) else latest.creation_time
                                        creation_dt = datetime.fromtimestamp(
                                            creation_time_ms / 1000, tz=timezone.utc
                                        )
                                        # This is a placeholder - real lag would come from metrics
                                        potential_lag = (now - creation_dt).total_seconds()

                                        # Only consider as "lag" if pipeline is supposed to be streaming
                                        if details.spec and details.spec.continuous:
                                            lag_seconds = potential_lag
                                    except (ValueError, TypeError) as e:
                                        logger.debug(f"Could not parse creation_time: {e}")
                                        continue

                    # Check if lag exceeds threshold
                    if lag_seconds and lag_seconds > max_lag_seconds:
                        # Handle state field (can be object or dict)
                        state_str = "UNKNOWN"
                        if details.state:
                            if hasattr(details.state, 'value'):
                                state_str = details.state.value
                            elif isinstance(details.state, dict):
                                state_str = details.state.get('value') or str(details.state)
                            else:
                                state_str = str(details.state)

                        # Parse last_update_time safely
                        last_update_time = None
                        if details.latest_updates and details.latest_updates[0].creation_time:
                            try:
                                creation_time_ms = int(details.latest_updates[0].creation_time) if isinstance(details.latest_updates[0].creation_time, str) else details.latest_updates[0].creation_time
                                last_update_time = datetime.fromtimestamp(
                                    creation_time_ms / 1000, tz=timezone.utc
                                )
                            except (ValueError, TypeError) as e:
                                logger.debug(f"Could not parse last_update_time: {e}")

                        pipeline_status = PipelineStatus(
                            pipeline_id=pipeline.pipeline_id,
                            name=details.name or f"Pipeline {pipeline.pipeline_id}",
                            state=state_str,
                            last_update_time=last_update_time,
                            lag_seconds=lag_seconds,
                            last_error=None
                        )
                        lagging_pipelines.append(pipeline_status)
                        logger.debug(
                            f"Found lagging pipeline: {pipeline_status.name} "
                            f"(lag: {lag_seconds / 60:.1f} min)"
                        )

                        # Stop if we've reached the limit
                        if len(lagging_pipelines) >= limit:
                            break

                except Exception as e:
                    logger.warning(f"Error processing pipeline {pipeline.pipeline_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error listing lagging pipelines: {e}")
            raise APIError(f"Failed to list lagging pipelines: {e}")

        # Sort by lag (highest first)
        lagging_pipelines.sort(key=lambda x: x.lag_seconds if x.lag_seconds else 0, reverse=True)

        logger.info(f"Found {len(lagging_pipelines)} lagging pipelines")
        return lagging_pipelines[:limit]

    def list_failed_pipelines(
        self,
        lookback_hours: float = 24.0,
        limit: int = 50,
    ) -> List[PipelineStatus]:
        """
        List pipelines that have failed within the specified time window.

        This method identifies pipeline runs that have encountered errors or failures,
        helping with troubleshooting and monitoring pipeline reliability.

        Args:
            lookback_hours: How far back to search for failed pipelines. Must be positive.
                Default: 24.0 hours.
            limit: Maximum number of results to return. Must be positive.
                Default: 50.

        Returns:
            List of PipelineStatus objects for failed pipelines, sorted by time (newest first).

        Raises:
            ValidationError: If parameters are invalid (negative values, etc.)
            APIError: If the Databricks API returns an error

        Examples:
            >>> pipelines_admin = PipelinesAdmin()
            >>> # Find pipelines that failed in the last 12 hours
            >>> failed = pipelines_admin.list_failed_pipelines(lookback_hours=12.0)
            >>> for pipeline in failed:
            ...     print(f"{pipeline.name}: {pipeline.state}")
            ...     if pipeline.last_error:
            ...         print(f"  Error: {pipeline.last_error}")
        """
        # Validate parameters
        if lookback_hours <= 0:
            raise ValidationError("lookback_hours must be positive")
        if limit <= 0:
            raise ValidationError("limit must be positive")

        logger.info(f"Querying failed pipelines in last {lookback_hours}h")

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)

        failed_pipelines = []

        try:
            # List all pipelines
            pipelines = list(self.ws.pipelines.list_pipelines())
            logger.debug(f"Found {len(pipelines)} total pipelines")

            for pipeline in pipelines:
                if not pipeline.pipeline_id:
                    continue

                try:
                    # Get detailed pipeline info
                    details = self.ws.pipelines.get(pipeline_id=pipeline.pipeline_id)

                    if not details:
                        continue

                    # Check for failed state in recent updates
                    if details.latest_updates:
                        for update in details.latest_updates:
                            if not update.creation_time:
                                continue

                            # Check if update is within the time window
                            update_time = datetime.fromtimestamp(
                                update.creation_time / 1000, tz=timezone.utc
                            )

                            if update_time < start_time:
                                continue

                            # Check if the update failed
                            is_failed = (
                                update.state == PipelineState.FAILED or
                                (update.state == PipelineState.STOPPING and
                                 details.cause and "error" in details.cause.lower())
                            )

                            if is_failed:
                                # Extract error message if available
                                error_msg = None
                                if details.cause:
                                    error_msg = details.cause
                                elif details.latest_updates and details.latest_updates[0]:
                                    # Try to get error from update state message
                                    latest = details.latest_updates[0]
                                    if latest.state_message:
                                        error_msg = latest.state_message

                                # Handle state field (can be object or dict)
                                state_str = "UNKNOWN"
                                if update.state:
                                    if hasattr(update.state, 'value'):
                                        state_str = update.state.value
                                    elif isinstance(update.state, dict):
                                        state_str = update.state.get('value') or str(update.state)
                                    else:
                                        state_str = str(update.state)

                                pipeline_status = PipelineStatus(
                                    pipeline_id=pipeline.pipeline_id,
                                    name=details.name or f"Pipeline {pipeline.pipeline_id}",
                                    state=state_str,
                                    last_update_time=update_time,
                                    lag_seconds=None,
                                    last_error=error_msg
                                )
                                failed_pipelines.append(pipeline_status)
                                logger.debug(
                                    f"Found failed pipeline: {pipeline_status.name} "
                                    f"at {update_time}"
                                )

                                # Only include the most recent failure for each pipeline
                                break

                        # Stop if we've reached the limit
                        if len(failed_pipelines) >= limit:
                            break

                except Exception as e:
                    logger.warning(f"Error processing pipeline {pipeline.pipeline_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error listing failed pipelines: {e}")
            raise APIError(f"Failed to list failed pipelines: {e}")

        # Sort by update time (newest first)
        failed_pipelines.sort(
            key=lambda x: x.last_update_time if x.last_update_time else datetime.min.replace(tzinfo=timezone.utc),
            reverse=True
        )

        logger.info(f"Found {len(failed_pipelines)} failed pipelines")
        return failed_pipelines[:limit]
