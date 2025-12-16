"""
Jobs and Workflows observability for Databricks Admin AI Bridge.

This module provides read-only access to job run information including:
- Long-running jobs
- Failed jobs
- Job run history and metrics
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

from .config import AdminBridgeConfig, get_workspace_client
from .errors import APIError, ValidationError
from .schemas import JobRunSummary

logger = logging.getLogger(__name__)


class JobsAdmin:
    """
    Admin interface for Databricks Jobs and Workflows.

    This class provides read-only methods to query job runs, identify long-running
    or failed jobs, and analyze job performance patterns.

    All methods are safe and read-only - no destructive operations are performed.

    Attributes:
        ws: WorkspaceClient instance for API access
        warehouse_id: Optional SQL warehouse ID for system table queries
    """

    def __init__(self, cfg: AdminBridgeConfig | None = None, warehouse_id: str | None = None):
        """
        Initialize JobsAdmin with optional configuration.

        Args:
            cfg: AdminBridgeConfig instance. If None, uses default credentials.
            warehouse_id: Optional SQL warehouse ID for faster system table queries.
                If None, will fall back to API methods.

        Examples:
            >>> # Using profile
            >>> cfg = AdminBridgeConfig(profile="DEFAULT")
            >>> jobs_admin = JobsAdmin(cfg)

            >>> # Using default credentials with warehouse for faster queries
            >>> jobs_admin = JobsAdmin(warehouse_id="abc123def456")
        """
        self.ws = get_workspace_client(cfg)
        self.warehouse_id = warehouse_id
        logger.info(f"JobsAdmin initialized (warehouse_id={warehouse_id})")

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

    def list_long_running_jobs(
        self,
        min_duration_hours: float = 4.0,
        lookback_hours: float = 24.0,
        limit: int = 100,
        warehouse_id: str | None = None,
    ) -> List[JobRunSummary]:
        """
        List job runs with duration exceeding the specified threshold.

        This method identifies jobs that have been running longer than expected,
        which may indicate performance issues, inefficient queries, or stuck jobs.

        Args:
            min_duration_hours: Minimum duration in hours to be considered long-running.
                Must be positive. Default: 4.0 hours.
            lookback_hours: How far back to search for runs. Must be positive.
                Default: 24.0 hours.
            limit: Maximum number of results to return. Must be positive.
                Default: 100.
            warehouse_id: Optional SQL warehouse ID for faster system table queries.
                If provided, uses system tables. Otherwise falls back to API.

        Returns:
            List of JobRunSummary objects sorted by duration (longest first).

        Raises:
            ValidationError: If parameters are invalid (negative values, etc.)
            APIError: If the Databricks API returns an error

        Examples:
            >>> jobs_admin = JobsAdmin()
            >>> # Find jobs running over 6 hours in the last 48 hours
            >>> long_jobs = jobs_admin.list_long_running_jobs(
            ...     min_duration_hours=6.0,
            ...     lookback_hours=48.0,
            ...     limit=50
            ... )
            >>> for job in long_jobs:
            ...     print(f"{job.job_name}: {job.duration_seconds / 3600:.1f} hours")
        """
        # Validate parameters
        if min_duration_hours <= 0:
            raise ValidationError("min_duration_hours must be positive")
        if lookback_hours <= 0:
            raise ValidationError("lookback_hours must be positive")
        if limit <= 0:
            raise ValidationError("limit must be positive")

        logger.info(
            f"Searching for jobs running > {min_duration_hours}h in last {lookback_hours}h"
        )

        # Try SQL first if warehouse available
        if warehouse_id or self.warehouse_id:
            wh_id = warehouse_id or self.warehouse_id
            try:
                logger.info(f"Using system tables (warehouse: {wh_id})")
                return self._list_long_running_jobs_sql(min_duration_hours, lookback_hours, limit, wh_id)
            except Exception as e:
                logger.warning(f"System table query failed, falling back to API: {e}")

        # Fall back to API
        logger.info("Using API method")
        return self._list_long_running_jobs_api(min_duration_hours, lookback_hours, limit)

    def _list_long_running_jobs_sql(
        self,
        min_duration_hours: float,
        lookback_hours: float,
        limit: int,
        warehouse_id: str,
    ) -> List[JobRunSummary]:
        """Query long-running jobs from system.workflow tables (fast)."""

        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        min_duration_ms = min_duration_hours * 3600 * 1000

        sql = f"""
        SELECT
            t.job_id,
            t.job_name,
            t.run_id,
            t.result_state,
            t.life_cycle_state,
            t.start_time,
            t.end_time,
            t.execution_duration as duration_ms
        FROM system.workflow.job_task_run_timeline t
        WHERE t.start_time >= '{start_time_str}'
          AND t.execution_duration >= {min_duration_ms}
        ORDER BY t.execution_duration DESC
        LIMIT {limit}
        """

        try:
            logger.debug(f"Executing SQL query: {sql}")
            statement = self.ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout="30s"
            )

            long_running_jobs = []
            if statement.result and statement.result.data_array:
                for row in statement.result.data_array:
                    job_id = int(row[0]) if row[0] is not None else 0
                    job_name = str(row[1]) if row[1] is not None else f"Job {job_id}"
                    run_id = int(row[2]) if row[2] is not None else 0
                    result_state = str(row[3]) if row[3] is not None else "UNKNOWN"
                    life_cycle_state = str(row[4]) if row[4] is not None else None
                    start_time_val = datetime.fromisoformat(row[5]) if row[5] else None
                    end_time_val = datetime.fromisoformat(row[6]) if row[6] else None
                    duration_ms = float(row[7]) if row[7] is not None else 0
                    duration_seconds = duration_ms / 1000.0

                    job_summary = JobRunSummary(
                        job_id=job_id,
                        job_name=job_name,
                        run_id=run_id,
                        state=result_state,
                        life_cycle_state=life_cycle_state,
                        start_time=start_time_val,
                        end_time=end_time_val,
                        duration_seconds=duration_seconds,
                    )
                    long_running_jobs.append(job_summary)

            logger.info(f"Found {len(long_running_jobs)} long-running jobs via SQL")
            return long_running_jobs

        except Exception as e:
            logger.error(f"Error executing SQL query: {e}")
            raise APIError(f"Failed to query long-running jobs from system tables: {e}")

    def _list_long_running_jobs_api(
        self,
        min_duration_hours: float,
        lookback_hours: float,
        limit: int,
    ) -> List[JobRunSummary]:
        """Query long-running jobs using API calls (slower)."""

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)
        min_duration_seconds = min_duration_hours * 3600

        long_running_jobs = []

        try:
            # List all jobs to get their names and IDs
            jobs = list(self.ws.jobs.list())
            logger.debug(f"Found {len(jobs)} total jobs")

            for job in jobs:
                if not job.job_id:
                    continue

                try:
                    # Get recent runs for this job
                    runs = self.ws.jobs.list_runs(
                        job_id=job.job_id,
                        start_time_from=int(start_time.timestamp() * 1000),
                        start_time_to=int(now.timestamp() * 1000),
                        expand_tasks=False,
                    )

                    for run in runs:
                        if not run.run_id:
                            continue

                        # Calculate duration
                        start_ms = run.start_time
                        end_ms = run.end_time

                        if start_ms is None:
                            continue

                        # For running jobs, use current time as end time
                        if end_ms is None and run.state and run.state.life_cycle_state == RunLifeCycleState.RUNNING:
                            end_ms = int(now.timestamp() * 1000)
                        elif end_ms is None:
                            continue

                        duration_seconds = (end_ms - start_ms) / 1000.0

                        # Check if it meets the duration threshold
                        if duration_seconds >= min_duration_seconds:
                            # Determine overall state with robust handling
                            state = "UNKNOWN"
                            if run.state:
                                if run.state.result_state:
                                    if hasattr(run.state.result_state, 'value'):
                                        state = run.state.result_state.value
                                    elif isinstance(run.state.result_state, dict):
                                        state = run.state.result_state.get('value') or str(run.state.result_state)
                                    else:
                                        state = str(run.state.result_state)
                                elif run.state.life_cycle_state:
                                    if hasattr(run.state.life_cycle_state, 'value'):
                                        state = run.state.life_cycle_state.value
                                    elif isinstance(run.state.life_cycle_state, dict):
                                        state = run.state.life_cycle_state.get('value') or str(run.state.life_cycle_state)
                                    else:
                                        state = str(run.state.life_cycle_state)

                            # Handle life_cycle_state field
                            life_cycle_state_str = None
                            if run.state and run.state.life_cycle_state:
                                if hasattr(run.state.life_cycle_state, 'value'):
                                    life_cycle_state_str = run.state.life_cycle_state.value
                                elif isinstance(run.state.life_cycle_state, dict):
                                    life_cycle_state_str = run.state.life_cycle_state.get('value') or str(run.state.life_cycle_state)
                                else:
                                    life_cycle_state_str = str(run.state.life_cycle_state)

                            job_summary = JobRunSummary(
                                job_id=job.job_id,
                                job_name=job.settings.name if job.settings else f"Job {job.job_id}",
                                run_id=run.run_id,
                                state=state,
                                life_cycle_state=life_cycle_state_str,
                                start_time=datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
                                end_time=datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc) if end_ms else None,
                                duration_seconds=duration_seconds,
                            )
                            long_running_jobs.append(job_summary)
                            logger.debug(
                                f"Found long-running job: {job_summary.job_name} "
                                f"(run {job_summary.run_id}), duration: {duration_seconds / 3600:.2f}h"
                            )

                            # Stop if we've reached the limit
                            if len(long_running_jobs) >= limit:
                                break

                except Exception as e:
                    logger.warning(f"Error processing job {job.job_id}: {e}")
                    continue

                if len(long_running_jobs) >= limit:
                    break

        except Exception as e:
            logger.error(f"Error listing long-running jobs: {e}")
            raise APIError(f"Failed to list long-running jobs: {e}")

        # Sort by duration (longest first)
        long_running_jobs.sort(key=lambda x: x.duration_seconds or 0, reverse=True)

        logger.info(f"Found {len(long_running_jobs)} long-running jobs via API")
        return long_running_jobs[:limit]

    def list_failed_jobs(
        self,
        lookback_hours: float = 24.0,
        limit: int = 100,
        warehouse_id: str | None = None,
    ) -> List[JobRunSummary]:
        """
        List failed job runs within the specified time window.

        This method identifies jobs that have failed, helping with troubleshooting
        and identifying recurring issues.

        Args:
            lookback_hours: How far back to search for failed runs. Must be positive.
                Default: 24.0 hours.
            limit: Maximum number of results to return. Must be positive.
                Default: 100.
            warehouse_id: Optional SQL warehouse ID for faster system table queries.
                If provided, uses system tables. Otherwise falls back to API.

        Returns:
            List of JobRunSummary objects for failed runs, sorted by start time (newest first).

        Raises:
            ValidationError: If parameters are invalid (negative values, etc.)
            APIError: If the Databricks API returns an error

        Examples:
            >>> jobs_admin = JobsAdmin()
            >>> # Find all failed jobs in the last 12 hours
            >>> failed = jobs_admin.list_failed_jobs(lookback_hours=12.0)
            >>> print(f"Found {len(failed)} failed jobs")
            >>> for job in failed:
            ...     print(f"{job.job_name} failed at {job.start_time}")
        """
        # Validate parameters
        if lookback_hours <= 0:
            raise ValidationError("lookback_hours must be positive")
        if limit <= 0:
            raise ValidationError("limit must be positive")

        logger.info(f"Searching for failed jobs in last {lookback_hours}h")

        # Try SQL first if warehouse available
        if warehouse_id or self.warehouse_id:
            wh_id = warehouse_id or self.warehouse_id
            try:
                logger.info(f"Using system tables (warehouse: {wh_id})")
                return self._list_failed_jobs_sql(lookback_hours, limit, wh_id)
            except Exception as e:
                logger.warning(f"System table query failed, falling back to API: {e}")

        # Fall back to API
        logger.info("Using API method")
        return self._list_failed_jobs_api(lookback_hours, limit)

    def _list_failed_jobs_sql(
        self,
        lookback_hours: float,
        limit: int,
        warehouse_id: str,
    ) -> List[JobRunSummary]:
        """Query failed jobs from system.workflow tables (fast)."""

        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
        SELECT
            t.job_id,
            t.job_name,
            t.run_id,
            t.result_state,
            t.life_cycle_state,
            t.start_time,
            t.end_time,
            t.execution_duration as duration_ms
        FROM system.workflow.job_task_run_timeline t
        WHERE t.start_time >= '{start_time_str}'
          AND t.result_state IN ('FAILED', 'TIMEDOUT', 'CANCELED')
        ORDER BY t.start_time DESC
        LIMIT {limit}
        """

        try:
            logger.debug(f"Executing SQL query: {sql}")
            statement = self.ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout="30s"
            )

            failed_jobs = []
            if statement.result and statement.result.data_array:
                for row in statement.result.data_array:
                    job_id = int(row[0]) if row[0] is not None else 0
                    job_name = str(row[1]) if row[1] is not None else f"Job {job_id}"
                    run_id = int(row[2]) if row[2] is not None else 0
                    result_state = str(row[3]) if row[3] is not None else "FAILED"
                    life_cycle_state = str(row[4]) if row[4] is not None else None
                    start_time_val = datetime.fromisoformat(row[5]) if row[5] else None
                    end_time_val = datetime.fromisoformat(row[6]) if row[6] else None
                    duration_ms = float(row[7]) if row[7] is not None else 0
                    duration_seconds = duration_ms / 1000.0 if duration_ms else None

                    job_summary = JobRunSummary(
                        job_id=job_id,
                        job_name=job_name,
                        run_id=run_id,
                        state=result_state,
                        life_cycle_state=life_cycle_state,
                        start_time=start_time_val,
                        end_time=end_time_val,
                        duration_seconds=duration_seconds,
                    )
                    failed_jobs.append(job_summary)

            logger.info(f"Found {len(failed_jobs)} failed jobs via SQL")
            return failed_jobs

        except Exception as e:
            logger.error(f"Error executing SQL query: {e}")
            raise APIError(f"Failed to query failed jobs from system tables: {e}")

    def _list_failed_jobs_api(
        self,
        lookback_hours: float,
        limit: int,
    ) -> List[JobRunSummary]:
        """Query failed jobs using API calls (slower)."""

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)

        failed_jobs = []

        try:
            # List all jobs
            jobs = list(self.ws.jobs.list())
            logger.debug(f"Found {len(jobs)} total jobs")

            for job in jobs:
                if not job.job_id:
                    continue

                try:
                    # Get recent runs for this job
                    runs = self.ws.jobs.list_runs(
                        job_id=job.job_id,
                        start_time_from=int(start_time.timestamp() * 1000),
                        start_time_to=int(now.timestamp() * 1000),
                        expand_tasks=False,
                    )

                    for run in runs:
                        if not run.run_id or not run.state:
                            continue

                        # Check if the run failed
                        is_failed = (
                            run.state.result_state == RunResultState.FAILED or
                            run.state.result_state == RunResultState.TIMEDOUT or
                            run.state.life_cycle_state == RunLifeCycleState.INTERNAL_ERROR
                        )

                        if is_failed:
                            start_ms = run.start_time
                            end_ms = run.end_time

                            duration_seconds = None
                            if start_ms and end_ms:
                                duration_seconds = (end_ms - start_ms) / 1000.0

                            # Determine overall state with robust handling
                            state = "FAILED"
                            if run.state.result_state:
                                if hasattr(run.state.result_state, 'value'):
                                    state = run.state.result_state.value
                                elif isinstance(run.state.result_state, dict):
                                    state = run.state.result_state.get('value') or str(run.state.result_state)
                                else:
                                    state = str(run.state.result_state)
                            elif run.state.life_cycle_state:
                                if hasattr(run.state.life_cycle_state, 'value'):
                                    state = run.state.life_cycle_state.value
                                elif isinstance(run.state.life_cycle_state, dict):
                                    state = run.state.life_cycle_state.get('value') or str(run.state.life_cycle_state)
                                else:
                                    state = str(run.state.life_cycle_state)

                            # Handle life_cycle_state field
                            life_cycle_state_str = None
                            if run.state.life_cycle_state:
                                if hasattr(run.state.life_cycle_state, 'value'):
                                    life_cycle_state_str = run.state.life_cycle_state.value
                                elif isinstance(run.state.life_cycle_state, dict):
                                    life_cycle_state_str = run.state.life_cycle_state.get('value') or str(run.state.life_cycle_state)
                                else:
                                    life_cycle_state_str = str(run.state.life_cycle_state)

                            job_summary = JobRunSummary(
                                job_id=job.job_id,
                                job_name=job.settings.name if job.settings else f"Job {job.job_id}",
                                run_id=run.run_id,
                                state=state,
                                life_cycle_state=life_cycle_state_str,
                                start_time=datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc) if start_ms else None,
                                end_time=datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc) if end_ms else None,
                                duration_seconds=duration_seconds,
                            )
                            failed_jobs.append(job_summary)
                            logger.debug(
                                f"Found failed job: {job_summary.job_name} "
                                f"(run {job_summary.run_id}), state: {state}"
                            )

                            # Stop if we've reached the limit
                            if len(failed_jobs) >= limit:
                                break

                except Exception as e:
                    logger.warning(f"Error processing job {job.job_id}: {e}")
                    continue

                if len(failed_jobs) >= limit:
                    break

        except Exception as e:
            logger.error(f"Error listing failed jobs: {e}")
            raise APIError(f"Failed to list failed jobs: {e}")

        # Sort by start time (newest first)
        failed_jobs.sort(
            key=lambda x: x.start_time if x.start_time else datetime.min.replace(tzinfo=timezone.utc),
            reverse=True
        )

        logger.info(f"Found {len(failed_jobs)} failed jobs via API")
        return failed_jobs[:limit]
