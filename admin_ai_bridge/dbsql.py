"""
DBSQL query history and performance analysis for Databricks Admin AI Bridge.

This module provides read-only access to SQL query history including:
- Slowest queries
- User query summaries
- Query performance metrics
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import QueryStatus

from .config import AdminBridgeConfig, get_workspace_client
from .errors import APIError, ValidationError
from .schemas import QueryHistoryEntry

logger = logging.getLogger(__name__)


class DBSQLAdmin:
    """
    Admin interface for Databricks SQL query history and performance.

    This class provides read-only methods to analyze SQL query performance,
    identify slow queries, and summarize user activity.

    All methods are safe and read-only - no destructive operations are performed.

    Attributes:
        ws: WorkspaceClient instance for API access
    """

    def __init__(self, cfg: AdminBridgeConfig | None = None):
        """
        Initialize DBSQLAdmin with optional configuration.

        Args:
            cfg: AdminBridgeConfig instance. If None, uses default credentials.

        Examples:
            >>> # Using profile
            >>> cfg = AdminBridgeConfig(profile="DEFAULT")
            >>> dbsql_admin = DBSQLAdmin(cfg)

            >>> # Using default credentials
            >>> dbsql_admin = DBSQLAdmin()
        """
        self.ws = get_workspace_client(cfg)
        logger.info("DBSQLAdmin initialized")

    def top_slowest_queries(
        self,
        lookback_hours: float = 24.0,
        limit: int = 20,
    ) -> List[QueryHistoryEntry]:
        """
        Return the top N slowest queries by duration in the given time window.

        This method identifies queries with the longest execution time, which can
        help pinpoint performance bottlenecks and optimization opportunities.

        Args:
            lookback_hours: How far back to search for queries. Must be positive.
                Default: 24.0 hours.
            limit: Maximum number of results to return. Must be positive.
                Default: 20.

        Returns:
            List of QueryHistoryEntry objects sorted by duration (slowest first).

        Raises:
            ValidationError: If parameters are invalid (negative values, etc.)
            APIError: If the Databricks API returns an error

        Examples:
            >>> dbsql_admin = DBSQLAdmin()
            >>> # Find the 10 slowest queries in the last 48 hours
            >>> slow_queries = dbsql_admin.top_slowest_queries(
            ...     lookback_hours=48.0,
            ...     limit=10
            ... )
            >>> for query in slow_queries:
            ...     print(f"Query {query.query_id}: {query.duration_seconds:.1f}s by {query.user_name}")
        """
        # Validate parameters
        if lookback_hours <= 0:
            raise ValidationError("lookback_hours must be positive")
        if limit <= 0:
            raise ValidationError("limit must be positive")

        logger.info(f"Searching for slowest queries in last {lookback_hours}h")

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)

        queries = []

        try:
            # Use query history API
            # Note: The SDK's query_history.list() method filters by time
            query_filter = {
                "start_time_from": int(start_time.timestamp() * 1000),
                "start_time_to": int(now.timestamp() * 1000),
            }

            history = self.ws.query_history.list(
                filter_by=query_filter,
                max_results=1000,  # Get more than needed to ensure we find the slowest
            )

            for query_info in history:
                if not query_info.query_id:
                    continue

                # Calculate duration
                start_ms = query_info.query_start_time_ms
                end_ms = query_info.query_end_time_ms

                if start_ms is None or end_ms is None:
                    continue

                duration_seconds = (end_ms - start_ms) / 1000.0

                # Only include completed queries with meaningful duration
                if duration_seconds <= 0:
                    continue

                # Safely extract optional sql_text field
                sql_text = None
                if hasattr(query_info, 'query_text'):
                    val = query_info.query_text
                    sql_text = val if isinstance(val, (str, type(None))) else None

                query_entry = QueryHistoryEntry(
                    query_id=query_info.query_id,
                    warehouse_id=query_info.warehouse_id,
                    user_name=query_info.user_name,
                    status=query_info.status.value if query_info.status else None,
                    start_time=datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
                    end_time=datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc),
                    duration_seconds=duration_seconds,
                    sql_text=sql_text,
                )
                queries.append(query_entry)

        except Exception as e:
            logger.error(f"Error listing query history: {e}")
            raise APIError(f"Failed to list query history: {e}")

        # Sort by duration (slowest first) and take top N
        queries.sort(key=lambda x: x.duration_seconds or 0, reverse=True)
        result = queries[:limit]

        logger.info(f"Found {len(result)} slow queries")
        return result

    def user_query_summary(
        self,
        user_name: str,
        lookback_hours: float = 24.0,
    ) -> Dict[str, Any]:
        """
        Summarize queries for a given user in the last time window.

        This method provides aggregate statistics about a user's query activity
        including counts, average duration, failure rate, and more.

        Args:
            user_name: Username to summarize queries for. Must not be empty.
            lookback_hours: How far back to analyze. Must be positive.
                Default: 24.0 hours.

        Returns:
            Dictionary containing:
                - user_name: The username
                - total_queries: Total number of queries
                - successful_queries: Number of successful queries
                - failed_queries: Number of failed queries
                - avg_duration_seconds: Average query duration
                - max_duration_seconds: Longest query duration
                - min_duration_seconds: Shortest query duration
                - total_duration_seconds: Sum of all query durations
                - failure_rate: Percentage of queries that failed (0-100)
                - warehouses_used: List of unique warehouse IDs used
                - time_window_start: Start of analysis window
                - time_window_end: End of analysis window

        Raises:
            ValidationError: If parameters are invalid
            APIError: If the Databricks API returns an error

        Examples:
            >>> dbsql_admin = DBSQLAdmin()
            >>> summary = dbsql_admin.user_query_summary(
            ...     user_name="john.doe@company.com",
            ...     lookback_hours=72.0
            ... )
            >>> print(f"User {summary['user_name']} ran {summary['total_queries']} queries")
            >>> print(f"Average duration: {summary['avg_duration_seconds']:.2f}s")
            >>> print(f"Failure rate: {summary['failure_rate']:.1f}%")
        """
        # Validate parameters
        if not user_name or not user_name.strip():
            raise ValidationError("user_name must not be empty")
        if lookback_hours <= 0:
            raise ValidationError("lookback_hours must be positive")

        logger.info(f"Summarizing queries for user {user_name} in last {lookback_hours}h")

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)

        # Initialize counters
        total_queries = 0
        successful_queries = 0
        failed_queries = 0
        durations = []
        warehouses = set()

        try:
            # Use query history API with user filter
            query_filter = {
                "user_name": user_name,
                "start_time_from": int(start_time.timestamp() * 1000),
                "start_time_to": int(now.timestamp() * 1000),
            }

            history = self.ws.query_history.list(
                filter_by=query_filter,
                max_results=1000,
            )

            for query_info in history:
                if not query_info.query_id:
                    continue

                total_queries += 1

                # Track warehouse usage
                if query_info.warehouse_id:
                    warehouses.add(query_info.warehouse_id)

                # Count success/failure
                if query_info.status:
                    if query_info.status == QueryStatus.FINISHED:
                        successful_queries += 1
                    elif query_info.status in (QueryStatus.FAILED, QueryStatus.CANCELED):
                        failed_queries += 1

                # Calculate duration
                start_ms = query_info.query_start_time_ms
                end_ms = query_info.query_end_time_ms

                if start_ms is not None and end_ms is not None:
                    duration_seconds = (end_ms - start_ms) / 1000.0
                    if duration_seconds > 0:
                        durations.append(duration_seconds)

        except Exception as e:
            logger.error(f"Error getting query summary for user {user_name}: {e}")
            raise APIError(f"Failed to get query summary: {e}")

        # Calculate statistics
        avg_duration = sum(durations) / len(durations) if durations else 0.0
        max_duration = max(durations) if durations else 0.0
        min_duration = min(durations) if durations else 0.0
        total_duration = sum(durations)
        failure_rate = (failed_queries / total_queries * 100.0) if total_queries > 0 else 0.0

        summary = {
            "user_name": user_name,
            "total_queries": total_queries,
            "successful_queries": successful_queries,
            "failed_queries": failed_queries,
            "avg_duration_seconds": round(avg_duration, 2),
            "max_duration_seconds": round(max_duration, 2),
            "min_duration_seconds": round(min_duration, 2),
            "total_duration_seconds": round(total_duration, 2),
            "failure_rate": round(failure_rate, 2),
            "warehouses_used": sorted(list(warehouses)),
            "time_window_start": start_time.isoformat(),
            "time_window_end": now.isoformat(),
        }

        logger.info(
            f"User {user_name} summary: {total_queries} queries, "
            f"{failure_rate:.1f}% failure rate"
        )
        return summary
