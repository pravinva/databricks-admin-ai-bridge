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
from databricks.sdk.service.sql import QueryStatus, QueryFilter, TimeRange

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
        warehouse_id: Optional SQL warehouse ID for system table queries
    """

    def __init__(self, cfg: AdminBridgeConfig | None = None, warehouse_id: str | None = None):
        """
        Initialize DBSQLAdmin with optional configuration.

        Args:
            cfg: AdminBridgeConfig instance. If None, uses default credentials.
            warehouse_id: Optional SQL warehouse ID for faster system table queries.
                If None, will fall back to API methods.

        Examples:
            >>> # Using profile
            >>> cfg = AdminBridgeConfig(profile="DEFAULT")
            >>> dbsql_admin = DBSQLAdmin(cfg)

            >>> # Using default credentials with warehouse for faster queries
            >>> dbsql_admin = DBSQLAdmin(warehouse_id="abc123def456")
        """
        self.ws = get_workspace_client(cfg)
        self.warehouse_id = warehouse_id
        logger.info(f"DBSQLAdmin initialized (warehouse_id={warehouse_id})")

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

    def top_slowest_queries(
        self,
        lookback_hours: float = 24.0,
        limit: int = 20,
        warehouse_id: str | None = None,
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
            warehouse_id: Optional SQL warehouse ID for faster system table queries.
                If provided, uses system tables. Otherwise falls back to API.

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

        # Try SQL first if warehouse available
        if warehouse_id or self.warehouse_id:
            wh_id = warehouse_id or self.warehouse_id
            try:
                logger.info(f"Using system tables (warehouse: {wh_id})")
                return self._top_slowest_queries_sql(lookback_hours, limit, wh_id)
            except Exception as e:
                logger.warning(f"System table query failed, falling back to API: {e}")

        # Fall back to API
        logger.info("Using API method")
        return self._top_slowest_queries_api(lookback_hours, limit)

    def _top_slowest_queries_sql(
        self,
        lookback_hours: float,
        limit: int,
        warehouse_id: str,
    ) -> List[QueryHistoryEntry]:
        """Query slowest queries from system.query.history (fast)."""

        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
        SELECT
            query_id,
            warehouse_id,
            executed_by,
            status,
            start_time,
            end_time,
            execution_duration / 1000.0 as duration_seconds,
            statement_text
        FROM system.query.history
        WHERE start_time >= '{start_time_str}'
          AND execution_duration IS NOT NULL
          AND execution_duration > 0
        ORDER BY execution_duration DESC
        LIMIT {limit}
        """

        try:
            logger.debug(f"Executing SQL query: {sql}")
            statement = self.ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout="5m"  # Increased timeout for warehouse startup
            )

            queries = []
            if statement.result and statement.result.data_array:
                for row in statement.result.data_array:
                    query_id = str(row[0]) if row[0] is not None else "unknown"
                    wh_id = str(row[1]) if row[1] is not None else None
                    user_name = str(row[2]) if row[2] is not None else None
                    status = str(row[3]) if row[3] is not None else None
                    start_time_val = datetime.fromisoformat(row[4]) if row[4] else None
                    end_time_val = datetime.fromisoformat(row[5]) if row[5] else None
                    duration_seconds = float(row[6]) if row[6] is not None else 0
                    sql_text = str(row[7]) if row[7] is not None else None

                    query_entry = QueryHistoryEntry(
                        query_id=query_id,
                        warehouse_id=wh_id,
                        user_name=user_name,
                        status=status,
                        start_time=start_time_val,
                        end_time=end_time_val,
                        duration_seconds=duration_seconds,
                        sql_text=sql_text,
                    )
                    queries.append(query_entry)

            logger.info(f"Found {len(queries)} slow queries via SQL")
            return queries

        except Exception as e:
            logger.error(f"Error executing SQL query: {e}")
            raise APIError(f"Failed to query slowest queries from system tables: {e}")

    def _top_slowest_queries_api(
        self,
        lookback_hours: float,
        limit: int,
    ) -> List[QueryHistoryEntry]:
        """Query slowest queries using API calls (slower)."""

        # Calculate time window
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=lookback_hours)

        queries = []

        try:
            # Use query history API with proper QueryFilter and TimeRange objects
            query_filter = QueryFilter(
                query_start_time_range=TimeRange(
                    start_time_ms=int(start_time.timestamp() * 1000),
                    end_time_ms=int(now.timestamp() * 1000),
                )
            )

            history_response = self.ws.query_history.list(
                filter_by=query_filter,
                max_results=1000,  # Get more than needed to ensure we find the slowest
            )

            # Extract the list of queries from the response
            # Handle both real response objects and mocked lists
            if isinstance(history_response, list):
                history = history_response
            elif history_response and hasattr(history_response, 'res'):
                history = history_response.res if history_response.res else []
            else:
                history = []

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

                # Handle status field (can be object or dict)
                status_str = None
                if query_info.status:
                    if hasattr(query_info.status, 'value'):
                        status_str = query_info.status.value
                    elif isinstance(query_info.status, dict):
                        status_str = query_info.status.get('value') or str(query_info.status)
                    else:
                        status_str = str(query_info.status)

                query_entry = QueryHistoryEntry(
                    query_id=query_info.query_id,
                    warehouse_id=query_info.warehouse_id,
                    user_name=query_info.user_name,
                    status=status_str,
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

        logger.info(f"Found {len(result)} slow queries via API")
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
            # Use query history API with proper QueryFilter and TimeRange objects
            # Note: user filtering is done via user_ids, not user_name
            # For now, we'll filter by time and then by user_name in code
            query_filter = QueryFilter(
                query_start_time_range=TimeRange(
                    start_time_ms=int(start_time.timestamp() * 1000),
                    end_time_ms=int(now.timestamp() * 1000),
                )
            )

            history_response = self.ws.query_history.list(
                filter_by=query_filter,
                max_results=1000,
            )

            # Extract the list of queries from the response
            # Handle both real response objects and mocked lists
            if isinstance(history_response, list):
                history = history_response
            elif history_response and hasattr(history_response, 'res'):
                history = history_response.res if history_response.res else []
            else:
                history = []

            for query_info in history:
                if not query_info.query_id:
                    continue

                # Filter by user_name (since API doesn't support filtering by name directly)
                if query_info.user_name != user_name:
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
