"""
Databricks Agent tools for Admin AI Bridge.

This module provides plain Python functions for all admin domains that can be used
with Databricks agents, MLflow agents, or registered as Unity Catalog functions.

All tools are READ-ONLY and safe for LLM usage. Each tool wraps exactly one method
of the corresponding admin class and returns JSON-serializable outputs using
Pydantic model_dump().

Usage with Databricks/MLflow agents:
    >>> from admin_ai_bridge import jobs_admin_tools
    >>> tools = jobs_admin_tools()
    >>> # tools is a list of Python functions
    >>> # Register with MLflow or use with Databricks agents
    >>> import mlflow
    >>> mlflow.models.set_model(tools=tools)

Usage with Unity Catalog:
    >>> from databricks import sql
    >>> # Register functions as UC functions
    >>> for tool in jobs_admin_tools():
    >>>     # Register tool as UC function
"""

from typing import List, Dict, Any, Callable

from .config import AdminBridgeConfig
from .jobs import JobsAdmin
from .dbsql import DBSQLAdmin
from .clusters import ClustersAdmin
from .security import SecurityAdmin
from .usage import UsageAdmin
from .audit import AuditAdmin
from .pipelines import PipelinesAdmin


def jobs_admin_tools(cfg: AdminBridgeConfig | None = None, warehouse_id: str | None = None) -> List[Callable]:
    """
    Create Python functions for Jobs administration to use with Databricks agents.

    These functions can be registered with MLflow agents or used with Unity Catalog.

    Args:
        cfg: AdminBridgeConfig instance. If None, uses default credentials.
        warehouse_id: Optional SQL warehouse ID for faster system table queries.

    Returns:
        List of Python callable functions for job-related operations.

    Examples:
        >>> from admin_ai_bridge import jobs_admin_tools
        >>> tools = jobs_admin_tools(warehouse_id="abc123")
        >>> # Use with MLflow or Databricks agents
        >>> for tool in tools:
        >>>     print(tool.__name__, tool.__doc__)
    """
    jobs = JobsAdmin(cfg, warehouse_id=warehouse_id)

    def list_long_running_jobs(
        min_duration_hours: float = 4.0,
        lookback_hours: float = 24.0,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """List job runs that have been running longer than a specified number of hours. Useful for identifying performance issues, stuck jobs, or workloads that need optimization. Returns job details including duration, state, and timing information.

        Args:
            min_duration_hours: Minimum runtime in hours to be considered long-running (default: 4.0)
            lookback_hours: How far back to search for runs in hours (default: 24.0)
            limit: Maximum number of results to return (default: 20)

        Returns:
            List of job run summaries with job_id, job_name, run_id, state, duration, etc.
        """
        return [j.model_dump() for j in jobs.list_long_running_jobs(
            min_duration_hours=min_duration_hours,
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    def list_failed_jobs(
        lookback_hours: float = 24.0,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """List job runs that have failed within a recent time window. Helps identify recurring failures, troubleshoot issues, and monitor job reliability. Returns failed job details including state and timing information.

        Args:
            lookback_hours: How far back to search for failed runs in hours (default: 24.0)
            limit: Maximum number of results to return (default: 20)

        Returns:
            List of failed job run summaries with job_id, job_name, run_id, state, etc.
        """
        return [j.model_dump() for j in jobs.list_failed_jobs(
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    return [list_long_running_jobs, list_failed_jobs]


def dbsql_admin_tools(cfg: AdminBridgeConfig | None = None, warehouse_id: str | None = None) -> List[Callable]:
    """
    Create Python functions for DBSQL administration to use with Databricks agents.

    These functions can be registered with MLflow agents or used with Unity Catalog.

    Args:
        cfg: AdminBridgeConfig instance. If None, uses default credentials.
        warehouse_id: Optional SQL warehouse ID for faster system table queries.

    Returns:
        List of Python callable functions for DBSQL query history operations.

    Examples:
        >>> from admin_ai_bridge import dbsql_admin_tools
        >>> tools = dbsql_admin_tools(warehouse_id="abc123")
    """
    db = DBSQLAdmin(cfg, warehouse_id=warehouse_id)

    def top_slowest_queries(
        lookback_hours: float = 24.0,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Return the top slowest SQL queries by execution duration in a time window. Useful for identifying query performance bottlenecks and optimization opportunities. Returns query details including duration, user, warehouse, and SQL text.

        Args:
            lookback_hours: How far back to search for queries in hours (default: 24.0)
            limit: Maximum number of results to return (default: 20)

        Returns:
            List of query entries with query_id, duration, user, warehouse, SQL text, etc.
        """
        return [q.model_dump() for q in db.top_slowest_queries(
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    def user_query_summary(
        user_name: str,
        lookback_hours: float = 24.0,
    ) -> Dict[str, Any]:
        """Summarize SQL query activity for a specific user within a time window. Provides aggregate statistics including query counts, success/failure rates, average duration, and warehouse usage. Useful for user activity analysis and troubleshooting.

        Args:
            user_name: Username to analyze (e.g., "user@company.com")
            lookback_hours: How far back to analyze in hours (default: 24.0)

        Returns:
            Summary dictionary with total queries, success/failure counts, average duration,
            failure rate, warehouses used, and time window information.
        """
        return db.user_query_summary(
            user_name=user_name,
            lookback_hours=lookback_hours,
        )

    return [top_slowest_queries, user_query_summary]


def clusters_admin_tools(cfg: AdminBridgeConfig | None = None, warehouse_id: str | None = None) -> List[Callable]:
    """
    Create Python functions for Clusters administration to use with Databricks agents.

    These functions can be registered with MLflow agents or used with Unity Catalog.

    Args:
        cfg: AdminBridgeConfig instance. If None, uses default credentials.
        warehouse_id: Optional SQL warehouse ID for faster system table queries.

    Returns:
        List of Python callable functions for cluster monitoring operations.

    Examples:
        >>> from admin_ai_bridge import clusters_admin_tools
        >>> tools = clusters_admin_tools(warehouse_id="abc123")
    """
    clusters = ClustersAdmin(cfg, warehouse_id=warehouse_id)

    def list_long_running_clusters(
        min_duration_hours: float = 8.0,
        lookback_hours: float = 24.0,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """List clusters that have been running longer than a specified number of hours. Useful for cost optimization by identifying clusters left running unnecessarily or finding long-running workloads that might benefit from optimization. Returns cluster details including runtime, state, and configuration.

        Args:
            min_duration_hours: Minimum runtime in hours to be considered long-running (default: 8.0)
            lookback_hours: How far back to consider cluster start times in hours (default: 24.0)
            limit: Maximum number of results to return (default: 50)

        Returns:
            List of cluster summaries with cluster_id, name, state, runtime, node types, etc.
        """
        return [c.model_dump() for c in clusters.list_long_running_clusters(
            min_duration_hours=min_duration_hours,
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    def list_idle_clusters(
        idle_hours: float = 2.0,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """List running clusters that have been idle with no activity for a specified time. Helps identify clusters consuming resources without performing useful work, which are candidates for termination to reduce costs. Returns cluster details including last activity time.

        Args:
            idle_hours: Number of hours of inactivity to be considered idle (default: 2.0)
            limit: Maximum number of results to return (default: 50)

        Returns:
            List of idle cluster summaries with last activity time and cluster details.
        """
        return [c.model_dump() for c in clusters.list_idle_clusters(
            idle_hours=idle_hours,
            limit=limit,
        )]

    return [list_long_running_clusters, list_idle_clusters]


def security_admin_tools(cfg: AdminBridgeConfig | None = None) -> List[Callable]:
    """
    Create Python functions for Security administration to use with Databricks agents.

    These functions can be registered with MLflow agents or used with Unity Catalog.

    Args:
        cfg: AdminBridgeConfig instance. If None, uses default credentials.

    Returns:
        List of Python callable functions for security and permissions operations.

    Examples:
        >>> from admin_ai_bridge import security_admin_tools
        >>> tools = security_admin_tools()
    """
    sec = SecurityAdmin(cfg)

    def who_can_manage_job(job_id: int) -> List[Dict[str, Any]]:
        """Return all users, groups, and service principals with CAN_MANAGE permission on a job. Useful for understanding job ownership, troubleshooting access issues, and auditing administrative permissions. Returns principal names and permission levels.

        Args:
            job_id: Unique identifier for the job

        Returns:
            List of permission entries with principal names and permission levels.
        """
        return [p.model_dump() for p in sec.who_can_manage_job(job_id)]

    def who_can_use_cluster(cluster_id: str) -> List[Dict[str, Any]]:
        """Return all users, groups, and service principals with permission to use a cluster. Includes CAN_ATTACH_TO, CAN_RESTART, and CAN_MANAGE permissions. Useful for understanding cluster access, troubleshooting permission issues, and auditing who can execute code on specific compute resources.

        Args:
            cluster_id: Unique identifier for the cluster (e.g., "1234-567890-abc123")

        Returns:
            List of permission entries with principal names and permission levels
            (CAN_ATTACH_TO, CAN_RESTART, CAN_MANAGE).
        """
        return [p.model_dump() for p in sec.who_can_use_cluster(cluster_id)]

    return [who_can_manage_job, who_can_use_cluster]


def usage_admin_tools(cfg: AdminBridgeConfig | None = None, warehouse_id: str | None = None) -> List[Callable]:
    """
    Create Python functions for Usage and Cost administration to use with Databricks agents.

    These functions can be registered with MLflow agents or used with Unity Catalog.

    Args:
        cfg: AdminBridgeConfig instance. If None, uses default credentials.
        warehouse_id: Optional SQL warehouse ID for faster system table queries.

    Returns:
        List of Python callable functions for usage, cost, and budget operations.

    Examples:
        >>> from admin_ai_bridge import usage_admin_tools
        >>> tools = usage_admin_tools(warehouse_id="abc123")
    """
    usage = UsageAdmin(cfg, warehouse_id=warehouse_id)

    def top_cost_centers(
        lookback_days: int = 7,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Return the top cost-contributing workloads (clusters, jobs, warehouses, workspaces) over a specified time window. Useful for understanding where costs are coming from and identifying optimization opportunities. Returns cost and DBU consumption data sorted by highest cost first.

        Args:
            lookback_days: Number of days to look back for usage data (default: 7)
            limit: Maximum number of results to return (default: 20)

        Returns:
            List of usage entries with scope, name, cost, DBUs consumed, and time period.
        """
        return [u.model_dump() for u in usage.top_cost_centers(
            lookback_days=lookback_days,
            limit=limit,
        )]

    def cost_by_dimension(
        dimension: str,
        lookback_days: int = 30,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Aggregate cost and DBU consumption by a specific dimension for chargeback analysis. Supports grouping by workspace, cluster, job, warehouse, or custom tags (e.g., project, team). Essential for implementing chargeback models and understanding which teams or projects are consuming resources. Returns aggregated cost data for each dimension value.

        Args:
            dimension: Dimension to group by - "workspace", "cluster", "job", "warehouse",
                      or "tag:KEY" (e.g., "tag:project", "tag:team")
            lookback_days: Number of days to look back for usage data (default: 30)
            limit: Maximum number of results to return (default: 100)

        Returns:
            List of usage entries with aggregated cost and DBUs for each dimension value.
        """
        return [u.model_dump() for u in usage.cost_by_dimension(
            dimension=dimension,
            lookback_days=lookback_days,
            limit=limit,
        )]

    def budget_status(
        dimension: str,
        period_days: int = 30,
        warn_threshold: float = 0.8,
    ) -> List[Dict[str, Any]]:
        """Compare actual costs against allocated budgets for workspaces, projects, or teams. Returns budget utilization status (within_budget, warning, or breached) for each entity. Critical for budget monitoring, detecting overspending early, and financial governance. The warning threshold (default 80%) triggers alerts before budget is fully consumed.

        Args:
            dimension: Dimension to check - "workspace", "project", "team", or custom dimension
            period_days: Number of days in the budget period (default: 30)
            warn_threshold: Utilization threshold for warning status, 0.0-1.0 (default: 0.8)

        Returns:
            List of budget status dictionaries with dimension_value, actual_cost, budget_amount,
            utilization_pct, and status (within_budget, warning, or breached).
        """
        return usage.budget_status(
            dimension=dimension,
            period_days=period_days,
            warn_threshold=warn_threshold,
        )

    return [top_cost_centers, cost_by_dimension, budget_status]


def audit_admin_tools(cfg: AdminBridgeConfig | None = None) -> List[Callable]:
    """
    Create Python functions for Audit log administration to use with Databricks agents.

    These functions can be registered with MLflow agents or used with Unity Catalog.

    Args:
        cfg: AdminBridgeConfig instance. If None, uses default credentials.

    Returns:
        List of Python callable functions for audit log operations.

    Examples:
        >>> from admin_ai_bridge import audit_admin_tools
        >>> tools = audit_admin_tools()
    """
    audit = AuditAdmin(cfg)

    def failed_logins(
        lookback_hours: float = 24.0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return failed login attempts from audit logs within a time window. Useful for detecting potential security threats, brute force attacks, or investigating user access issues. Returns event details including timestamps, usernames, and source IP addresses.

        Args:
            lookback_hours: How far back to search for failed logins in hours (default: 24.0)
            limit: Maximum number of results to return (default: 100)

        Returns:
            List of audit events for failed login attempts with timestamps, users, and IPs.
        """
        return [e.model_dump() for e in audit.failed_logins(
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    def recent_admin_changes(
        lookback_hours: float = 24.0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return recent administrative and permission change events from audit logs. Includes sensitive operations like admin group membership changes, permission grants/revokes, service principal changes, and workspace configuration updates. Essential for security monitoring, compliance auditing, and change tracking.

        Args:
            lookback_hours: How far back to search for admin changes in hours (default: 24.0)
            limit: Maximum number of results to return (default: 100)

        Returns:
            List of audit events for administrative actions like permission changes,
            group membership updates, and workspace configuration changes.
        """
        return [e.model_dump() for e in audit.recent_admin_changes(
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    return [failed_logins, recent_admin_changes]


def pipelines_admin_tools(cfg: AdminBridgeConfig | None = None) -> List[Callable]:
    """
    Create Python functions for Pipelines administration to use with Databricks agents.

    These functions can be registered with MLflow agents or used with Unity Catalog.

    Args:
        cfg: AdminBridgeConfig instance. If None, uses default credentials.

    Returns:
        List of Python callable functions for pipeline monitoring operations.

    Examples:
        >>> from admin_ai_bridge import pipelines_admin_tools
        >>> tools = pipelines_admin_tools()
    """
    pipes = PipelinesAdmin(cfg)

    def list_lagging_pipelines(
        max_lag_seconds: float = 600.0,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """List Delta Live Tables (DLT) and streaming pipelines whose processing lag exceeds a threshold. High lag indicates pipelines falling behind in processing input data, which may indicate performance issues, insufficient resources, or data quality problems. Returns pipeline details including lag duration and state.

        Args:
            max_lag_seconds: Maximum acceptable lag in seconds (default: 600.0 = 10 minutes)
            limit: Maximum number of results to return (default: 50)

        Returns:
            List of pipeline status entries with lag information, state, and timing.
        """
        return [p.model_dump() for p in pipes.list_lagging_pipelines(
            max_lag_seconds=max_lag_seconds,
            limit=limit,
        )]

    def list_failed_pipelines(
        lookback_hours: float = 24.0,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """List Delta Live Tables (DLT) and streaming pipelines that have failed recently. Useful for troubleshooting pipeline issues, monitoring reliability, and identifying recurring problems. Returns pipeline details including failure state and error messages.

        Args:
            lookback_hours: How far back to search for failed pipelines in hours (default: 24.0)
            limit: Maximum number of results to return (default: 50)

        Returns:
            List of pipeline status entries for failed pipelines with error messages.
        """
        return [p.model_dump() for p in pipes.list_failed_pipelines(
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    return [list_lagging_pipelines, list_failed_pipelines]
